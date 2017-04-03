#include <unistd.h>
#include <fcntl.h>
#include "sm-chkpt.h"
#include "sm-index.h"
#include "sm-thread.h"

#include "../benchmarks/ordered_index.h"

sm_chkpt_mgr *chkptmgr;

// The fd for the original chkpt file we recovered from.
// Never changes once assigned value - we currently don't
// evict tuples from main memory, so if an OID entry points
// to somewhere in the chkpt, it must be in this base chkpt.
// TODO(tzwang): implement tuple eviction (anti-caching like).
int sm_chkpt_mgr::base_chkpt_fd= -1;

uint32_t sm_chkpt_mgr::num_recovery_threads = 1;

void
sm_chkpt_mgr::take(bool wait) {
  if(wait) {
    std::unique_lock<std::mutex> lock(_wait_chkpt_mutex);
    _daemon_cv.notify_all();
    _wait_chkpt_cv.wait(lock);
  } else {
    _daemon_cv.notify_all();
  }
}

void sm_chkpt_mgr::daemon() {
  RCU::rcu_register();
  while(!volatile_read(_shutdown)) {
    std::unique_lock<std::mutex> lock(_daemon_mutex);
    _daemon_cv.wait_for(lock, std::chrono::seconds(config::chkpt_interval));
    if(!volatile_read(_shutdown)) {
      do_chkpt();
    }
  }
  RCU::rcu_deregister();
}

void sm_chkpt_mgr::do_chkpt() {
  if(!__sync_bool_compare_and_swap(&_in_progress, false, true)) {
    return;
  }
  ASSERT(volatile_read(_in_progress));
  RCU::rcu_enter();
  // Flush before taking the chkpt: after the flush it's guaranteed
  // that all logs before cstart is durable, no holes possible.
  // The chkpt thread only takes versions created before cstart,
  // making the chkpt essentially consistent.
  auto cstart = logmgr->flush();
  ASSERT(cstart >= _last_cstart);
  if(_last_cstart == cstart) {
    return;
  }
  prepare_file(cstart);
  oidmgr->take_chkpt(cstart.offset());
  // FIXME (tzwang): originally we should put info about the chkpt
  // in a log record and then commit that sys transaction that's
  // responsible for doing chkpt. But that would interfere with
  // normal forward processing. Instead, here we don't use a system
  // transaction to chkpt, but use a dedicated thread and avoid going
  // to the log at all. As a result, we only need to care abou the
  // chkpt begin stamp, and only cstart is useful in this case. cend
  // is ignored and emulated as cstart+1.
  //
  // Note that the chkpt data file's name only contains cstart, and
  // we only write the chkpt marker file (chk-cstart-cend) when chkpt
  // is succeeded.
  //
  // TODO: modify update_chkpt_mark etc to remove/ignore cend related.
  //
  // (align_up is there to supress an ASSERT in sm-log-file.cpp when
  // iterating files in the log dir)
  os_fsync(_fd);
  os_close(_fd);
  logmgr->update_chkpt_mark(cstart,
          LSN::make(align_up(cstart.offset()+1), cstart.segment()));
  scavenge();
  _last_cstart = cstart;
  RCU::rcu_exit();
  LOG(INFO) << "[Checkpoint] marker: 0x" << std::hex << cstart.offset() << std::dec;

  std::unique_lock<std::mutex> l(_wait_chkpt_mutex);
  _wait_chkpt_cv.notify_all();
  volatile_write(_in_progress, false);
  __sync_synchronize();
}

void
sm_chkpt_mgr::scavenge()
{
  // Don't scavenge the (possibly open) base_chkpt
  if(_last_cstart > _base_chkpt_lsn) {
    char buf[CHKPT_DATA_FILE_NAME_BUFSZ];
    size_t n = os_snprintf(buf, sizeof(buf),
                           CHKPT_DATA_FILE_NAME_FMT, _last_cstart._val);
    ASSERT(n < sizeof(buf));
    ASSERT(oidmgr and oidmgr->dfd);
    os_unlinkat(oidmgr->dfd, buf);
  }
}

void
sm_chkpt_mgr::prepare_file(LSN cstart)
{
    char buf[CHKPT_DATA_FILE_NAME_BUFSZ];
    size_t n = os_snprintf(buf, sizeof(buf),
                           CHKPT_DATA_FILE_NAME_FMT, cstart._val);
    ASSERT(n < sizeof(buf));
    ASSERT(oidmgr and oidmgr->dfd);
    _fd = os_openat(oidmgr->dfd, buf, O_CREAT|O_WRONLY);
    _buf_pos = _dur_pos = 0;
}

void
sm_chkpt_mgr::write_buffer(void *p, size_t s)
{
    if (s > kBufferSize) {
        // Too large, write to file directly
        sync_buffer();
        ALWAYS_ASSERT(false);
    }
    else {
        if (_buf_pos + s > kBufferSize) {
            sync_buffer();
            _buf_pos = _dur_pos = 0;
        }
        ASSERT(_buf_pos + s <= kBufferSize);
        memcpy(_buffer + _buf_pos, p, s);
        _buf_pos += s;
    }
}

void sm_chkpt_mgr::do_recovery(char* chkpt_name, OID oid_partition, uint64_t start_offset) {
  int fd = os_openat(oidmgr->dfd, chkpt_name, O_RDONLY);
  lseek(fd, start_offset, SEEK_SET);
  DEFER(close(fd));

  static const uint32_t kBufferSize = 256 * config::MB;
  char* buffer = (char*)malloc(kBufferSize);
  DEFER(free(buffer));

  uint64_t nbytes = start_offset;

  // Get a large chunk each time
  uint64_t file_offset = start_offset;
  uint64_t buf_offset = 0;
  uint64_t cur_buffer_size = read(fd, buffer, kBufferSize);
  while(true) {
    auto read_buffer = [&] (uint32_t size) {
      if(buf_offset + size > cur_buffer_size) {
        file_offset += buf_offset;
        lseek(fd, file_offset , SEEK_SET);
        cur_buffer_size = read(fd, buffer, kBufferSize);
        if(cur_buffer_size == 0) {
          return (char*)nullptr;
        }
        buf_offset = 0;
      }
      uint64_t roff = buf_offset;
      buf_offset += size;
      return &buffer[roff];
    };

    // Extract himark
    OID* himark_ptr = (OID*)read_buffer(sizeof(OID));
    if(!himark_ptr) {
      break;
    }
    OID himark = *himark_ptr;
    nbytes += sizeof(OID);

    // tuple FID
    FID tuple_fid = *(FID*)read_buffer(sizeof(FID));
    ALWAYS_ASSERT(tuple_fid);
    nbytes += sizeof(FID);

    // key FID
    FID key_fid = *(FID*)read_buffer(sizeof(FID));
    ALWAYS_ASSERT(key_fid);
    nbytes += sizeof(FID);

    // Benchmark code should have already registered the table with the engine
    ALWAYS_ASSERT(IndexDescriptor::FidExists(tuple_fid));
    ALWAYS_ASSERT(IndexDescriptor::FidExists(key_fid));
    ALWAYS_ASSERT(oidmgr->file_exists(tuple_fid));
    ALWAYS_ASSERT(oidmgr->file_exists(key_fid));

    oid_array *oa = oidmgr->get_array(tuple_fid);
    oid_array *ka = oidmgr->get_array(key_fid);

    // Populate the OID/key array and index
    OrderedIndex* index = IndexDescriptor::GetIndex(key_fid);
    bool is_primary = index->GetDescriptor()->IsPrimary();
    ALWAYS_ASSERT(index);
    while(1) {
      // Read the OID
      OID o = *(OID*)read_buffer(sizeof(OID));
      nbytes += sizeof(OID);
      if (o == himark)
          break;

      // Key
      uint32_t key_size = *(uint32_t*)read_buffer(sizeof(uint32_t));
      nbytes += sizeof(uint32_t);
      ALWAYS_ASSERT(key_size);

      varstr* key = nullptr;
      if(o % num_recovery_threads == oid_partition) {
        key = (varstr*)MM::allocate(sizeof(varstr) + key_size, 0);
        new (key) varstr((char*)key + sizeof(varstr), key_size);
        memcpy((void*)key->p, read_buffer(key->l), key->l);
        ALWAYS_ASSERT(index->tree_.underlying_btree.insert_if_absent(*key, o, NULL, 0));
        oidmgr->oid_put_new(ka, o, fat_ptr::make(key, INVALID_SIZE_CODE));
        ALWAYS_ASSERT(key->size());
      } else {
        read_buffer(key_size);
        //lseek(fd, key_size, SEEK_CUR);
      }
      nbytes += key_size;

      // Tuple data for primary index only
      if(is_primary) {
        // Size code
        uint8_t size_code = *(uint8_t*)read_buffer(sizeof(uint8_t));
        nbytes += sizeof(uint8_t);
        ALWAYS_ASSERT(size_code != INVALID_SIZE_CODE);
        auto data_size = decode_size_aligned(size_code);
        if(o % num_recovery_threads == oid_partition) {
          fat_ptr pdest = fat_ptr::make((uintptr_t)nbytes, size_code, fat_ptr::ASI_CHK_FLAG);
          Object* obj = (Object*)MM::allocate(decode_size_aligned(size_code), 0);
          new(obj) Object(pdest, NULL_PTR, 0, config::eager_warm_up());
          if(config::eager_warm_up()) {
            obj->Pin();
          } else {
            obj->SetClsn(fat_ptr::make(pdest.offset(), size_code, fat_ptr::ASI_CHK_FLAG));
          }
          // load_durable_object uses the base_chkpt_fd, which is different, so lseek anyway.
          oidmgr->oid_put_new(oa, o, fat_ptr::make(obj, size_code, 0));
        }
        read_buffer(data_size);//(fd, data_size, SEEK_CUR);
        nbytes += data_size;
      }
    }
  }
}

void
sm_chkpt_mgr::recover(LSN chkpt_start, sm_log_recover_mgr *lm) {
  util::scoped_timer t("chkpt_recovery");
  num_recovery_threads = config::worker_threads;
  // Find the chkpt file and recover from there
  char buf[CHKPT_DATA_FILE_NAME_BUFSZ];
  uint64_t n = os_snprintf(buf, sizeof(buf),
                         CHKPT_DATA_FILE_NAME_FMT, chkpt_start._val);
  LOG(INFO) << "[CHKPT Recovery] " << buf;
  ASSERT(n < sizeof(buf));
  ALWAYS_ASSERT(base_chkpt_fd == -1);
  base_chkpt_fd = os_openat(oidmgr->dfd, buf, O_RDONLY);

  // Read a large chunk each time (hopefully we only need it once for the header)
  static const uint32_t kBufferSize = 4 * config::MB;
  char* buffer = (char*)malloc(kBufferSize);
  DEFER(free(buffer));

  uint64_t file_offset = 0;
  uint64_t buf_offset = 0;
  uint64_t cur_buffer_size = read(base_chkpt_fd, buffer, kBufferSize);

  auto read_buffer = [&] (uint32_t size) {
    if(buf_offset + size > cur_buffer_size) {
      file_offset += buf_offset;
      lseek(base_chkpt_fd, file_offset , SEEK_SET);
      cur_buffer_size = read(base_chkpt_fd, buffer, kBufferSize);
      if(cur_buffer_size == 0) {
        return (char*)nullptr;
      }
      buf_offset = 0;
    }
    uint64_t roff = buf_offset;
    buf_offset += size;
    return &buffer[roff];
  };

  // Recover files first from the chkpt header
  uint32_t nfiles = *(uint32_t*)read_buffer(sizeof(uint32_t));
  ALWAYS_ASSERT(nfiles);
  LOG(INFO) << nfiles << " tables";
  uint64_t nbytes = sizeof(uint32_t);

  for(uint32_t i = 0; i < nfiles; ++i) {
    // Format: [table name length, table name, table FID, table himark]
    // Read the table's name
    size_t len = *(size_t*)read_buffer(sizeof(size_t));;
    nbytes += sizeof(size_t);
    char name_buf[256];
    memcpy(name_buf, read_buffer(len), len);
    nbytes += len;
    std::string name(name_buf, len);

    // FIDs
    FID tuple_fid = *(FID*)read_buffer(sizeof(FID));;
    nbytes += sizeof(FID);

    FID key_fid = *(FID*)read_buffer(sizeof(FID));;
    nbytes += sizeof(FID);

    // High mark
    OID himark = *(OID*)read_buffer(sizeof(OID));
    nbytes += sizeof(FID);

    // Benchmark code should have already registered the table with the engine
    ALWAYS_ASSERT(IndexDescriptor::NameExists(name));
    ALWAYS_ASSERT(IndexDescriptor::GetIndex(name));

    IndexDescriptor::Get(name)->Recover(tuple_fid, key_fid, himark);
    LOG(INFO) << "[CHKPT Recovery] " << name << "(" << tuple_fid
              << ", " << key_fid << ")";
  }
  LOG(INFO) << "[Checkpoint] Prepared files";

  // Now deal with the real data, get many threads to do it in parallel
  std::vector<thread::sm_thread*> workers;
  for(uint32_t i = 0; i < num_recovery_threads; ++i) {
    auto* t = thread::get_thread();
    ALWAYS_ASSERT(t);
    thread::sm_thread::task_t task = std::bind(&do_recovery, buf, i, nbytes);
    t->start_task(task);
    workers.push_back(t);
  }

  for(auto& w : workers) {
    w->join();
    thread::put_thread(w);
  }
  LOG(INFO) << "[Checkpoint] Recovered";
}

