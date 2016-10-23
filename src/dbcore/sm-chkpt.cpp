#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include "sm-chkpt.h"
#include "sm-file.h"
#include "sm-oid.h"

#include "../benchmarks/ndb_wrapper.h"

sm_chkpt_mgr *chkptmgr;

sm_chkpt_mgr::sm_chkpt_mgr(LSN last_cstart) :
    _shutdown(false), _buf_pos(0), _dur_pos(0),
    _fd(-1), _last_cstart(last_cstart)
{
    ALWAYS_ASSERT(not mlock(_buffer, BUFFER_SIZE));
}

sm_chkpt_mgr::~sm_chkpt_mgr()
{
    volatile_write(_shutdown, true);
    take();
    _daemon->join();
}

void
sm_chkpt_mgr::start_chkpt_thread()
{
    ASSERT(logmgr and oidmgr);
    _daemon = new std::thread(&sm_chkpt_mgr::daemon, this);
}

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
      if(__sync_bool_compare_and_swap(&_in_progress, false, true)) {
        do_chkpt();
      }
    }
  }
  RCU::rcu_deregister();
}

void sm_chkpt_mgr::do_chkpt() {
  ASSERT(volatile_read(_in_progress));
  RCU::rcu_enter();
  auto cstart = logmgr->flush();
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
  // (align_up is there to supress an assert in sm-log-file.cpp when
  // iterating files in the log dir)
  os_fsync(_fd);
  os_close(_fd);
  logmgr->update_chkpt_mark(cstart,
          LSN::make(align_up(cstart.offset()+1), cstart.segment()));
  scavenge();
  _last_cstart = cstart;
  RCU::rcu_exit();
  LOG(INFO) << "[Checkpoint] marker: 0x" << std::hex << cstart.offset() << std::dec;

  // TODO(tzwang): change next/pdest to point to the chkpt file if we're
  // evicting tuples from main memory as the old logs will be scavenged.
  std::unique_lock<std::mutex> l(_wait_chkpt_mutex);
  _wait_chkpt_cv.notify_all();
  volatile_write(_in_progress, false);
  __sync_synchronize();
}

void
sm_chkpt_mgr::scavenge()
{
    if (not _last_cstart.offset())
        return;
    char buf[CHKPT_DATA_FILE_NAME_BUFSZ];
    size_t n = os_snprintf(buf, sizeof(buf),
                           CHKPT_DATA_FILE_NAME_FMT, _last_cstart._val);
    ASSERT(n < sizeof(buf));
    ASSERT(oidmgr and oidmgr->dfd);
    os_unlinkat(oidmgr->dfd, buf);
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
    if (s > BUFFER_SIZE) {
        // Too large, write to file directly
        sync_buffer();
        ALWAYS_ASSERT(false);
    }
    else {
        if (_buf_pos + s > BUFFER_SIZE) {
            sync_buffer();
            _buf_pos = _dur_pos = 0;
        }
        ASSERT(_buf_pos + s <= BUFFER_SIZE);
        memcpy(_buffer + _buf_pos, p, s);
        _buf_pos += s;
    }
}

void
sm_chkpt_mgr::sync_buffer()
{
    if (_buf_pos > _dur_pos) {
        os_write(_fd, _buffer + _dur_pos, _buf_pos - _dur_pos);
        _dur_pos = _buf_pos;
    }
    os_fsync(_fd);
}

void
sm_chkpt_mgr::recover(LSN chkpt_start, sm_log_recover_mgr *lm) {
  // Find the chkpt file and recover from there
  char buf[CHKPT_DATA_FILE_NAME_BUFSZ];
  size_t n = os_snprintf(buf, sizeof(buf),
                         CHKPT_DATA_FILE_NAME_FMT, chkpt_start._val);
  LOG(INFO) << "[CHKPT Recovery] " << buf;
  ASSERT(n < sizeof(buf));
  int fd = os_openat(oidmgr->dfd, buf, O_RDONLY);

  while (1) {
    // Read himark
    OID himark = 0;
    n = read(fd, &himark, sizeof(OID));
    if(not n) {  // EOF
      break;
    }

    ASSERT(n == sizeof(OID));

    // Read the table's name
    size_t len = 0;
    n = read(fd, &len, sizeof(size_t));
    THROW_IF(n != sizeof(size_t), illegal_argument,
             "Error reading tabel name length");
    char name_buf[256];
    n = read(fd, name_buf, len);
    std::string name(name_buf, len);

    // FID
    FID f = 0;
    n = read(fd, &f, sizeof(FID));

    // Recover fid_map and recreate the empty file
    ALWAYS_ASSERT(!sm_file_mgr::get_index(name));

    // Benchmark code should have already registered the table with the engine
    ALWAYS_ASSERT(sm_file_mgr::name_map[name]);

    sm_file_mgr::name_map[name]->fid = f;
    sm_file_mgr::name_map[name]->index = new ndb_ordered_index(name);
    sm_file_mgr::fid_map[f] = sm_file_mgr::name_map[name];
    ASSERT(not oidmgr->file_exists(f));
    oidmgr->recreate_file(f);
    LOG(INFO) << "[CHKPT Recovery] FID=" << f << "(" << name << ")";

    // Recover allocator status
    oid_array *oa = oidmgr->get_array(f);
    oa->ensure_size(oa->alloc_size(himark));
    oidmgr->recreate_allocator(f, himark);

    sm_file_mgr::name_map[name]->main_array = oa;
    ALWAYS_ASSERT(sm_file_mgr::get_index(name));
    sm_file_mgr::get_index(name)->set_oid_array(f);

    // Initialize the pdest array
    if(config::is_backup_srv() && config::log_ship_by_rdma) {
      sm_file_mgr::get_file(f)->init_pdest_array();
      std::cout << "Created pdest array for FID " << f << std::endl;
    }

    // Populate the OID array and index
    auto* index = sm_file_mgr::get_index(name);
    while(1) {
      // Read the OID
      OID o = 0;
      n = read(fd, &o, sizeof(OID));
      if (o == himark)
          break;

      // Key
      uint32_t key_size;
      n = read(fd, &key_size, sizeof(uint32_t));
      ALWAYS_ASSERT(n == sizeof(uint32_t));
      ALWAYS_ASSERT(key_size);
      varstr* key = (varstr*)MM::allocate(sizeof(varstr) + key_size, 0);
      new (key) varstr((char*)key + sizeof(varstr), key_size);
      n = read(fd, (void*)key->p, key->l);
      ALWAYS_ASSERT(n == key->l);
      ALWAYS_ASSERT(index->btr.underlying_btree.insert_if_absent(*key, o, NULL, 0));

      // Size code
      uint8_t size_code = INVALID_SIZE_CODE;
      n = read(fd, &size_code, sizeof(uint8_t));
      ALWAYS_ASSERT(size_code != INVALID_SIZE_CODE);
      auto data_size = decode_size_aligned(size_code);

      // Read the data: there will usually be read-ahead, so it's likely
      // the data is already cached when we try to fetch the object header.
      // XXX(tzwang): load everything in. Revisit if this takes too long.
      object* obj = new (MM::allocate(data_size, 0)) object;
      n = read(fd, obj, data_size);
      ALWAYS_ASSERT(obj->_clsn.offset());
      ALWAYS_ASSERT(obj->_clsn.offset() < chkpt_start.offset());
      ALWAYS_ASSERT(obj->_pdest.offset());
      ALWAYS_ASSERT(n == data_size);
      oidmgr->oid_put_new(oa, o, fat_ptr::make((uintptr_t)obj, size_code, 0), key);
    }
  }
}

