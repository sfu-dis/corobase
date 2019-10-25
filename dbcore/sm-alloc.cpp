#include <numa.h>
#include <sched.h>
#include <sys/mman.h>

#include <atomic>
#include <future>

#include "sm-alloc.h"
#include "sm-chkpt.h"
#include "sm-common.h"
#include "sm-object.h"
#include "../txn.h"

namespace ermia {
namespace MM {

// tzwang (2015-11-01):
// gc_lsn is the LSN of the no-longer-needed versions, which is the end LSN
// of the epoch that most recently reclaimed (i.e., all **readers** are gone).
// It corresponds to two epochs ago of the most recently reclaimed epoch (in
// which all the **creators** are gone). Note that gc_lsn only records the
// LSN when the *creator* of the versions left---not the *visitor* of those
// versions. So consider the versions created in epoch N, we can have stragglers
// up to at most epoch N+2; to start a new epoch N+3, N must be reclaimed. This
// means versions last *visited* (not created!) in epoch N+2 are no longer
// needed after epoch N+4 is reclaimed.
//
// Note: when really recycling versions, we also need to make sure we leave
// one committed versions that is older than the begin LSN of any transaction.
// This "begin LSN of any tx" translates to the current log LSN for normal
// transactions, and the safesnap LSN for read-only transactions using a safe
// snapshot. For simplicity, the daemon simply leave one version with LSN < the
// gc_lsn available (so no need to figure out which safesnap lsn is the
// appropriate one to rely on - it changes as the daemon does its work).
uint64_t gc_lsn CACHE_ALIGNED;
epoch_num gc_epoch CACHE_ALIGNED;

static const uint64_t EPOCH_SIZE_NBYTES = 1 << 24;
static const uint64_t EPOCH_SIZE_COUNT = 2000;

// epoch_excl_begin_lsn belongs to the previous **ending** epoch, and we're
// using it as the **begin** lsn of the new epoch.
uint64_t epoch_excl_begin_lsn[3] = {0, 0, 0};
uint64_t epoch_reclaim_lsn[3] = {0, 0, 0};

static thread_local struct thread_data epoch_tls CACHE_ALIGNED;
epoch_mgr mm_epochs{{nullptr, &global_init, &get_tls, &thread_registered,
                     &thread_deregistered, &epoch_ended, &epoch_ended_thread,
                     &epoch_reclaimed}};

uint64_t safesnap_lsn = 0;

thread_local TlsFreeObjectPool *tls_free_object_pool CACHE_ALIGNED;
char **node_memory = nullptr;
uint64_t *allocated_node_memory = nullptr;
static uint64_t thread_local tls_allocated_node_memory CACHE_ALIGNED;
static const uint64_t tls_node_memory_mb = 200;

void prepare_node_memory() {
  ALWAYS_ASSERT(config::numa_nodes);
  allocated_node_memory =
      (uint64_t *)malloc(sizeof(uint64_t) * config::numa_nodes);
  node_memory = (char **)malloc(sizeof(char *) * config::numa_nodes);
  std::vector<std::future<void> > futures;
  LOG(INFO) << "Will run and allocate on " << config::numa_nodes << " nodes, "
            << config::node_memory_gb << "GB each";
  for (int i = 0; i < config::numa_nodes; i++) {
    LOG(INFO) << "Allocating " << config::node_memory_gb << "GB on node " << i;
    auto f = [=] {
      ALWAYS_ASSERT(config::node_memory_gb);
      allocated_node_memory[i] = 0;
      numa_set_preferred(i);
      node_memory[i] = (char *)mmap(
          nullptr, config::node_memory_gb * config::GB, PROT_READ | PROT_WRITE,
          MAP_ANONYMOUS | MAP_PRIVATE | MAP_HUGETLB | MAP_POPULATE, -1, 0);
      THROW_IF(node_memory[i] == nullptr or node_memory[i] == MAP_FAILED,
               os_error, errno, "Unable to allocate huge pages");
      ALWAYS_ASSERT(node_memory[i]);
      LOG(INFO) << "Allocated " << config::node_memory_gb << "GB on node " << i;
    };
    futures.push_back(std::async(std::launch::async, f));
  }
  for (auto &f : futures) {
    f.get();
  }
}

void free_node_memory() {
  ALWAYS_ASSERT(node_memory);
  ALWAYS_ASSERT(config::node_memory_gb);

  for (int i = 0; i < config::numa_nodes; i++) {
    allocated_node_memory[i] = 0;
    munmap(node_memory[i], config::node_memory_gb * config::GB);
  }

  free(allocated_node_memory);
  allocated_node_memory = nullptr;
  free(node_memory);
  node_memory = nullptr;
}

void gc_version_chain(fat_ptr *oid_entry) {
  fat_ptr ptr = *oid_entry;
  Object *cur_obj = (Object *)ptr.offset();
  if (!cur_obj) {
    // Tuple is deleted, skip
    return;
  }

  // Start from the first **committed** version, and delete after its next,
  // because the head might be still being modified (hence its _next field)
  // and might be gone any time (tx abort). Skip records in chkpt file as
  // well - not even in memory.
  auto clsn = cur_obj->GetClsn();
  fat_ptr *prev_next = nullptr;
  if (clsn.asi_type() == fat_ptr::ASI_CHK) {
    return;
  }
  if (clsn.asi_type() != fat_ptr::ASI_LOG) {
    DCHECK(clsn.asi_type() == fat_ptr::ASI_XID);
    ptr = cur_obj->GetNextVolatile();
    cur_obj = (Object *)ptr.offset();
  }

  // Now cur_obj should be the fisrt committed version, continue to the version
  // that can be safely recycled (the version after cur_obj).
  ptr = cur_obj->GetNextVolatile();
  prev_next = cur_obj->GetNextVolatilePtr();

  while (ptr.offset()) {
    cur_obj = (Object *)ptr.offset();
    clsn = cur_obj->GetClsn();
    if (clsn == NULL_PTR || clsn.asi_type() != fat_ptr::ASI_LOG) {
      // Might already got recycled, give up
      break;
    }
    ptr = cur_obj->GetNextVolatile();
    prev_next = cur_obj->GetNextVolatilePtr();
    // If the chkpt needs to be a consistent one, must make sure not to GC a
    // version that might be needed by chkpt:
    // uint64_t glsn = std::min(logmgr->durable_flushed_lsn().offset(),
    // volatile_read(gc_lsn));
    // This makes the GC thread has to traverse longer in the chain, unless
    // with small log buffers or frequent log flush, which is bad for disk
    // performance.  For good performance, we use inconsistent chkpt which
    // grabs the latest committed version directly. Log replay after the
    // chkpt-start lsn is necessary for correctness.
    uint64_t glsn = volatile_read(gc_lsn);
    if (LSN::from_ptr(clsn).offset() <= glsn && ptr._ptr) {
      // Fast forward to the **second** version < gc_lsn. Consider that we set
      // safesnap lsn to 1.8, and gc_lsn to 1.6. Assume we have two versions
      // with LSNs 2 and 1.5.  We need to keep the one with LSN=1.5 although
      // its < gc_lsn; otherwise the tx using safesnap won't be able to find
      // any version available.
      //
      // We only traverse and GC a version chain when an update transaction
      // successfully installed a version. So at any time there will be only
      // one guy possibly doing this for a version chain - just blind write. If
      // we're traversing at other times, e.g., after committed, then a CAS is
      // needed: __sync_bool_compare_and_swap(&prev_next->_ptr, ptr._ptr, 0)
      volatile_write(prev_next->_ptr, 0);
      while (ptr.offset()) {
        cur_obj = (Object *)ptr.offset();
        clsn = cur_obj->GetClsn();
        ALWAYS_ASSERT(clsn.asi_type() == fat_ptr::ASI_LOG);
        ALWAYS_ASSERT(LSN::from_ptr(clsn).offset() <= glsn);
        fat_ptr next_ptr = cur_obj->GetNextVolatile();
        cur_obj->SetClsn(NULL_PTR);
        cur_obj->SetNextVolatile(NULL_PTR);
        if (!tls_free_object_pool) {
          tls_free_object_pool = new TlsFreeObjectPool;
        }
        tls_free_object_pool->Put(ptr);
        ptr = next_ptr;
      }
      break;
    }
  }
}

void *allocate(size_t size) {
  size = align_up(size);
  void *p = NULL;

  // Try the tls free object store first
  if (tls_free_object_pool) {
    auto size_code = encode_size_aligned(size);
    fat_ptr ptr = tls_free_object_pool->Get(size_code);
    if (ptr.offset()) {
      p = (void *)ptr.offset();
      goto out;
    }
  }

  ALWAYS_ASSERT(not p);
  // Have to use the vanilla bump allocator, hopefully later we reuse them
  static thread_local char *tls_node_memory CACHE_ALIGNED;
  if (unlikely(not tls_node_memory) or
      tls_allocated_node_memory + size >= tls_node_memory_mb * config::MB) {
    tls_node_memory = (char *)allocate_onnode(tls_node_memory_mb * config::MB);
    tls_allocated_node_memory = 0;
  }

  if (likely(tls_node_memory)) {
    p = tls_node_memory + tls_allocated_node_memory;
    tls_allocated_node_memory += size;
    goto out;
  }

out:
  if (not p) {
    LOG(FATAL) << "Out of memory";
  }
  epoch_tls.nbytes += size;
  epoch_tls.counts += 1;
  return p;
}

// Allocate memory directly from the node pool
void *allocate_onnode(size_t size) {
  size = align_up(size);
  auto node = numa_node_of_cpu(sched_getcpu());
  ALWAYS_ASSERT(node < config::numa_nodes);
  auto offset = __sync_fetch_and_add(&allocated_node_memory[node], size);
  if (likely(offset + size <= config::node_memory_gb * config::GB)) {
    return node_memory[node] + offset;
  }
  return nullptr;
}

void deallocate(fat_ptr p) {
  ASSERT(p != NULL_PTR);
  ASSERT(p.size_code());
  ASSERT(p.size_code() != INVALID_SIZE_CODE);
  Object *obj = (Object *)p.offset();
  obj->SetNextVolatile(NULL_PTR);
  obj->SetClsn(NULL_PTR);
  if (!tls_free_object_pool) {
    tls_free_object_pool = new TlsFreeObjectPool;
  }
  tls_free_object_pool->Put(p);
}

// epoch mgr callbacks
void global_init(void *) {
  volatile_write(gc_lsn, 0);
  volatile_write(gc_epoch, 0);
}

epoch_mgr::tls_storage *get_tls(void *) {
  static thread_local epoch_mgr::tls_storage s;
  return &s;
}
void *thread_registered(void *) {
  epoch_tls.initialized = true;
  epoch_tls.nbytes = 0;
  epoch_tls.counts = 0;
  return &epoch_tls;
}

void thread_deregistered(void *cookie, void *thread_cookie) {
  MARK_REFERENCED(cookie);
  auto *t = (thread_data *)thread_cookie;
  ASSERT(t == &epoch_tls);
  t->initialized = false;
  t->nbytes = 0;
  t->counts = 0;
}

void *epoch_ended(void *cookie, epoch_num e) {
  MARK_REFERENCED(cookie);
  // remember the epoch number so we can find it out when it's reclaimed later
  epoch_num *epoch = (epoch_num *)malloc(sizeof(epoch_num));
  *epoch = e;
  return (void *)epoch;
}

void *epoch_ended_thread(void *cookie, void *epoch_cookie,
                         void *thread_cookie) {
  MARK_REFERENCED(cookie);
  MARK_REFERENCED(thread_cookie);
  return epoch_cookie;
}

void epoch_reclaimed(void *cookie, void *epoch_cookie) {
  MARK_REFERENCED(cookie);
  epoch_num e = *(epoch_num *)epoch_cookie;
  free(epoch_cookie);
  uint64_t my_begin_lsn = epoch_excl_begin_lsn[e % 3];
  if (!config::enable_gc || my_begin_lsn == 0) {
    return;
  }
  epoch_reclaim_lsn[e % 3] = my_begin_lsn;
  if (config::enable_safesnap) {
    // Make versions created during epoch N available for transactions
    // using safesnap. All transactions that created something in this
    // epoch has gone, so it's impossible for a reader using that
    // epoch's end lsn as both begin and commit timestamp to have
    // conflicts with these writer transactions. But future transactions
    // need to start with a pstamp of safesnap_lsn.
    // Take max here because after the loading phase we directly set
    // safesnap_lsn to log.cur_lsn, which might be actually larger than
    // safesnap_lsn generated during the loading phase, which is too
    // conservertive that some tx might not be able to see the tuples
    // because they were not loaded at that time...
    // Also use epoch e+1's begin LSN = epoch e's end LSN
    auto new_safesnap_lsn = epoch_excl_begin_lsn[(e + 1) % 3];
    volatile_write(safesnap_lsn, std::max(safesnap_lsn, new_safesnap_lsn));
  }
  if (e >= 2) {
    volatile_write(gc_lsn, epoch_reclaim_lsn[(e - 2) % 3]);
    volatile_write(gc_epoch, e - 2);
    epoch_reclaim_lsn[(e - 2) % 3] = 0;
  }
}

void epoch_exit(uint64_t s, epoch_num e) {
  // Transactions under a safesnap will pass s = 0 (INVALID_LSN)
  if (s != 0 && (epoch_tls.nbytes >= EPOCH_SIZE_NBYTES ||
                 epoch_tls.counts >= EPOCH_SIZE_COUNT)) {
    // epoch_ended() (which is called by new_epoch() in its critical section)
    // will pick up this safe lsn if new_epoch() succeeded (it could also be
    // set by somebody else who's also doing epoch_exit(), but this is captured
    // before new_epoch succeeds, so we're fine).  If instead, we do this in
    // epoch_ended(), that cur_lsn captured actually belongs to the *new* epoch
    // - not safe to gc based on this lsn. The real gc_lsn should be some lsn
    // at the end of the ending epoch, not some lsn after the next epoch.
    epoch_excl_begin_lsn[(e + 1) % 3] = s;
    if (mm_epochs.new_epoch_possible() && mm_epochs.new_epoch()) {
      epoch_tls.nbytes = epoch_tls.counts = 0;
    }
  }
  mm_epochs.thread_exit();
}
}  // namespace MM
}  // namespace ermia
