#include <numa.h>
#include <sched.h>
#include <sys/mman.h>

#include <atomic>
#include <future>
#include "libpmem.h"

#include "sm-alloc.h"
#include "sm-chkpt.h"
#include "sm-common.h"
#include "sm-object.h"
#include "../txn.h"

namespace ermia {
namespace MM {

thread_local TlsFreeObjectPool *tls_free_object_pool_pmem CACHE_ALIGNED;
char **node_pmem = nullptr;
uint64_t *allocated_node_pmem = nullptr;
static uint64_t thread_local tls_allocated_node_pmem CACHE_ALIGNED;
static const uint64_t tls_node_pmem_mb = 200;

static const char *pmem_paths[] = {"/mnt/pmem0/corobase/pool_numa0",
	                           "/mnt/pmem1/corobase/pool_numa1"};

void prepare_node_pmem() {
  ALWAYS_ASSERT(config::numa_nodes);
  allocated_node_pmem = (uint64_t *)malloc(sizeof(uint64_t) * config::numa_nodes);
  node_pmem = (char **)malloc(sizeof(char *) * config::numa_nodes);
  std::vector<std::future<void> > futures;
  for (int i = 0; i < config::numa_nodes; i++) {
    auto f = [=] {
      ALWAYS_ASSERT(config::node_memory_gb);
      allocated_node_pmem[i] = 0;
      size_t mmaped_len = 0;
      int is_pmem = 0;
      node_pmem[i] = (char *)pmem_map_file(pmem_paths[i], config::node_memory_gb * config::GB,
		                             PMEM_FILE_CREATE | PMEM_FILE_SPARSE, (S_IWUSR | S_IRUSR),
					     &mmaped_len, &is_pmem);
      //memset(node_pmem[i], 0, config::node_memory_gb * config::GB);
      ALWAYS_ASSERT(is_pmem);
      ALWAYS_ASSERT(mmaped_len == config::node_memory_gb * config::GB);
      ALWAYS_ASSERT(node_pmem[i]);
    };
    futures.push_back(std::async(std::launch::async, f));
  }
  for (auto &f : futures) {
    f.get();
  }
}

void *allocate_pmem(size_t size) {
  size = align_up(size);
  void *p = NULL;

  // Try the tls free object store first
  if (tls_free_object_pool_pmem) {
    auto size_code = encode_size_aligned(size);
    fat_ptr ptr = tls_free_object_pool_pmem->Get(size_code);
    if (ptr.offset()) {
      p = (void *)ptr.offset();
      goto out;
    }
  }

  ALWAYS_ASSERT(not p);
  // Have to use the vanilla bump allocator, hopefully later we reuse them
  static thread_local char *tls_node_pmem CACHE_ALIGNED;
  if (unlikely(not tls_node_pmem) or
      tls_allocated_node_pmem + size >= tls_node_pmem_mb * config::MB) {
    tls_node_pmem = (char *)allocate_onnode_pmem(tls_node_pmem_mb * config::MB);
    tls_allocated_node_pmem = 0;
  }

  if (likely(tls_node_pmem)) {
    p = tls_node_pmem + tls_allocated_node_pmem;
    tls_allocated_node_pmem += size;
    goto out;
  }

out:
  if (not p) {
    LOG(FATAL) << "Out of memory";
  }
  return p;
}

// Allocate memory directly from the node pool
void *allocate_onnode_pmem(size_t size) {
  size = align_up(size);
  auto node = numa_node_of_cpu(sched_getcpu());
  ALWAYS_ASSERT(node < config::numa_nodes);
  auto offset = __sync_fetch_and_add(&allocated_node_pmem[node], size);
  if (likely(offset + size <= config::node_memory_gb * config::GB)) {
    return node_pmem[node] + offset;
  }
  return nullptr;
}

void deallocate_pmem(fat_ptr p) {
  ASSERT(p != NULL_PTR);
  ASSERT(p.size_code());
  ASSERT(p.size_code() != INVALID_SIZE_CODE);
  Object *obj = (Object *)p.offset();
  obj->SetNextVolatile(NULL_PTR);
  obj->SetClsn(NULL_PTR);
  if (!tls_free_object_pool_pmem) {
    tls_free_object_pool_pmem = new TlsFreeObjectPool;
  }
  tls_free_object_pool_pmem->Put(p);
}

}  // namespace MM
}  // namespace ermia
