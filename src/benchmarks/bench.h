#pragma once

#include <stdint.h>

#include <map>
#include <vector>
#include <utility>
#include <string>

#include <gflags/gflags.h>

#include "ndb_wrapper.h"
#include "ordered_index.h"
#include "../macros.h"
#include "../txn.h"
#include "../util.h"
#include "../spinbarrier.h"
#include "../dbcore/sm-config.h"
#include "../dbcore/rcu.h"
#include "../dbcore/serial.h"
#include "../dbcore/sm-log.h"
#include "../dbcore/sm-alloc.h"
#include "../dbcore/sm-oid.h"
#include "../dbcore/sm-rc.h"
#include "../dbcore/sm-thread.h"

#include <stdio.h>
#include <sys/mman.h> // Needed for mlockall()
#include <sys/time.h> // needed for getrusage
#include <sys/resource.h> // needed for getrusage
#include <numa.h>
#include <vector>
#include <set>

extern void ycsb_do_test(ndb_wrapper *db, int argc, char **argv);
extern void tpcc_do_test(ndb_wrapper *db, int argc, char **argv);
extern void tpce_do_test(ndb_wrapper *db, int argc, char **argv);

enum {
  RUNMODE_TIME = 0,
  RUNMODE_OPS  = 1
};

// benchmark global variables
extern volatile bool running;

template <typename T> static std::vector<T>
unique_filter(const std::vector<T> &v)
{
	std::set<T> seen;
	std::vector<T> ret;
	for (auto &e : v)
		if (!seen.count(e)) {
			ret.emplace_back(e);
			seen.insert(e);
		}
	return ret;
}

class bench_loader : public thread::sm_runner {
public:
  bench_loader(unsigned long seed, ndb_wrapper *db,
               const std::map<std::string, OrderedIndex*> &open_tables)
    : sm_runner(), r(seed), db(db), open_tables(open_tables)
  {
    // don't try_instantiate() here; do it when we start to load. The way we reuse
    // threads relies on this fact (see bench_runner::run()).
    txn_obj_buf = (transaction*)malloc(sizeof(transaction));
  }

  virtual ~bench_loader() {}
  inline ALWAYS_INLINE varstr &
  str(uint64_t size)
  {
    return *arena.next(size);
  }

private:
  virtual void my_work(char *)
  {
    load();
  }

protected:
  inline transaction *txn_buf() { return txn_obj_buf; }
  virtual void load() = 0;

  util::fast_random r;
  ndb_wrapper *const db;
  std::map<std::string, OrderedIndex*> open_tables;
  transaction* txn_obj_buf;
  str_arena arena;
};

typedef std::tuple<uint64_t, uint64_t, uint64_t, uint64_t> tx_stat;
typedef std::map<std::string, tx_stat> tx_stat_map;

class bench_worker : public thread::sm_runner {
  friend class sm_log_alloc_mgr;
public:

  bench_worker(unsigned int worker_id,
               unsigned long seed, ndb_wrapper *db,
               const std::map<std::string, OrderedIndex*> &open_tables,
               spin_barrier *barrier_a, spin_barrier *barrier_b)
    : sm_runner(),
      worker_id(worker_id),
      r(seed), db(db), open_tables(open_tables),
      barrier_a(barrier_a), barrier_b(barrier_b),
      latency_numer_us(0),
      backoff_shifts(0), // spin between [0, 2^backoff_shifts) times before retry
      // the ntxn_* numbers are per worker
      ntxn_commits(0),
      ntxn_aborts(0),
      ntxn_user_aborts(0),
      ntxn_int_aborts(0),
      ntxn_si_aborts(0),
      ntxn_serial_aborts(0),
      ntxn_rw_aborts(0),
      ntxn_phantom_aborts(0),
      ntxn_query_commits(0)
  {
    txn_obj_buf = (transaction*)malloc(sizeof(transaction));
    try_impersonate();
  }

  typedef rc_t (*txn_fn_t)(bench_worker *);

  struct workload_desc {
    workload_desc() {}
    workload_desc(const std::string &name, double frequency, txn_fn_t fn)
      : name(name), frequency(frequency), fn(fn)
    {
      ALWAYS_ASSERT(frequency > 0.0);
      ALWAYS_ASSERT(frequency <= 1.0);
    }
    std::string name;
    double frequency;
    txn_fn_t fn;
  };
  typedef std::vector<workload_desc> workload_desc_vec;
  virtual workload_desc_vec get_workload() const = 0;

  inline size_t get_ntxn_commits() const { return ntxn_commits; }
  inline size_t get_ntxn_aborts() const { return ntxn_aborts; }
  inline size_t get_ntxn_user_aborts() const { return ntxn_user_aborts; }
  inline size_t get_ntxn_si_aborts() const { return ntxn_si_aborts; }
  inline size_t get_ntxn_serial_aborts() const { return ntxn_serial_aborts; }
  inline size_t get_ntxn_rw_aborts() const { return ntxn_rw_aborts; }
  inline size_t get_ntxn_int_aborts() const { return ntxn_int_aborts; }
  inline size_t get_ntxn_phantom_aborts() const { return ntxn_phantom_aborts; }
  inline size_t get_ntxn_query_commits() const { return ntxn_query_commits; }
  inline void inc_ntxn_user_aborts() { ++ntxn_user_aborts; }
  inline void inc_ntxn_si_aborts() { ++ntxn_si_aborts; }
  inline void inc_ntxn_serial_aborts() { ++ntxn_serial_aborts; }
  inline void inc_ntxn_rw_aborts() { ++ntxn_rw_aborts; }
  inline void inc_ntxn_int_aborts() { ++ntxn_int_aborts; }
  inline void inc_ntxn_phantom_aborts() { ++ntxn_phantom_aborts; }
  inline void inc_ntxn_query_commits() { ++ntxn_query_commits; }

  inline uint64_t get_latency_numer_us() const { return volatile_read(latency_numer_us); }

  inline double
  get_avg_latency_us() const
  {
    return double(latency_numer_us) / double(ntxn_commits);
  }

  const tx_stat_map get_txn_counts() const;

  typedef ndb_wrapper::counter_map counter_map;
  typedef ndb_wrapper::txn_counter_map txn_counter_map;

#ifdef ENABLE_BENCH_TXN_COUNTERS
  inline txn_counter_map
  get_local_txn_counters() const
  {
    return local_txn_counters;
  }
#endif

private:
  virtual void my_work(char *);

protected:
  inline transaction *txn_buf() { return txn_obj_buf; }

  unsigned int worker_id;
  util::fast_random r;
  ndb_wrapper *const db;
  std::map<std::string, OrderedIndex*> open_tables;
  spin_barrier *const barrier_a;
  spin_barrier *const barrier_b;

private:
  uint64_t latency_numer_us;
  unsigned backoff_shifts;

  // stats
  size_t ntxn_commits;
  size_t ntxn_aborts;
  size_t ntxn_user_aborts;
  size_t ntxn_int_aborts;
  size_t ntxn_si_aborts;
  size_t ntxn_serial_aborts;
  size_t ntxn_rw_aborts;
  size_t ntxn_phantom_aborts;
  size_t ntxn_query_commits;

protected:

#ifdef ENABLE_BENCH_TXN_COUNTERS
  txn_counter_map local_txn_counters;
  void measure_txn_counters(void *txn, const char *txn_name);
#else
  inline ALWAYS_INLINE void measure_txn_counters(void *txn, const char *txn_name) {}
#endif

  std::vector<tx_stat> txn_counts; // commits and aborts breakdown

  transaction* txn_obj_buf;
  str_arena arena;
};

class bench_runner {
public:
  bench_runner(const bench_runner &) = delete;
  bench_runner(bench_runner &&) = delete;
  bench_runner &operator=(const bench_runner &) = delete;

  bench_runner(ndb_wrapper *db)
    : db(db), barrier_a(config::worker_threads), barrier_b(1) {}
  virtual ~bench_runner() {}
  virtual void prepare(char *) = 0;
  void run();
  void create_files_task(char *);
  void start_measurement();

  static std::vector<bench_worker*> workers;

protected:
  // only called once
  virtual std::vector<bench_loader*> make_loaders() = 0;

  // only called once
  virtual std::vector<bench_worker*> make_workers() = 0;

  ndb_wrapper *const db;
  std::map<std::string, OrderedIndex*> open_tables;

  // barriers for actual benchmark execution
  spin_barrier barrier_a;
  spin_barrier barrier_b;
};

// XXX(stephentu): limit_callback is not optimal, should use
// static_limit_callback if possible
class limit_callback : public OrderedIndex::scan_callback {
public:
  limit_callback(ssize_t limit = -1)
    : limit(limit), n(0)
  {
    ALWAYS_ASSERT(limit == -1 || limit > 0);
  }

  virtual bool invoke(
      const char *keyp, size_t keylen,
      const varstr &value)
  {
    ASSERT(limit == -1 || n < size_t(limit));
    values.emplace_back(varstr(keyp, keylen), value);
    return (limit == -1) || (++n < size_t(limit));
  }

  typedef std::pair<varstr, varstr> kv_pair;
  std::vector<kv_pair> values;

  const ssize_t limit;
private:
  size_t n;
};

// Note: try_catch_cond_abort might call __abort_txn with rc=RC_FALSE
// so no need to assure rc must be RC_ABORT_*.
#define __abort_txn(r) \
{   \
  db->abort_txn(txn); \
  if (not rc_is_abort(r)) \
    return {RC_ABORT_USER}; \
  return r; \
}

// NOTE: only use these in transaction benchmark (e.g., TPCC) code, not in engine code

// reminescent the try...catch block:
// if return code is one of those RC_ABORT* then abort
#define try_catch(rc) \
{ \
  rc_t r = rc; \
  if (rc_is_abort(r)) \
    __abort_txn(r); \
}

// same as try_catch but don't do abort, only return rc
// So far the only user is TPC-E's TxnHarness***.h.
#define try_return(rc) \
{ \
  rc_t r = rc; \
  if (rc_is_abort(r)) \
    return r; \
}

#define try_tpce_output(op) \
{ \
  rc_t r = op; \
  if (rc_is_abort(r)) \
    return r; \
  if (output.status == 0) \
    return {RC_TRUE}; \
  return {RC_ABORT_USER}; \
}

// if rc == RC_FALSE then do op
#define try_catch_cond(rc, op) \
{ \
  rc_t r = rc; \
  if (rc_is_abort(r)) \
    __abort_txn(r); \
  if (r._val == RC_FALSE) \
    op; \
}

#define try_catch_cond_abort(rc) \
{ \
  rc_t r = rc; \
  if (rc_is_abort(r) or r._val == RC_FALSE) \
    __abort_txn(r); \
}
// combines the try...catch block with ALWAYS_ASSERT and allows abort.
// The rc_is_abort case is there because sometimes we want to make
// sure say, a get, succeeds, but the read itsef could also cause
// abort (by SSN). Use try_verify_strict if you need rc=true.
#define try_verify_relax(oper) \
{ \
  rc_t r = oper;   \
  ALWAYS_ASSERT(r._val == RC_TRUE or rc_is_abort(r)); \
  if (rc_is_abort(r))  \
    __abort_txn(r);  \
}

// No abort is allowed, usually for loading
#define try_verify_strict(oper) \
{ \
  rc_t rc = oper;   \
  ALWAYS_ASSERT(rc._val == RC_TRUE);    \
}
