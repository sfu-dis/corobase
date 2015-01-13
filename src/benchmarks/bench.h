#ifndef _NDB_BENCH_H_
#define _NDB_BENCH_H_

#include <stdint.h>

#include <map>
#include <vector>
#include <utility>
#include <string>

#include "abstract_db.h"
#include "../macros.h"
#include "../thread.h"
#include "../util.h"
#include "../spinbarrier.h"
#include "../dbcore/rcu.h"
#include "../dbcore/sm-alloc.h"
#include "../dbcore/ssn.h"
#include "../dbcore/sm-trace.h"
#include "../dbcore/sm-rc.h"
#include <stdio.h>
#include <sys/mman.h> // Needed for mlockall()
#include <malloc.h>
#include <sys/time.h> // needed for getrusage
#include <sys/resource.h> // needed for getrusage
#include <numa.h>

extern void ycsb_do_test(abstract_db *db, int argc, char **argv);
extern void tpcc_do_test(abstract_db *db, int argc, char **argv);
extern void queue_do_test(abstract_db *db, int argc, char **argv);
extern void encstress_do_test(abstract_db *db, int argc, char **argv);
extern void bid_do_test(abstract_db *db, int argc, char **argv);

enum {
  RUNMODE_TIME = 0,
  RUNMODE_OPS  = 1
};

// benchmark global variables
extern size_t nthreads;
extern volatile bool running;
extern int verbose;
extern uint64_t txn_flags;
extern double scale_factor;
extern uint64_t runtime;
extern uint64_t ops_per_worker;
extern int run_mode;
extern int enable_parallel_loading;
extern int pin_cpus;
extern int slow_exit;
extern int retry_aborted_transaction;
extern int no_reset_counters;
extern int backoff_aborted_transaction;

class scoped_db_thread_ctx {
public:
  scoped_db_thread_ctx(const scoped_db_thread_ctx &) = delete;
  scoped_db_thread_ctx(scoped_db_thread_ctx &&) = delete;
  scoped_db_thread_ctx &operator=(const scoped_db_thread_ctx &) = delete;

  scoped_db_thread_ctx(abstract_db *db, bool loader)
    : db(db)
  {
    db->thread_init(loader);
  }
  ~scoped_db_thread_ctx()
  {
    db->thread_end();
  }
private:
  abstract_db *const db;
};

class bench_loader : public ndb_thread {
public:
  bench_loader(unsigned long seed, abstract_db *db,
               const std::map<std::string, abstract_ordered_index *> &open_tables)
    : r(seed), db(db), open_tables(open_tables), b(0)
  {
    txn_obj_buf.reserve(str_arena::MinStrReserveLength);
    txn_obj_buf.resize(db->sizeof_txn_object(txn_flags));
  }
  inline void
  set_barrier(spin_barrier &b)
  {
    ALWAYS_ASSERT(!this->b);
    this->b = &b;
  }
  virtual void
  run()
  {
#ifdef USE_PARALLEL_SSN
    assign_reader_bitmap_entry();
#endif
	  // XXX. RCU register/deregister should be the outer most one b/c RA::ra_deregister could call cur_lsn inside
	RCU::rcu_register();
	RA::ra_register();
    ALWAYS_ASSERT(b);
    b->count_down();
    b->wait_for();
    scoped_db_thread_ctx ctx(db, true);
    load();
	RA::ra_deregister();
	RCU::rcu_deregister();
#ifdef USE_PARALLEL_SSN
    deassign_reader_bitmap_entry();
#endif
  }
protected:
  inline void *txn_buf() { return (void *) txn_obj_buf.data(); }

  virtual void load() = 0;

  util::fast_random r;
  abstract_db *const db;
  std::map<std::string, abstract_ordered_index *> open_tables;
  spin_barrier *b;
  std::string txn_obj_buf;
  str_arena arena;
};

class bench_worker : public ndb_thread {
public:

  bench_worker(unsigned int worker_id,
               bool set_core_id,
               unsigned long seed, abstract_db *db,
               const std::map<std::string, abstract_ordered_index *> &open_tables,
               spin_barrier *barrier_a, spin_barrier *barrier_b)
    : worker_id(worker_id), set_core_id(set_core_id),
      r(seed), db(db), open_tables(open_tables),
      barrier_a(barrier_a), barrier_b(barrier_b),
      // the ntxn_* numbers are per worker
      ntxn_commits(0), ntxn_aborts(0),
      latency_numer_us(0),
      backoff_shifts(0), // spin between [0, 2^backoff_shifts) times before retry
      size_delta(0)
  {
    txn_obj_buf.reserve(str_arena::MinStrReserveLength);
    txn_obj_buf.resize(db->sizeof_txn_object(txn_flags));
  }

  virtual ~bench_worker() {}

  // returns [did_commit?, size_increase_bytes]
  typedef std::pair<bool, ssize_t> txn_result;
  typedef txn_result (*txn_fn_t)(bench_worker *);

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

  virtual void run();


  inline size_t get_ntxn_commits() const { return ntxn_commits; }
  inline size_t get_ntxn_aborts() const { return ntxn_aborts; }

  inline uint64_t get_latency_numer_us() const { return latency_numer_us; }

  inline double
  get_avg_latency_us() const
  {
    return double(latency_numer_us) / double(ntxn_commits);
  }

  std::map<std::string, size_t> get_txn_counts() const;

  typedef abstract_db::counter_map counter_map;
  typedef abstract_db::txn_counter_map txn_counter_map;

#ifdef ENABLE_BENCH_TXN_COUNTERS
  inline txn_counter_map
  get_local_txn_counters() const
  {
    return local_txn_counters;
  }
#endif

  inline ssize_t get_size_delta() const { return size_delta; }

protected:

  virtual void on_run_setup() {}

  inline void *txn_buf() { return (void *) txn_obj_buf.data(); }

  unsigned int worker_id;
  bool set_core_id;
  util::fast_random r;
  abstract_db *const db;
  std::map<std::string, abstract_ordered_index *> open_tables;
  spin_barrier *const barrier_a;
  spin_barrier *const barrier_b;

private:
  size_t ntxn_commits;
  size_t ntxn_aborts;
  uint64_t latency_numer_us;
  unsigned backoff_shifts;

protected:

#ifdef ENABLE_BENCH_TXN_COUNTERS
  txn_counter_map local_txn_counters;
  void measure_txn_counters(void *txn, const char *txn_name);
#else
  inline ALWAYS_INLINE void measure_txn_counters(void *txn, const char *txn_name) {}
#endif

  std::vector<size_t> txn_counts; // breakdown of txns
  ssize_t size_delta; // how many logical bytes (of values) did the worker add to the DB

  std::string txn_obj_buf;
  str_arena arena;
};

class bench_runner {
public:
  bench_runner(const bench_runner &) = delete;
  bench_runner(bench_runner &&) = delete;
  bench_runner &operator=(const bench_runner &) = delete;

  bench_runner(abstract_db *db)
    : db(db), barrier_a(nthreads), barrier_b(1) {}
  virtual ~bench_runner() {}
  void run();
  void heap_prefault()
  {
	  //uint64_t FAULT_SIZE = (((uint64_t)1<<30)*40);		// 45G for 24 warehouses
	  //uint8_t* p = (uint8_t*)malloc( FAULT_SIZE );
	  //ALWAYS_ASSERT(p);
      //ALWAYS_ASSERT(not mlock(p, FAULT_SIZE));
	  //ALWAYS_ASSERT(not mlockall(MCL_CURRENT));
	  mallopt (M_TRIM_THRESHOLD, -1);
	  mallopt (M_MMAP_MAX, 0);

	  struct rusage usage;
	  getrusage(RUSAGE_SELF, &usage);
	  std::cout<<"Major fault: " <<  usage.ru_majflt<< "Minor fault: " << usage.ru_minflt<< std::endl;

	  //free(p);
  }
protected:
  // only called once
  virtual std::vector<bench_loader*> make_loaders() = 0;

  // only called once
  virtual std::vector<bench_worker*> make_workers() = 0;

  abstract_db *const db;
  std::map<std::string, abstract_ordered_index *> open_tables;

  // barriers for actual benchmark execution
  spin_barrier barrier_a;
  spin_barrier barrier_b;
};

// XXX(stephentu): limit_callback is not optimal, should use
// static_limit_callback if possible
class limit_callback : public abstract_ordered_index::scan_callback {
public:
  limit_callback(ssize_t limit = -1)
    : limit(limit), n(0)
  {
    ALWAYS_ASSERT(limit == -1 || limit > 0);
  }

  virtual bool invoke(
      const char *keyp, size_t keylen,
      const std::string &value)
  {
    INVARIANT(limit == -1 || n < size_t(limit));
    values.emplace_back(std::string(keyp, keylen), value);
    return (limit == -1) || (++n < size_t(limit));
  }

  typedef std::pair<std::string, std::string> kv_pair;
  std::vector<kv_pair> values;

  const ssize_t limit;
private:
  size_t n;
};


class latest_key_callback : public abstract_ordered_index::scan_callback {
public:
  latest_key_callback(std::string &k, ssize_t limit = -1)
    : limit(limit), n(0), k(&k)
  {
    ALWAYS_ASSERT(limit == -1 || limit > 0);
  }

  virtual bool invoke(
      const char *keyp, size_t keylen,
      const std::string &value)
  {
    INVARIANT(limit == -1 || n < size_t(limit));
    k->assign(keyp, keylen);
    ++n;
    return (limit == -1) || (n < size_t(limit));
  }

  inline size_t size() const { return n; }
  inline std::string &kstr() { return *k; }

private:
  ssize_t limit;
  size_t n;
  std::string *k;
};

// explicitly copies keys, because btree::search_range_call() interally
// re-uses a single string to pass keys (so using standard string assignment
// will force a re-allocation b/c of shared ref-counting)
//
// this isn't done for values, because each value has a distinct string from
// the string allocator, so there are no mutations while holding > 1 ref-count
template <size_t N>
class static_limit_callback : public abstract_ordered_index::scan_callback {
public:
  // XXX: push ignore_key into lower layer
  static_limit_callback(str_arena *arena, bool ignore_key)
    : n(0), arena(arena), ignore_key(ignore_key)
  {
    static_assert(N > 0, "xx");
  }

  virtual bool invoke(
      const char *keyp, size_t keylen,
      const std::string &value)
  {
    INVARIANT(n < N);
    INVARIANT(arena->manages(&value));
    if (ignore_key) {
      values.emplace_back(nullptr, &value);
    } else {
      std::string * const s_px = arena->next();
      INVARIANT(s_px && s_px->empty());
      s_px->assign(keyp, keylen);
      values.emplace_back(s_px, &value);
    }
    return ++n < N;
  }

  inline size_t
  size() const
  {
    return values.size();
  }

  typedef std::pair<const std::string *, const std::string *> kv_pair;
  typename util::vec<kv_pair, N>::type values;

private:
  size_t n;
  str_arena *arena;
  bool ignore_key;
};

#define __abort_txn \
{   \
  db->abort_txn(txn); \
  return bench_worker::txn_result(false, 0); \
}

// NOTE: only use these in transaction benchmark (e.g., TPCC) code, not in engine code

// reminescent the try...catch block:
// if return code is one of those RC_ABORT* then abort
#define try_catch(rc) \
{ \
  if (rc_is_abort(rc)) \
    __abort_txn; \
}

// if rc == RC_FALSE then do op
#define try_catch_cond(rc, op) \
{ \
  rc_t r = rc; \
  if (rc_is_abort(r)) \
    __abort_txn; \
  if (r._val == RC_FALSE) \
    op; \
}

#define try_catch_cond_abort(rc) try_catch_cond(rc, __abort_txn)

// combines the try...catch block with ALWAYS_ASSERT
// TODO: change to use rc_t by bits, i.e., set abort bit and
// return value (true/false) bit, rather than use the whole int.
// The rc_is_abort case is there because sometimes we want to make
// sure say, a get, succeeds, but the read itsef could also cause
// abort (by SSN).
#define try_verify(oper) \
{ \
  rc_t rc = oper;   \
  ALWAYS_ASSERT(rc._val == RC_TRUE or rc_is_abort(rc)); \
}

#endif /* _NDB_BENCH_H_ */
