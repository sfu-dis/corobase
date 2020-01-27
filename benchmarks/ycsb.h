#pragma once

#include <string>
#include <utility>
#include <vector>

#include "../third-party/foedus/zipfian_random.hpp"
#include "bench.h"
#include "record/encoder.h"
#include "record/inline_str.h"
#include "../macros.h"

extern uint g_initial_table_size;
extern int g_zipfian_rng;
extern double g_zipfian_theta;

enum class ReadTransactionType {
  Sequential,
  AMACMultiGet,
  SimpleCoroMultiGet,
  AdvCoroMultiGet,
  SimpleCoro,
  AdvCoro
};

// TODO(tzwang); support other value length specified by user
#define YCSB_KEY_FIELDS(x, y) x(uint64_t, y_key)
#define YCSB_VALUE_FIELDS(x, y) x(inline_str_fixed<8>, y_value)
DO_STRUCT(ycsb_kv, YCSB_KEY_FIELDS, YCSB_VALUE_FIELDS);

struct YcsbWorkload {
  YcsbWorkload(char desc, int16_t insert_percent, int16_t read_percent,
               int16_t update_percent, int16_t scan_percent,
               int16_t rmw_percent)
      : desc(desc),
        insert_percent_(insert_percent),
        read_percent_(read_percent),
        update_percent_(update_percent),
        scan_percent_(scan_percent),
        rmw_percent_(rmw_percent),
        rmw_additional_reads_(0),
        reps_per_tx_(1),
        distinct_keys_(true) {}

  YcsbWorkload() {}
  int16_t insert_percent() const { return insert_percent_; }
  int16_t read_percent() const {
    return read_percent_ == 0 ? 0 : read_percent_ - insert_percent_;
  }
  int16_t update_percent() const {
    return update_percent_ == 0 ? 0 : update_percent_ - read_percent_;
  }
  int16_t scan_percent() const {
    return scan_percent_ == 0 ? 0 : scan_percent_ - update_percent_;
  }
  int16_t rmw_percent() const {
    return rmw_percent_ == 0 ? 0 : rmw_percent_ - scan_percent_;
  }

  char desc;
  // Cumulative percentage of i/r/u/s/rmw. From insert...rmw the percentages
  // accumulates, e.g., i=5, r=12 => we'll have 12-5=7% of reads in total.
  int16_t insert_percent_;
  int16_t read_percent_;
  int16_t update_percent_;
  int16_t scan_percent_;
  int16_t rmw_percent_;
  int32_t rmw_additional_reads_;
  int32_t reps_per_tx_;
  bool distinct_keys_;
};

inline void BuildKey(uint64_t key, ermia::varstr &k) {
  Encode(k, ycsb_kv::key(key));
}

class ycsb_usertable_loader : public bench_loader {
 public:
  ycsb_usertable_loader(unsigned long seed, ermia::Engine *db,
                        const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                        uint32_t loader_id)
      : bench_loader(seed, db, open_tables, loader_id), loader_id(loader_id) {}
 private:
  uint32_t loader_id;

 protected:
  void load();
};

void ycsb_create_db(ermia::Engine *db);
void ycsb_parse_options(int argc, char **argv);

template<class WorkerType>
class ycsb_bench_runner : public bench_runner {
 public:
  ycsb_bench_runner(ermia::Engine *db) : bench_runner(db) {
    ycsb_create_db(db);
  }

  virtual void prepare(char *) {
    open_tables["USERTABLE"] = ermia::TableDescriptor::GetPrimaryIndex("USERTABLE");
  }

 protected:
  virtual std::vector<bench_loader *> make_loaders() {
    uint64_t requested = g_initial_table_size;
    uint64_t records_per_thread = std::max<uint64_t>(1, g_initial_table_size / ermia::config::worker_threads);
    g_initial_table_size = records_per_thread * ermia::config::worker_threads;

    if (ermia::config::verbose) {
      std::cerr << "[INFO] requested for " << requested << " records, will load "
           << records_per_thread *ermia::config::worker_threads << std::endl;
    }

    std::vector<bench_loader *> ret;
    for (uint32_t i = 0; i < ermia::config::worker_threads; ++i) {
      ret.push_back(new ycsb_usertable_loader(0, db, open_tables, i));
    }
    return ret;
  }

  virtual std::vector<bench_worker *> make_cmdlog_redoers() {
    // Not implemented
    LOG(FATAL) << "Not applicable";
    std::vector<bench_worker *> ret;
    return ret;
  }

  virtual std::vector<bench_worker *> make_workers() {
    util::fast_random r(8544290);
    std::vector<bench_worker *> ret;
    for (size_t i = 0; i < ermia::config::worker_threads; i++) {
      ret.push_back(new WorkerType(i, r.next(), db, open_tables, &barrier_a, &barrier_b));
    }
    return ret;
  }
};

class ycsb_base_worker : public bench_worker {
 public:
  ycsb_base_worker(unsigned int worker_id, unsigned long seed, ermia::Engine *db,
                   const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                   spin_barrier *barrier_a, spin_barrier *barrier_b)
      : bench_worker(worker_id, true, seed, db, open_tables, barrier_a, barrier_b),
        table_index((ermia::ConcurrentMasstreeIndex*)open_tables.at("USERTABLE")),
        uniform_rng(1237 + worker_id) {
    if (g_zipfian_rng) {
      zipfian_rng.init(g_initial_table_size, g_zipfian_theta, 1237 + worker_id);
    }
  }

  virtual cmdlog_redo_workload_desc_vec get_cmdlog_redo_workload() const {
    LOG(FATAL) << "Not applicable";
  }

 protected:
  struct KeyCompare : public std::unary_function<ermia::varstr, bool> {
    explicit KeyCompare(ermia::varstr &baseline) : baseline(baseline) {}
    bool operator() (const ermia::varstr &arg) {
      return *(uint64_t*)arg.p == *(uint64_t*)baseline.p;
    }
    ermia::varstr &baseline;
  };

  ALWAYS_INLINE ermia::varstr &str(uint64_t size) { return *arena->next(size); }

  ermia::varstr &GenerateKey(ermia::transaction *t) {
    uint64_t r = 0;
    if (g_zipfian_rng) {
      r = zipfian_rng.next();
    } else {
      r = uniform_rng.uniform_within(0, g_initial_table_size - 1);
    }

    ermia::varstr &k = t ? *t->string_allocator().next(sizeof(uint64_t)) : str(sizeof(uint64_t));
    new (&k) ermia::varstr((char *)&k + sizeof(ermia::varstr), sizeof(uint64_t));
    ::BuildKey(r, k);
    return k;
  }

  ermia::ConcurrentMasstreeIndex *table_index;
  foedus::assorted::UniformRandom uniform_rng;
  foedus::assorted::ZipfianRandom zipfian_rng;
};
