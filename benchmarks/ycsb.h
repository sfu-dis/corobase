#pragma once

#include <string>
#include <utility>
#include <vector>

#include "../third-party/foedus/zipfian_random.hpp"
#include "bench.h"

// FIXME(tzwang): since we don't have the read/write_all_fields knobs, here we
// assume one field
// In FOEDUS, we have 10 and with the knobs it can choose from any one field
// randomly.
const uint32_t kFields = 1;
const uint32_t kFieldLength = 10;
const uint64_t kRecordSize = kFieldLength * kFields;
const uint32_t kMaxWorkers = 1024;

struct YcsbRecord {
  char data_[kRecordSize];

  YcsbRecord() {}
  YcsbRecord(char value) { memset(data_, value, kFields * kFieldLength * sizeof(char)); }

  char* get_field(uint32_t f) { return data_ + f * kFieldLength; }
  static void initialize_field(char* field) {
    memset(field, 'a', kFieldLength);
  }
};

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

inline void BuildKey(uint64_t key, ermia::varstr &k) { *(uint64_t *)k.p = key; }

class ycsb_worker : public bench_worker {
public:
  ycsb_worker(unsigned int worker_id, unsigned long seed, ermia::Engine *db,
              const std::map<std::string, ermia::OrderedIndex *> &open_tables,
              spin_barrier *barrier_a, spin_barrier *barrier_b);

  virtual cmdlog_redo_workload_desc_vec
  get_cmdlog_redo_workload() const override {
    LOG(FATAL) << "Not applicable";
  }

  virtual workload_desc_vec get_workload() const override;

  static rc_t TxnInsert(bench_worker *w) {
    MARK_REFERENCED(w);
    return {RC_TRUE};
  }
  static rc_t TxnRead(bench_worker *w) {
    return static_cast<ycsb_worker *>(w)->txn_read();
  }
  static rc_t TxnReadAMAC(bench_worker *w) {
    return static_cast<ycsb_worker *>(w)->txn_read_amac();
  }
  static rc_t TxnReadCORO(bench_worker *w) {
    return static_cast<ycsb_worker *>(w)->txn_read_coro();
  }
  static rc_t TxnUpdate(bench_worker *w) {
    MARK_REFERENCED(w);
    return {RC_TRUE};
  }
  static rc_t TxnScan(bench_worker *w) {
    MARK_REFERENCED(w);
    return {RC_TRUE};
  }
  static rc_t TxnRMW(bench_worker *w) {
    return static_cast<ycsb_worker *>(w)->txn_rmw();
  }

  struct KeyCompare : public std::unary_function<ermia::varstr, bool> {
    explicit KeyCompare(ermia::varstr &baseline) : baseline(baseline) {}
    bool operator()(const ermia::varstr &arg) {
      return *(uint64_t *)arg.p == *(uint64_t *)baseline.p;
    }
    ermia::varstr &baseline;
  };

  rc_t txn_read();
  rc_t txn_read_amac();
  rc_t txn_read_coro();
  rc_t txn_rmw();

protected:
  ALWAYS_INLINE ermia::varstr &str(uint64_t size) { return *arena.next(size); }
  ermia::varstr &GenerateKey();

private:
  ermia::ConcurrentMasstreeIndex *tbl;
  foedus::assorted::UniformRandom uniform_rng;
  foedus::assorted::ZipfianRandom zipfian_rng;
  std::vector<ermia::ConcurrentMasstree::AMACState> as;
  std::vector<ermia::varstr *> keys;
  std::vector<ermia::varstr *> values;
};

class ycsb_usertable_loader : public bench_loader {
public:
  ycsb_usertable_loader(
      unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      uint32_t loader_id): bench_loader(seed, db, open_tables, loader_id), loader_id(loader_id) {}

protected:
  virtual void load() override;

private:
  uint32_t loader_id;
};
