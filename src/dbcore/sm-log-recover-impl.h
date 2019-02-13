#pragma once

#include "../ermia.h"
#include "sm-config.h"
#include "sm-thread.h"
#include "sm-log-recover.h"

namespace ermia {

/* The base functor class that implements common methods needed
 * by most recovery methods. The specific recovery method can
 * inherit this guy and implement its own way of recovery, e.g.,
 * parallel replay by file/OID partition, etc.
 */
struct sm_log_recover_impl {
  sm_log_recover_impl() {}
  virtual ~sm_log_recover_impl() {}
  void recover_insert(sm_log_scan_mgr::record_scan *logrec,
                      bool latest = false);
  void recover_index_insert(sm_log_scan_mgr::record_scan *logrec);
  void recover_update(sm_log_scan_mgr::record_scan *logrec, bool is_delete,
                      bool latest);
  void recover_update_key(sm_log_scan_mgr::record_scan *logrec);
  fat_ptr PrepareObject(sm_log_scan_mgr::record_scan *logrec);
  OrderedIndex *recover_fid(sm_log_scan_mgr::record_scan *logrec);
  void recover_index_insert(sm_log_scan_mgr::record_scan *logrec,
                            OrderedIndex *index);

  // The main recovery function; the inheriting class should implement this
  // The implementation shall replay the log from position [from] until [to],
  // no more and no less; this is important for async log replay on backups.
  // Recovery at startup however can give [from]=chkpt_begin, and [to]=+inf
  // to replay the whole log.
  virtual LSN operator()(void *arg, sm_log_scan_mgr *scanner, LSN from,
                         LSN to) = 0;
};

struct parallel_oid_replay : public sm_log_recover_impl {
  struct redo_runner : public thread::Runner {
    parallel_oid_replay *owner;
    OID oid_partition;
    bool done;
    LSN replayed_lsn;

    redo_runner(parallel_oid_replay *o, OID part)
        : thread::Runner(), owner(o), oid_partition(part), done(false), replayed_lsn(INVALID_LSN) {}
    virtual ~redo_runner() {}
    virtual void MyWork(char *);
    void redo_partition();
  };

  uint32_t nredoers;
  std::vector<struct redo_runner> redoers;
  sm_log_scan_mgr *scanner;
  LSN start_lsn;
  LSN end_lsn;

  parallel_oid_replay(uint32_t threads) : nredoers(threads) {}
  virtual ~parallel_oid_replay() {}
  virtual LSN operator()(void *arg, sm_log_scan_mgr *scanner, LSN from,
                         LSN to);
};

// A special case that each thread will replay a given range of LSN offsets
// that are guaranteed to respect log block/transaction boundaries. Used by
// replay during log shipping.
struct parallel_offset_replay : public sm_log_recover_impl {
  struct redo_runner : public thread::Runner {
    parallel_offset_replay *owner;
    // The half-open interval
    LSN start_lsn;
    LSN end_lsn;
    uint64_t redo_latency_us;
    uint64_t redo_size;
    uint64_t redo_batches;

    redo_runner(parallel_offset_replay *o, LSN start, LSN end)
        : thread::Runner(), owner(o), start_lsn(start),
          end_lsn(end), redo_latency_us(0), redo_size(0), redo_batches(0) {}
    virtual ~redo_runner() {}
    virtual void MyWork(char *);
    void redo_logbuf_partition();
    void persist_logbuf_partition();
  };

  uint32_t nredoers;
  std::vector<struct redo_runner *> redoers;
  sm_log_scan_mgr *scanner;

  parallel_offset_replay() : nredoers(config::replay_threads) {
    LOG(INFO) << "[Backup] " << nredoers << " replay threads";
  }
  virtual ~parallel_offset_replay() {}
  virtual LSN operator()(void *arg, sm_log_scan_mgr *scanner, LSN from,
                         LSN to);
};
}  // namespace ermia
