#pragma once

#include "sm-config.h"
#include "sm-thread.h"
#include "sm-log-recover.h"

extern std::unordered_map<FID, ndb_ordered_index *> reverse_fid_map;

/* The base functor class that implements common methods needed
 * by most recovery methods. The specific recovery method can
 * inherit this guy and implement its own way of recovery, e.g.,
 * parallel replay by file/OID partition, etc.
 */
struct sm_log_recover_impl {
  void recover_insert(sm_log_scan_mgr::record_scan *logrec);
  void recover_index_insert(sm_log_scan_mgr::record_scan *logrec);
  void recover_update(sm_log_scan_mgr::record_scan *logrec, bool is_delete = false);
  fat_ptr recover_prepare_version(
                              sm_log_scan_mgr::record_scan *logrec,
                              fat_ptr next);
  ndb_ordered_index *recover_fid(sm_log_scan_mgr::record_scan *logrec);
  void recover_index_insert(
      sm_log_scan_mgr::record_scan *logrec, ndb_ordered_index *index);
  void rebuild_index(sm_log_scan_mgr *scanner, FID fid, ndb_ordered_index *index, LSN from, LSN to);

  // The main recovery function; the inheriting class should implement this
  // The implementation shall replay the log from position [from] until [to],
  // no more and no less; this is important for async log replay on backups.
  // Recovery at startup however can give [from]=chkpt_begin, and [to]=+inf
  // to replay the whole log.
  virtual void operator()(void *arg, sm_log_scan_mgr *scanner, LSN from, LSN to) = 0;
};

struct parallel_file_replay : public sm_log_recover_impl {
  struct redo_runner : public thread::sm_runner {
    parallel_file_replay *owner;
    FID fid;
    ndb_ordered_index *fid_index;
    bool done;

    redo_runner(parallel_file_replay *o, FID f, ndb_ordered_index *i) : 
      thread::sm_runner(), owner(o), fid(f), fid_index(i), done(false) {}
    virtual void my_work(char *);
    FID redo_file();
  };

  sm_log_scan_mgr *scanner;
  LSN start_lsn;
  LSN end_lsn;

  virtual void operator()(void *arg, sm_log_scan_mgr *scanner, LSN from, LSN to);
};

struct parallel_oid_replay : public sm_log_recover_impl {
  struct redo_runner : public thread::sm_runner {
    parallel_oid_replay *owner;
    OID oid_partition;
    bool done;

    redo_runner(parallel_oid_replay *o, OID part) :
      thread::sm_runner(), owner(o), oid_partition(part), done(false) {}
    virtual void my_work(char *);
    void redo_partition();
  };

  uint32_t nredoers;
  std::vector<struct redo_runner> redoers;
  sm_log_scan_mgr *scanner;
  LSN start_lsn;
  LSN end_lsn;

  parallel_oid_replay() : nredoers(sysconf::worker_threads) {}
  virtual void operator()(void *arg, sm_log_scan_mgr *scanner, LSN from, LSN to);
};
