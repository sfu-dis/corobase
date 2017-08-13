// -*- mode:c++ -*-
#ifndef __SM_LOG_RECOVER_H
#define __SM_LOG_RECOVER_H

#include "sm-log-offset.h"

#include <vector>

/* The log recovery manager.

   To avoid confusion and errors, we have to recover the existing log
   and checkpoints before we instantiate the part of the log capable
   of generating new records for the system.

   When it comes to testing and implementation, this class breaks the
   usual order of abstraction. We can't really test recovery until we
   have a reliable way to create log records, so sm_log_alloc_mgr
   (which uses this class) has to be tested first. Fortunately,
   there's little to be done here for a freshly created log, so we can
   get sm_log_alloc_mgr working before all the kinks have been ironed
   out here.
 */
struct sm_log_recover_mgr : sm_log_offset_mgr {
  /* No one log block is allowed to be larger, or contain more
     records, than this. Oversized records must be stored in another
     file and logged as relocations, and blocks with too many
     entries must overflow (careful to always reserve enough space
     for an overflow record!).

     The restriction is largely arbitrary, serving mostly to bound
     the size of the buffer required during log recovery.

     The block size sets the size of the buffer used to validate
     checksums. The record count controls the buffer space needed to
     reinstall records during startup.

     WARNING: MAX_BLOCK_SIZE must be acceptable for window_buffer
     (ie a power of two and not smaller than the system page size).
   */
  // static size_t const MAX_BLOCK_SIZE = 16*1024*1024;
  // Change this to allow using encoded size with rcu_alloc
  // (the size encoding can only represent values ranging from 0
  // to 950272).
  static size_t const MAX_BLOCK_SIZE = 512 * 1024;
  static size_t const MAX_BLOCK_RECORDS = 254;

  /* Starting from a known log block location, iterate over the log
     blocks in sequence.

     If [follow_overflow] is true, the iterator will follow overflow
     block chains, returning the oldest first. Note that overflow
     blocks cause the reported LSN to jump around.

     If [fetch_payloads] is true, the iterator will read in
     payloads. Otherwise, only the log header is fetched.

     WARNING: this iterator is only safe to use during startup/recovery.
  */
  struct block_scanner {
    static size_t const MIN_BLOCK_FETCH = log_block::size(MAX_BLOCK_RECORDS, 0);

    static log_block invalid_block() {
      return log_block{
          0, 0, INVALID_LSN, {LOG_NOP, INVALID_SIZE_CODE, 0, 0, {INVALID_LSN}}};
    }

    block_scanner(sm_log_recover_mgr *lm, LSN start, bool follow_overflow,
                  bool fetch_payloads, bool force_fetch_from_logbuf);
    ~block_scanner();

    bool valid() { return _cur_block and _cur_block->lsn != INVALID_LSN; }

    log_block *get() { return valid() ? _cur_block : NULL; }
    operator log_block *() { return get(); }
    log_block *operator->() { return get(); }
    log_block &operator*() { return *get(); }

    void operator++();

    void _load_block(LSN x, bool follow_overflow);

    // non-copyable, non-movable!
    block_scanner(block_scanner const &) = delete;
    block_scanner(block_scanner &&) = delete;
    void operator=(block_scanner) = delete;

    std::vector<LSN> _overflow_chain;
    sm_log_recover_mgr *_lm;
    bool _follow_overflow;
    bool _fetch_payloads;
    bool _force_fetch_from_logbuf;
    log_block *_buf;
    log_block *_cur_block;
  };

  /* Iterate over individual records in the log, starting from the
     LSN of a known log block. The iterator only stops on "normal"
     records (e.g. those having an FID/OID pair), and will not
     return raw skip or overflow records.

     Record payloads are only available if [fetch_payloads] was
     passed to the constructor.
  */
  struct log_scanner {
    struct decay {
      char *ptr;
      size_t size;

      template <typename T>
      operator T *() {
        ASSERT(sizeof(T) <= size);
        return (T *)ptr;
      }

      // don't use the template for these...
      operator void *() { return ptr; }
      operator char *() { return ptr; }
    };

    log_scanner(sm_log_recover_mgr *lm, LSN start, bool fetch_payloads,
                bool force_fetch_from_logbuf);

    bool valid() { return _bscan.valid() and _i < _bscan->nrec; }

    log_record *get() { return valid() ? &_bscan->records[_i] : 0; }
    operator log_record *() { return get(); }

    log_record *operator->() { return *this; }

    LSN payload_lsn() { return _bscan->payload_lsn(_i); }

    size_t payload_size() { return _bscan->payload_size(_i); }

    decay payload() {
      char *p = has_payloads ? _bscan->payload(_i) : nullptr;
      return decay{p, payload_size()};
    }

    void operator++();

    void _find_record();

    block_scanner _bscan;
    size_t _i;
    bool has_payloads;
  };

  /* Load the object referenced by [ptr] from the log. The pointer
     must reference the log (ASI_LOG) and the given buffer must be large
     enough to hold the object.
   */
  void load_object(char *buf, size_t bufsz, fat_ptr ptr,
                   int align_bits = DEFAULT_ALIGNMENT_BITS);
  void load_object_from_logbuf(char *buf, size_t bufsz, fat_ptr ptr,
                               int align_bits = DEFAULT_ALIGNMENT_BITS);

  /* A convenience method that can be used instead of load_object
     when the object to be loaded is an ext_ptr payload.
   */
  fat_ptr load_ext_pointer(fat_ptr ptr);

  sm_log_recover_mgr(sm_log_recover_impl *rf, void *rf_arg);

  ~sm_log_recover_mgr();

  sm_log_scan_mgr *scanner;
  sm_log_scan_mgr *
      logbuf_scanner;  // Dedicated for backup to redo from the logbuf
  sm_log_recover_impl *recover_functor;
  sm_log_recover_impl *logbuf_redo_functor;
  void *recover_functor_arg;

  void redo_log(LSN start_lsn, LSN end_lsn);
  // For log shipping only
  void start_logbuf_redoers();
  void recover();
};

#endif
