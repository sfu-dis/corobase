/*
 * A YCSB implementation based off of Silo's and equivalent to FOEDUS's.
 */
#include "bench.h"
#include "ycsb.h"

#ifndef ADV_COROUTINE

extern uint g_reps_per_tx;
extern uint g_rmw_additional_reads;
extern YcsbWorkload ycsb_workload;
extern ReadTransactionType g_read_txn_type;

class ycsb_sequential_worker : public ycsb_base_worker {
 public:
  ycsb_sequential_worker(unsigned int worker_id, unsigned long seed, ermia::Engine *db,
                         const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                         spin_barrier *barrier_a, spin_barrier *barrier_b)
    : ycsb_base_worker(worker_id, seed, db, open_tables, barrier_a, barrier_b) {
  }

  virtual workload_desc_vec get_workload() const {
    workload_desc_vec w;
    if (ycsb_workload.insert_percent() || ycsb_workload.update_percent()) {
      LOG(FATAL) << "Not implemented";
    }

    if (ycsb_workload.read_percent()) {
      if (g_read_txn_type == ReadTransactionType::AMACMultiGet) {
        w.push_back(workload_desc("Read", double(ycsb_workload.read_percent()) / 100.0, TxnReadAMACMultiGet));
      } else if (g_read_txn_type == ReadTransactionType::SimpleCoroMultiGet) {
        w.push_back(workload_desc("Read", double(ycsb_workload.read_percent()) / 100.0, TxnReadSimpleCoroMultiGet));
      } else if (g_read_txn_type == ReadTransactionType::Sequential) {
        w.push_back(workload_desc("Read", double(ycsb_workload.read_percent()) / 100.0, TxnRead));
      } else {
        LOG(FATAL) << "Wrong read txn type. Supported: sequential, multiget-simple-coro, multiget-adv-coro";
      }
    }

    if (ycsb_workload.rmw_percent()) {
      LOG_IF(FATAL, ermia::config::index_probe_only) << "Not supported";
      LOG_IF(FATAL, g_read_txn_type != ReadTransactionType::Sequential) << "RMW txn type must be sequential";
      w.push_back(workload_desc("RMW", double(ycsb_workload.rmw_percent()) / 100.0, TxnRMW));
    }

    if (ycsb_workload.scan_percent()) {
      LOG_IF(FATAL, g_read_txn_type != ReadTransactionType::Sequential) << "Scan txn type must be sequential";
      if (ermia::config::scan_with_it) {
        w.push_back(workload_desc("ScanWithIterator", double(ycsb_workload.scan_percent()) / 100.0, TxnScanWithIterator));
      } else {
        LOG_IF(FATAL, ermia::config::index_probe_only) << "Not supported";
        w.push_back(workload_desc("Scan", double(ycsb_workload.scan_percent()) / 100.0, TxnScan));
      }
    }

    return w;
  }

  static rc_t TxnRead(bench_worker *w) { return static_cast<ycsb_sequential_worker *>(w)->txn_read(); }
  static rc_t TxnReadAMACMultiGet(bench_worker *w) { return static_cast<ycsb_sequential_worker *>(w)->txn_read_amac_multiget(); }
  static rc_t TxnReadSimpleCoroMultiGet(bench_worker *w) { return static_cast<ycsb_sequential_worker *>(w)->txn_read_simple_coro_multiget(); }
  static rc_t TxnRMW(bench_worker *w) { return static_cast<ycsb_sequential_worker *>(w)->txn_rmw(); }
  static rc_t TxnScan(bench_worker *w) { return static_cast<ycsb_sequential_worker *>(w)->txn_scan(); }
  static rc_t TxnScanWithIterator(bench_worker *w) { return static_cast<ycsb_sequential_worker *>(w)->txn_scan_with_iterator(); }

  // Read transaction using traditional sequential execution
  rc_t txn_read() {
    ermia::transaction *txn = nullptr;
    if (ermia::config::index_probe_only) {
      // Reset the arena as txn will be nullptr and GenerateKey will get space from it
      arena->reset();
    } else {
      txn = db->NewTransaction(ermia::transaction::TXN_FLAG_READ_ONLY, *arena, txn_buf());
    }

    for (uint i = 0; i < g_reps_per_tx; ++i) {
      auto &k = GenerateKey(txn);
      ermia::varstr &v = str((ermia::config::index_probe_only) ? 0 : sizeof(ycsb_kv::value));
      // TODO(tzwang): add read/write_all_fields knobs
      rc_t rc = rc_t{RC_INVALID};
      table_index->GetRecord(txn, rc, k, v);  // Read

#if defined(SSI) || defined(SSN) || defined(MVOCC)
      TryCatch(rc);  // Might abort if we use SSI/SSN/MVOCC
#else
      // Under SI this must succeed
      ALWAYS_ASSERT(rc._val == RC_TRUE);
      ASSERT(ermia::config::index_probe_only || *(char*)v.data() == 'a');
#endif
      if (!ermia::config::index_probe_only) {
        memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)v.data(), sizeof(ycsb_kv::value));
      }
    }
    if (!ermia::config::index_probe_only) {
      TryCatch(db->Commit(txn));
    }
    return {RC_TRUE};
  }

  // Multi-get using AMAC
  rc_t txn_read_amac_multiget() {
    ermia::transaction *txn = nullptr;
    if (ermia::config::index_probe_only) {
      arena->reset();
    } else {
      values.clear();
      txn = db->NewTransaction(ermia::transaction::TXN_FLAG_READ_ONLY, *arena, txn_buf());
      for (uint i = 0; i < g_reps_per_tx; ++i) {
        values.push_back(&str(sizeof(ycsb_kv::value)));
      }
    }

    // Prepare states
    for (uint i = 0; i < g_reps_per_tx; ++i) {
      auto &k = GenerateKey(txn);
      if (as.size() < g_reps_per_tx)
        as.emplace_back(&k);
      else
        as[i].reset(&k);
    }

    table_index->amac_MultiGet(txn, as, values);

    if (!ermia::config::index_probe_only) {
      ermia::varstr &v = str(sizeof(ycsb_kv::value));
      for (uint i = 0; i < g_reps_per_tx; ++i) {
        ALWAYS_ASSERT(*(char*)values[i]->data() == 'a');
        memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)values[i]->data(), sizeof(ycsb_kv::value));
      }

      TryCatch(db->Commit(txn));
    }
    return {RC_TRUE};
  }

  // Multi-get using simple coroutine
  rc_t txn_read_simple_coro_multiget() {
    ermia::transaction *txn = nullptr;
    if (ermia::config::index_probe_only) {
      arena->reset();
    } else {
      values.clear();
      txn = db->NewTransaction(ermia::transaction::TXN_FLAG_READ_ONLY, *arena, txn_buf());
      for (uint i = 0; i < g_reps_per_tx; ++i) {
        values.push_back(&str(sizeof(ycsb_kv::value)));
      }
    }

    keys.clear();
    for (uint i = 0; i < g_reps_per_tx; ++i) {
      auto &k = GenerateKey(nullptr);
      keys.emplace_back(&k);
    }

    thread_local std::vector<std::experimental::coroutine_handle<>> handles(g_reps_per_tx);
    table_index->simple_coro_MultiGet(txn, keys, values, handles);

    if (!ermia::config::index_probe_only) {
      ermia::varstr &v = str(sizeof(ycsb_kv::value));
      for (uint i = 0; i< g_reps_per_tx; ++i) {
        ALWAYS_ASSERT(*(char*)values[i]->data() == 'a');
        memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)values[i]->data(), sizeof(ycsb_kv::value));
      }

      TryCatch(db->Commit(txn));
    }
    return {RC_TRUE};
  }

  // Read-modify-write transaction. Sequential execution only
  rc_t txn_rmw() {
    ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
    for (uint i = 0; i < g_reps_per_tx; ++i) {
      ermia::varstr &k = GenerateKey(txn);
      ermia::varstr &v = str(sizeof(ycsb_kv::value));
      // TODO(tzwang): add read/write_all_fields knobs
      rc_t rc = rc_t{RC_INVALID};
      table_index->GetRecord(txn, rc, k, v);  // Read

#if defined(SSI) || defined(SSN) || defined(MVOCC)
      TryCatch(rc);  // Might abort if we use SSI/SSN/MVOCC
#else
      // Under SI this must succeed
      LOG_IF(FATAL, rc._val != RC_TRUE);
      ALWAYS_ASSERT(rc._val == RC_TRUE);
      ASSERT(*(char*)v.data() == 'a');
#endif

      ASSERT(v.size() == sizeof(ycsb_kv::value));
      memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)v.data(), v.size());

      // Re-initialize the value structure to use my own allocated memory -
      // DoTupleRead will change v.p to the object's data area to avoid memory
      // copy (in the read op we just did).
      new (&v) ermia::varstr((char *)&v + sizeof(ermia::varstr), sizeof(ycsb_kv::value));
      new (v.data()) ycsb_kv::value("a");
      TryCatch(table_index->UpdateRecord(txn, k, v));  // Modify-write
    }

    for (uint i = 0; i < g_rmw_additional_reads; ++i) {
      ermia::varstr &k = GenerateKey(txn);
      ermia::varstr &v = str(sizeof(ycsb_kv::value));

      // TODO(tzwang): add read/write_all_fields knobs
      rc_t rc = rc_t{RC_INVALID};
      table_index->GetRecord(txn, rc, k, v);  // Read

#if defined(SSI) || defined(SSN) || defined(MVOCC)
      TryCatch(rc);  // Might abort if we use SSI/SSN/MVOCC
#else
      // Under SI this must succeed
      ALWAYS_ASSERT(rc._val == RC_TRUE);
      ASSERT(*(char*)v.data() == 'a');
#endif

      ASSERT(v.size() == sizeof(ycsb_kv::value));
      memcpy((char*)(&v) + sizeof(ermia::varstr), (char *)v.data(), v.size());
    }
    TryCatch(db->Commit(txn));
    return {RC_TRUE};
  }

  rc_t txn_scan() {
    ermia::transaction *txn = db->NewTransaction(ermia::transaction::TXN_FLAG_READ_ONLY, *arena, txn_buf());
    for (uint i = 0; i < g_reps_per_tx; ++i) {
      rc_t rc = rc_t{RC_INVALID};
      ScanRange range = GenerateScanRange(txn);
      ycsb_scan_callback callback;
      rc = table_index->Scan(txn, range.start_key, &range.end_key, callback);

      ALWAYS_ASSERT(callback.size() <= g_scan_max_length);
#if defined(SSI) || defined(SSN) || defined(MVOCC)
      TryCatch(rc);  // Might abort if we use SSI/SSN/MVOCC
#else
      ALWAYS_ASSERT(rc._val == RC_TRUE);
#endif
    }
    TryCatch(db->Commit(txn));
    return {RC_TRUE};
  }

  rc_t txn_scan_with_iterator() {
    ermia::transaction *txn = db->NewTransaction(ermia::transaction::TXN_FLAG_READ_ONLY, *arena, txn_buf());
    for (uint i = 0; i < g_reps_per_tx; ++i) {
      rc_t rc = rc_t{RC_INVALID};
      ScanRange range = GenerateScanRange(txn);
      ycsb_scan_callback callback;
      ermia::varstr valptr;
      ermia::dbtuple* tuple = nullptr;
      auto iter = ermia::ConcurrentMasstree::ScanIterator<
          /*IsRerverse=*/false>::factory(&table_index->GetMasstree(),
                                         txn->GetXIDContext(),
                                         range.start_key, &range.end_key);
      bool more = iter.init_or_next</*IsNext=*/false>();
      while (more) {
        if (!ermia::config::index_probe_only) {
          if (unlikely(ermia::config::is_backup_srv())) {
            tuple = ermia::oidmgr->BackupGetVersion(
                iter.tuple_array(), iter.pdest_array(), iter.value(),
                txn->GetXIDContext());
          } else {
            tuple = ermia::oidmgr->oid_get_version(
                iter.tuple_array(), iter.value(), txn->GetXIDContext());
          }
          if (tuple) {
            rc = txn->DoTupleRead(tuple, &valptr);
            if (rc._val == RC_TRUE) {
              callback.Invoke(iter.key().data(), iter.key().length(), valptr);
            }
          }
#if defined(SSI) || defined(SSN) || defined(MVOCC)
        TryCatch(rc);  // Might abort if we use SSI/SSN/MVOCC
#else
        ALWAYS_ASSERT(rc._val == RC_TRUE);
#endif
        }
        more = iter.init_or_next</*IsNext=*/true>();
      }
      ALWAYS_ASSERT(ermia::config::index_probe_only || callback.size() <= g_scan_max_length);
    }
    TryCatch(db->Commit(txn));
    return {RC_TRUE};
  }

 private:
  std::vector<ermia::ConcurrentMasstree::AMACState> as;
  std::vector<ermia::varstr *> keys;
  std::vector<ermia::varstr *> values;
};

void ycsb_do_test(ermia::Engine *db, int argc, char **argv) {
  ycsb_parse_options(argc, argv);
  ycsb_bench_runner<ycsb_sequential_worker> r(db);
  r.run();
}

#endif  // ADV_COROUTINE
