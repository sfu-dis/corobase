#pragma once
#include <map>
#include "dbcore/sm-common.h"

namespace ermia {

// Base class for user-facing index implementations
class OrderedIndex {
  friend class transaction;

protected:
  TableDescriptor *table_descriptor;
  bool is_primary;
  FID self_fid;

public:
  OrderedIndex(std::string table_name, bool is_primary);
  virtual ~OrderedIndex() {}
  inline TableDescriptor *GetTableDescriptor() { return table_descriptor; }
  inline bool IsPrimary() { return is_primary; }
  inline FID GetIndexFid() { return self_fid; }
  virtual void *GetTable() = 0;

  class ScanCallback {
  public:
    virtual ~ScanCallback() {}
    virtual bool Invoke(const char *keyp, size_t keylen,
                        const varstr &value) = 0;
  };

  class DiaScanCallback {
  public:
    virtual ~DiaScanCallback() {}
    virtual bool Invoke(const char *keyp, size_t keylen, OID oid) = 0;
    virtual bool Receive(transaction *t, TableDescriptor *td) = 0;
  };

  // Get a record with a key of length keylen. The underlying DB does not manage
  // the memory associated with key. [rc] stores TRUE if found, FALSE otherwise.
  virtual PROMISE(void) GetRecord(transaction *t, rc_t &rc, const varstr &key, varstr &value,
                                  OID *out_oid = nullptr) = 0;

  // Return the OID that corresponds the given key
  virtual PROMISE(void) GetOID(const varstr &key, rc_t &rc, TXN::xid_context *xc, OID &out_oid,
                               ConcurrentMasstree::versioned_node_t *out_sinfo = nullptr) = 0;

#if defined(NOWAIT) || defined(WAITDIE)
  virtual void GetRecordForUpdate(transaction *t, rc_t &rc, const varstr &key, varstr &value,
                   OID *out_oid = nullptr) = 0;
#endif

  // Update a database record with a key of length keylen, with mapping of length
  // valuelen.  The underlying DB does not manage the memory pointed to by key or
  // value (a copy is made).
  //
  // If the does not already exist and config::upsert is set to true, insert.
  virtual PROMISE(rc_t) UpdateRecord(transaction *t, const varstr &key, varstr &value) = 0;

  // Insert a record with a key of length keylen.
  virtual PROMISE(rc_t) InsertRecord(transaction *t, const varstr &key, varstr &value,
                                     OID *out_oid = nullptr) = 0;

  // Map a key to an existing OID. Could be used for primary or secondary index.
  virtual PROMISE(bool) InsertOID(transaction *t, const varstr &key, OID oid) = 0;

  // Search [start_key, *end_key) if end_key is not null, otherwise
  // search [start_key, +infty)
  virtual PROMISE(rc_t) Scan(transaction *t, const varstr &start_key,
                             const varstr *end_key, ScanCallback &callback,
                             str_arena *arena) = 0;
  // Search (*end_key, start_key] if end_key is not null, otherwise
  // search (-infty, start_key] (starting at start_key and traversing
  // backwards)
  virtual PROMISE(rc_t) ReverseScan(transaction *t, const varstr &start_key,
                                    const varstr *end_key, ScanCallback &callback,
                                    str_arena *arena) = 0;

  // Default implementation calls put() with NULL (zero-length) value
  virtual PROMISE(rc_t) RemoveRecord(transaction *t, const varstr &key) = 0;

  virtual size_t Size() = 0;
  virtual std::map<std::string, uint64_t> Clear() = 0;
  virtual void SetArrays(bool) = 0;

  /**
   * Insert key-oid pair to the underlying actual index structure.
   *
   * Returns false if the record already exists or there is potential phantom.
   */
  virtual PROMISE(bool) InsertIfAbsent(transaction *t, const varstr &key, OID oid) = 0;

  virtual PROMISE(void) ScanOID(transaction *t, const varstr &start_key,
                                const varstr *end_key, rc_t &rc, OID *dia_callback) = 0;
  virtual PROMISE(void) ReverseScanOID(transaction *t, const varstr &start_key,
                                       const varstr *end_key, rc_t &rc,
                                       OID *dia_callback) = 0;
};

}  // namespace ermia
