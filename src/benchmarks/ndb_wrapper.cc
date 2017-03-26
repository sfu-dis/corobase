#ifndef _NDB_WRAPPER_IMPL_H_
#define _NDB_WRAPPER_IMPL_H_

#include <stdint.h>
#include "ndb_wrapper.h"
#include "ordered_index.h"
#include "../dbcore/rcu.h"
#include "../dbcore/sm-index.h"
#include "../macros.h"
#include "../util.h"
#include "../txn.h"
#include "../tuple.h"

transaction *ndb_wrapper::new_txn(uint64_t txn_flags, str_arena &arena, transaction *buf) {
  new (buf) transaction(txn_flags, arena);
  return buf;
}

rc_t ndb_wrapper::commit_txn(transaction* t) {
  rc_t rc = t->commit();
  if (not rc_is_abort(rc))
    t->~transaction();
  return rc;
}

void ndb_wrapper::abort_txn(transaction* t) {
  t->abort_impl();
  t->~transaction();
}

#endif /* _NDB_WRAPPER_IMPL_H_ */
