#ifndef _NDB_TXN_PROTO2_IMPL_H_
#define _NDB_TXN_PROTO2_IMPL_H_

#include <iostream>
#include <atomic>
#include <vector>
#include <set>

#include "txn.h"
#include "txn_impl.h"
#include "txn_btree.h"
#include "macros.h"
#include "spinbarrier.h"
#include "record/serializer.h"

// forward decl
template <typename Traits> class transaction_proto2;

// protocol 2 - no global consistent TIDs
template <typename Traits>
class transaction_proto2 : public transaction<transaction_proto2, Traits> {

  friend class transaction<transaction_proto2, Traits>;
  typedef transaction<transaction_proto2, Traits> super_type;

public:

  typedef Traits traits_type;
  typedef transaction_base::string_type string_type;
#ifdef PHANTOM_PROT_NODE_SET
  typedef typename super_type::absent_set_map absent_set_map;
#endif
  typedef typename super_type::write_set_map write_set_map;

  transaction_proto2(uint64_t flags,
                     typename Traits::StringAllocator &sa)
    : transaction<transaction_proto2, Traits>(flags, sa)
  {
  }

  ~transaction_proto2() {}
};

#endif /* _NDB_TXN_PROTO2_IMPL_H_ */
