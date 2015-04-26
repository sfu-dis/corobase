/* Masstree
 * Eddie Kohler, Yandong Mao, Robert Morris
 * Copyright (c) 2012-2013 President and Fellows of Harvard College
 * Copyright (c) 2012-2013 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, subject to the conditions
 * listed in the Masstree LICENSE file. These conditions include: you must
 * preserve this copyright notice, and you cannot mention the copyright
 * holders in advertising related to the Software without their permission.
 * The Software is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED. This
 * notice is a summary of the Masstree LICENSE file; the license in that file
 * is legally binding.
 */
#ifndef MASSTREE_HH
#define MASSTREE_HH
#include "compiler.hh"
#include "str.hh"
#include "ksearch.hh"

#include "../dbcore/sm-oid.h"
#include "../tuple.h"
#include "../dbcore/xid.h"
#include "../macros.h"
#include "../dbcore/sm-alloc.h"

namespace Masstree {
using lcdf::Str;
using lcdf::String;

template <typename T> class value_print;

template <int LW = 15, int IW = LW> struct nodeparams {
    static constexpr int leaf_width = LW;
    static constexpr int internode_width = IW;
    static constexpr bool concurrent = true;
    static constexpr bool prefetch = true;
    static constexpr int bound_method = bound_method_binary;
    static constexpr int debug_level = 0;
    static constexpr bool printable_keys = true;
    typedef uint64_t ikey_type;
};

template <int LW, int IW> constexpr int nodeparams<LW, IW>::leaf_width;
template <int LW, int IW> constexpr int nodeparams<LW, IW>::internode_width;
template <int LW, int IW> constexpr int nodeparams<LW, IW>::debug_level;

template <typename P> class node_base;
template <typename P> class leaf;
template <typename P> class internode;
template <typename P> class leafvalue;
template <typename P> class key;
template <typename P> class basic_table;
template <typename P> class unlocked_tcursor;
template <typename P> class tcursor;

template <typename P>
class basic_table {
  public:
    typedef P param_type;
    typedef node_base<P> node_type;
    typedef leaf<P> leaf_type;
    typedef typename P::value_type value_type;
    typedef typename P::threadinfo_type threadinfo;
    typedef unlocked_tcursor<P> unlocked_cursor_type;
    typedef tcursor<P> cursor_type;

    inline basic_table();

    void initialize(threadinfo& ti);
    void destroy(threadinfo& ti);

    inline node_type* root() const;
    inline node_type* fix_root();

    bool get(Str key, value_type& value, threadinfo& ti) const;

    template <typename F>
    int scan(Str firstkey, bool matchfirst, F& scanner, FID f, xid_context *xc, threadinfo& ti) const;
    template <typename F>
    int rscan(Str firstkey, bool matchfirst, F& scanner, FID f, xid_context *xc, threadinfo& ti) const;

    template <typename F>
    inline int modify(Str key, F& f, threadinfo& ti);
    template <typename F>
    inline int modify_insert(Str key, F& f, threadinfo& ti);

    inline void print(FILE* f = 0, int indent = 0) const;

    inline FID get_fid() { return fid_; }

#if 0
    // return the sucessor of the version with rlsn (could be dirty)
    // used only for commit path, no xid checking etc.
    // for reads in commit path ONLY
    dbtuple *fetch_overwriter(oid_type oid, LSN rlsn) const
	{
		INVARIANT(tuple_vector);
		ALWAYS_ASSERT( oid );
        object *prev_obj = (object *)tuple_vector->begin(oid).offset();
		object* cur_obj = NULL;
        fat_ptr ptr = ((object *)tuple_vector->begin(oid).offset())->_next;
        for (; ptr.offset(); ptr = volatile_read(cur_obj->_next)) {
            cur_obj = (object *)ptr.offset();
            dbtuple *tuple = (dbtuple *)cur_obj->payload();
            // Note here we might see data that are still in post-commit
            // (ie., tuple clsn will be an XID), b/c we allow updating
            // pre-committed data in update_version. So this also means
            // we might return a tuple with clsn as an XID to the caller.
            if (volatile_read(tuple->clsn).asi_type() == fat_ptr::ASI_XID) {
                // so now there could be at most one dirty version (head)
                // followed by multiple **precommitted** versions, before
                // the committed version read by the tx that's invoking
                // this function. so this one must not be the rlsn that
                // I'm trying to find.
                prev_obj = cur_obj;
                continue;
            }

            // now clsn must be ASI_LOG
            LSN tuple_clsn = LSN::from_ptr(volatile_read(tuple->clsn));

            // no overwriter (we started from  the 2nd version in the chain)
            if (tuple_clsn < rlsn)
                break;
            if (tuple_clsn == rlsn)
                return (dbtuple*) prev_obj->payload();
            prev_obj = cur_obj;
        }
        return 0;
	}
#endif
#if 0
    // return the (latest) committed version (at verify_lsn)
    dbtuple *fetch_committed_version_at(oid_type oid, XID xid, LSN at_clsn) const
    {
        INVARIANT( tuple_vector );
        ALWAYS_ASSERT( oid );
        object* cur_obj = NULL;
        for (fat_ptr ptr = tuple_vector->begin(oid); ptr.offset(); ptr = volatile_read(cur_obj->_next)) {
            cur_obj = (object*)ptr.offset();
            dbtuple* version = reinterpret_cast<dbtuple*>(cur_obj->payload());
            auto clsn = volatile_read(version->clsn);
            ASSERT(clsn.asi_type() == fat_ptr::ASI_XID or clsn.asi_type() == fat_ptr::ASI_LOG);
            if (clsn.asi_type() == fat_ptr::ASI_XID or LSN::from_ptr(clsn) > at_clsn)
                continue;
            if (LSN::from_ptr(clsn) < at_clsn)
                break;
            ASSERT(LSN::from_ptr(clsn) == at_clsn);
            return version;
        }
        return 0;
    }
#endif

    inline node_type* fetch_node(OID oid) const
	{
        ALWAYS_ASSERT(fid_);
		// NOTE: oid 0 indicates absence of the node
		if( oid )
		{
            fat_ptr head = oidmgr->oid_get(fid_, oid);
			if( head.offset() != 0 )
			{
				object* obj = (object*)head.offset();
				return (node_type*)(obj->payload());
			}
		}
		return NULL;
	}

  private:
    OID root_oid_;
    // FID of this tree, **not** of the table it indexes
    // (note that the tree also uses indirection array)
    // The table's FID is stored in base_txn_btree for that
    // table. So don't break the separation of base_txn_btree
    // and basic_table.
    //
    // tzwang: actually, we might want to use the same FID
    // for the table and its index(es), so that after checkpointed
    // this FID's OID array, we have both the index and version
    // chain => simpler recovery as we don't need to rebuild the
    // index(es) again.
    FID fid_;

    template <typename H, typename F>
    int scan(H helper, Str firstkey, bool matchfirst,
         F& scanner, FID f, xid_context *xc, threadinfo& ti) const;

    friend class unlocked_tcursor<P>;
    friend class tcursor<P>;
};

} // namespace Masstree
#endif
