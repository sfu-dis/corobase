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

#include "../object.h"
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
    int scan(Str firstkey, bool matchfirst, F& scanner, XID xid, threadinfo& ti) const;
    template <typename F>
    int rscan(Str firstkey, bool matchfirst, F& scanner, XID xid, threadinfo& ti) const;

    template <typename F>
    inline int modify(Str key, F& f, threadinfo& ti);
    template <typename F>
    inline int modify_insert(Str key, F& f, threadinfo& ti);

    inline void print(FILE* f = 0, int indent = 0) const;

	typedef object_vector<value_type> tuple_vector_type; 
	typedef object_vector<node_type*> node_vector_type; 

	inline tuple_vector_type* get_tuple_vector()
	{
		INVARIANT(tuple_vector);
		return tuple_vector;
	}
	inline node_vector_type* get_node_vector()
	{
		INVARIANT(node_vector);
		return node_vector;
	}

	inline oid_type insert_tuple( value_type val )
	{
		INVARIANT( tuple_vector );
		return tuple_vector->insert( val );
	}

    // return the overwritten version (could be an in-flight version!)
	dbtuple *update_version(oid_type oid, object* new_desc, XID xid)
	{
		INVARIANT( tuple_vector );
		
#if CHECK_INVARIANTS
		int attempts = 0;
#endif
		fat_ptr new_ptr = fat_ptr::make( new_desc, INVALID_SIZE_CODE, fat_ptr::ASI_HOT_FLAG );
	start_over:
        fat_ptr head = tuple_vector->begin(oid);
		object* ptr = (object*)head.offset();
		xid_context *visitor = xid_get_context(xid);
		INVARIANT(visitor->owner == xid);
		dbtuple* version;
        bool overwrite = false;

		version = reinterpret_cast<dbtuple*>(ptr->payload());
		auto clsn = volatile_read(version->clsn);
		if( clsn.asi_type() == fat_ptr::ASI_XID )
		{
			/* Grab the context for this XID. If we're too slow,
			   the context might be recycled for a different XID,
			   perhaps even *while* we are reading the
			   context. Copy everything we care about and then
			   (last) check the context's XID for a mismatch that
			   would indicate an inconsistent read. If this
			   occurs, just start over---the version we cared
			   about is guaranteed to have a LSN now.
			 */
			//xid tracking
			auto holder_xid = XID::from_ptr(clsn);
			xid_context *holder= xid_get_context(holder_xid);
			INVARIANT(holder);
            auto end = volatile_read(holder->end);
            auto state = volatile_read(holder->state);
			auto owner = volatile_read(holder->owner);
			holder = NULL; // use cached values instead!

			// context still valid for this XID?
            if ( unlikely(owner != holder_xid) ) {
#if CHECK_INVARIANTS
				ASSERT(attempts < 2);
				attempts++;
#endif
				goto start_over;
			}
			switch (state)
			{
				// if committed and newer data, abort. if not, keep traversing
				case TXN_CMMTD:
					{
						if ( end > visitor->begin )		// to prevent version branch( or lost update)
							return false;
						else
							goto install;
					}

					// aborted data. ignore
				case TXN_ABRTD:
					goto install;

					// dirty data
				case TXN_EMBRYO:
				case TXN_ACTIVE:
					{
						// in-place update case ( multiple updates on the same record  by same transaction)
						if (holder_xid == xid ) {
                            overwrite = true;
							goto install;
                        }
						else
							return false;
					}

					// If this TX is committing, we shouldn't install new version!
				case TXN_COMMITTING:
					return false;
				default:
					ALWAYS_ASSERT( false );
			}
		}
		// check dirty writes 
		else 
		{
			// make sure this is valid committed data, or aborted data that is not reclaimed yet.
			// aborted, but not yet reclaimed.
			ASSERT(clsn.asi_type() == fat_ptr::ASI_LOG );
			if ( LSN::from_ptr(clsn) > visitor->begin )
				return false;
			else
				goto install;
		}

install:
		// install a new version
        if (tuple_vector->put(oid, head, new_ptr, overwrite))
			return version;
		return NULL;
	}

	// Sometimes, we don't care about version. We just need the first one!
	inline value_type fetch_latest_version( oid_type oid ) const
	{
		ALWAYS_ASSERT( tuple_vector );
		fat_ptr head = tuple_vector->begin(oid);
		if( head.offset() != 0 )
		{
			object* obj = (object*)head.offset();
			return reinterpret_cast<value_type>( obj->payload() );
		}
		else
			return NULL;
	}

    // return the sucessor of the version with rlsn (could be dirty)
    // used only for commit path, no xid checking etc.
    // for reads in commit path ONLY
    value_type fetch_overwriter(oid_type oid, LSN rlsn) const
	{
		INVARIANT(tuple_vector);
		ALWAYS_ASSERT( oid );
		object* cur_obj = NULL;
		for (fat_ptr ptr = tuple_vector->begin(oid); ptr.offset(); ptr = volatile_read(cur_obj->_next)) {
            cur_obj = (object*)ptr.offset();
            object *nxt_obj = (object *)cur_obj->_next.offset();
            if (not nxt_obj)
                return NULL;
            dbtuple *nxt_ver = reinterpret_cast<dbtuple *>(nxt_obj->payload());
            if (nxt_ver->clsn.asi_type() == fat_ptr::ASI_XID and LSN::from_ptr(nxt_ver->clsn) == rlsn)
                return (value_type)nxt_ver;
		}
		ALWAYS_ASSERT(0);
	}

    // return the (latest) committed version (at verify_lsn)
    value_type fetch_committed_version(oid_type oid, XID xid, fat_ptr verify_lsn) const
    {
        INVARIANT( tuple_vector );
        ASSERT(verify_lsn.asi_type() == fat_ptr::ASI_XID or verify_lsn.asi_type() == fat_ptr::ASI_LOG);
        ALWAYS_ASSERT( oid );
        xid_context *visitor= xid_get_context(xid);
        INVARIANT(visitor->owner == xid);
        object* cur_obj = NULL;
        for (fat_ptr ptr = tuple_vector->begin(oid); ptr.offset(); ptr = volatile_read(cur_obj->_next)) {
            cur_obj = (object*)ptr.offset();
            dbtuple* version = reinterpret_cast<dbtuple*>(cur_obj->payload());
            auto clsn = volatile_read(version->clsn);
            ASSERT(clsn.asi_type() == fat_ptr::ASI_XID or clsn.asi_type() == fat_ptr::ASI_LOG);
            if (clsn.asi_type() == fat_ptr::ASI_XID or LSN::from_ptr(clsn) > visitor->begin)
                continue;
            DIE_IF(clsn != verify_lsn, "fetch_latest_committed_version: lsn mismatch\n");
            return (value_type)version;
        }
        return 0;
    }

	value_type fetch_version( oid_type oid, XID xid ) const
	{
		INVARIANT( tuple_vector );
		ALWAYS_ASSERT( oid );
		xid_context *visitor= xid_get_context(xid);
		INVARIANT(visitor->owner == xid);

#if CHECK_INVARIANTS
		int attempts = 0;
#endif
		object* cur_obj = NULL;
	start_over:
		for( fat_ptr ptr = tuple_vector->begin(oid); ptr.offset(); ptr = volatile_read(cur_obj->_next) ) {
            cur_obj = (object*)ptr.offset();
			dbtuple* version = reinterpret_cast<dbtuple*>(cur_obj->payload());
			auto clsn = volatile_read(version->clsn);
            ASSERT(clsn.asi_type() == fat_ptr::ASI_XID or clsn.asi_type() == fat_ptr::ASI_LOG);
			// xid tracking & status check
			if( clsn.asi_type() == fat_ptr::ASI_XID )
			{
				/* Same as above: grab and verify XID context,
				   starting over if it has been recycled
				 */
				auto holder_xid = XID::from_ptr(clsn);
				xid_context *holder = xid_get_context(holder_xid);
				INVARIANT(holder);

				auto state = volatile_read(holder->state);
				auto end = volatile_read(holder->end);
				auto owner = volatile_read(holder->owner);
				holder = NULL; // use cached values instead!
				
				// context still valid for this XID?
				if( unlikely(owner != holder_xid) ) {
#if CHECK_INVARIANTS
					ASSERT(attempts < 2);
					attempts++;
#endif
					goto start_over;
				}

#if CHECK_INVARIANTS
                if (cur_obj->_next.offset())
                    ASSERT(((dbtuple*)(((object *)cur_obj->_next.offset())->payload()))->clsn.asi_type() == fat_ptr::ASI_LOG);
#endif

				// dirty data made by me is visible!
				if( owner == xid )
					goto out;

				// invalid data
				if( state != TXN_CMMTD)	   // only see committed data.
					continue;

				if( end > visitor->begin  			// committed(but invisible) data, 
						|| end == INVALID_LSN)		// aborted data
					continue;
			}
			else
			{
				if( LSN::from_ptr(clsn) > visitor->begin ) 	// invisible
					continue;
			}
        out:
            // try if we can trim
            ASSERT(version);
            dbtuple *ret_ver = version;
            // strat from the next's next, in case the head is in-flight
            // (could be remove if the tx aborted)
            cur_obj = (object *)cur_obj->_next.offset();
            LSN tlsn = volatile_read(RA::trim_lsn);
            while (cur_obj) {
                fat_ptr next_ptr = cur_obj->_next;
                object *next_obj = (object *)next_ptr.offset();
                if (not next_obj)
                    break;
                dbtuple *next_tuple = (dbtuple *)next_obj->payload();
                fat_ptr next_clsn = volatile_read(next_tuple->clsn);
                ASSERT(next_clsn.asi_type() == fat_ptr::ASI_LOG);
                if (LSN::from_ptr(next_clsn) < tlsn) {
                    ASSERT(next_tuple->readers.size() == 0);
                    if (__sync_bool_compare_and_swap(&cur_obj->_next._ptr, next_ptr._ptr, 0)) {
                        do {
                            next_ptr = next_obj->_next;
                            RA::deallocate(next_obj);
                            next_obj = (object *)next_ptr.offset();
                        } while (next_obj);
                    }
                    break;
                }
                cur_obj = (object *)cur_obj->_next;
            }
            return (value_type)ret_ver;
		}

		// No Visible records
		return 0;
	}

	inline node_type* fetch_node( oid_type oid ) const
	{
		ALWAYS_ASSERT( node_vector );
		// NOTE: oid 0 indicates absence of the node
		if( oid )
		{
			fat_ptr head = node_vector->begin(oid);
			if( head.offset() != 0 )
			{
				object* obj = (object*)head.offset();
				return (node_type*)(obj->payload());
			}
		}
		return NULL;
	}

	inline void unlink_tuple( oid_type oid, value_type item )
	{
		INVARIANT( tuple_vector );
		INVARIANT( oid );
		return tuple_vector->unlink( oid, item );
	}

  private:
	oid_type root_oid_;
	tuple_vector_type* tuple_vector; 
	node_vector_type* node_vector; 

    template <typename H, typename F>
    int scan(H helper, Str firstkey, bool matchfirst,
	     F& scanner, XID xid, threadinfo& ti) const;

    friend class unlocked_tcursor<P>;
    friend class tcursor<P>;
};

} // namespace Masstree
#endif
