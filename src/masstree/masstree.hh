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

#ifdef HACK_SILO
#include "../object.h"
#include "../tuple.h"
#include "../rcu/xid.h"
#endif

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
    int scan(Str firstkey, bool matchfirst, F& scanner, threadinfo& ti) const;
    template <typename F>
    int rscan(Str firstkey, bool matchfirst, F& scanner, threadinfo& ti) const;

    template <typename F>
    inline int modify(Str key, F& f, threadinfo& ti);
    template <typename F>
    inline int modify_insert(Str key, F& f, threadinfo& ti);

    inline void print(FILE* f = 0, int indent = 0) const;

#ifdef HACK_SILO
	typedef object_vector<value_type> tuple_vector_type; 
	typedef object_vector<node_type*> node_vector_type; 
	typedef object<value_type> object_type;

	inline oid_type insert_tuple( value_type val )
	{
		assert( tuple_vector );
		return tuple_vector->insert( val );
	}

	inline void update_tuple( oid_type oid, value_type val )
	{
		assert( tuple_vector );
		tuple_vector->put( oid, val );
	}

	std::pair<bool, value_type> update_version( oid_type oid, value_type val, XID xid)
	{
		object_type* ptr = tuple_vector->begin(oid);
		xid_context *visitor = xid_get_context(xid);
		dbtuple* version = reinterpret_cast<dbtuple*>(ptr);

		// check dirty writes 
		if( version->is_xid )
		{
			//xid tracking
			xid_context *holder= xid_get_context(version->v_.xid);

			// visibility test
			if( holder->end == INVALID_LSN 					// invalid entry XXX. we don't need to check state field, right?
					|| holder->end > visitor->begin )		// to prevent version branch( or lost update)
				return std::make_pair(false, reinterpret_cast<value_type>(NULL) );

			// in-place update case ( multiple updates on the same record )
			if( holder->owner == visitor->owner )
			{
				ptr->_data = val;
				return std::make_pair( true, reinterpret_cast<value_type>(version) );
			}
		}
		else 
		{
			// Okay. There's no dirty data in this chain.
			if( version->v_.clsn > visitor->begin )
				return std::make_pair( false, reinterpret_cast<value_type>(NULL) );
		}

		// install a new version
		if(!tuple_vector->put( oid, val ))
			return std::make_pair(false, reinterpret_cast<value_type>(NULL));
		return std::make_pair(true, reinterpret_cast<value_type>(NULL));
	}

	inline value_type fetch_tuple( oid_type oid ) const
	{
		assert( tuple_vector );
		return tuple_vector->get( oid );
	}
	value_type fetch_version( oid_type oid, XID xid ) const
	{
		xid_context *visitor= xid_get_context(xid);

		// TODO. iterate whole elements in a chain and pick up the LATEST one ( having the largest end field )
		for( object_type* ptr = tuple_vector->begin(oid); ptr; ptr = ptr->next() ) {
			dbtuple* version = reinterpret_cast<dbtuple*>(ptr->_data);

			// xid tracking & status check
			if( version->is_xid )
			{
				xid_context *holder = xid_get_context(version->v_.xid);

				// invalid data
				if( holder->state != TXN_CMMTD)       // only see committed data.
					continue;

				if( holder->end > visitor->begin  			// committed(but invisible) data, 
						|| holder->end == INVALID_LSN)		// aborted data
					continue;
				return version;
			}
			else
			{
				if( version->v_.clsn > visitor->begin ) 	// invisible
					continue;
				return version;
			}
		}

		// No Visible records
		return 0;
	}

	inline oid_type insert_node( node_type* node )
	{
		assert( node_vector );
		return node_vector->insert( node );
	}

	inline bool update_node( oid_type oid, node_type* node )
	{
		assert( node_vector );
		return node_vector->put( oid, node );
	}
	inline node_type* fetch_node( oid_type oid ) const
	{
		assert( node_vector );
		if( oid )
			return node_vector->get( oid );
		else
			return NULL;
	}
#endif

  private:
#ifdef HACK_SILO
	oid_type root_oid_;
	tuple_vector_type* tuple_vector; 
	node_vector_type* node_vector; 
#else
    node_type* root_;
#endif

    template <typename H, typename F>
    int scan(H helper, Str firstkey, bool matchfirst,
	     F& scanner, threadinfo& ti) const;

    friend class unlocked_tcursor<P>;
    friend class tcursor<P>;
};

} // namespace Masstree
#endif
