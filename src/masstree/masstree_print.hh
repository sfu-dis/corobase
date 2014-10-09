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
#ifndef MASSTREE_PRINT_HH
#define MASSTREE_PRINT_HH
#include "masstree_struct.hh"
#include <stdio.h>

#ifdef HACK_SILO
#include "../object.h"
#endif

namespace Masstree {

template <typename T>
class value_print {
  public:
    static void print(T value, FILE* f, const char* prefix,
                      int indent, Str key, kvtimestamp_t initial_timestamp,
                      char* suffix) {
        value->print(f, prefix, indent, key, initial_timestamp, suffix);
    }
};

template <>
class value_print<unsigned char*> {
  public:
    static void print(unsigned char* value, FILE* f, const char* prefix,
                      int indent, Str key, kvtimestamp_t,
                      char* suffix) {
	fprintf(f, "%s%*s%.*s = %p%s\n",
                prefix, indent, "", key.len, key.s, value, suffix);
    }
};

template <typename P>
void node_base<P>::print(FILE *f, const char *prefix, int indent, int kdepth)
{
    if (this->isleaf())
	((leaf<P> *) this)->print(f, prefix, indent, kdepth);
    else
	((internode<P> *) this)->print(f, prefix, indent, kdepth);
}

template <typename P>
void leaf<P>::print(FILE *f, const char *prefix, int indent, int kdepth)
{
    f = f ? f : stderr;
    prefix = prefix ? prefix : "";
    typename node_base<P>::nodeversion_type v;
    permuter_type perm;
    do {
	v = *this;
	fence();
	perm = permutation_;
    } while (this->has_changed(v));

    char keybuf[MASSTREE_MAXKEYLEN];
    fprintf(f, "%s%*sleaf %p: %d %s, version %x, permutation %s, ",
	    prefix, indent, "", this,
            perm.size(), perm.size() == 1 ? "key" : "keys",
            v.version_value(), perm.unparse().c_str());
    if (nremoved_)
	fprintf(f, "removed %d, ", nremoved_);
#ifdef HACK_SILO
    fprintf(f, "parent %llu, prev %llu, next %llu ", parent_oid_, prev_oid_, next_oid_);
#else
    fprintf(f, "parent %p, prev %p, next %p ", parent_, prev_, next_.ptr);
#endif
    if (ksuf_ && extrasize64_ < -1)
	fprintf(f, "[ksuf i%dx%d] ", -extrasize64_ - 1, ksuf_->allocated_size() / 64);
    else if (ksuf_)
	fprintf(f, "[ksuf x%d] ", ksuf_->allocated_size() / 64);
    else if (extrasize64_)
	fprintf(f, "[ksuf i%d] ", extrasize64_);
    if (P::debug_level > 0) {
	kvtimestamp_t cts = timestamp_sub(created_at_[0], initial_timestamp);
	fprintf(f, "@" PRIKVTSPARTS, KVTS_HIGHPART(cts), KVTS_LOWPART(cts));
    }
    fputc('\n', f);

#ifdef HACK_SILO
    if (v.deleted() || (perm[0] != 0 && prev_oid_))
#else
    if (v.deleted() || (perm[0] != 0 && prev_))
#endif
	fprintf(f, "%s%*s%s = [] #0\n", prefix, indent + 2, "", key_type(ikey_bound()).unparse().c_str());

    char xbuf[15];
    for (int idx = 0; idx < perm.size(); ++idx) {
	int p = perm[idx], l;
        if (P::printable_keys)
            l = this->get_key(p).unparse_printable(keybuf, sizeof(keybuf));
        else
            l = this->get_key(p).unparse(keybuf, sizeof(keybuf));
	sprintf(xbuf, " #%x/%d", p, keylenx_[p]);
	leafvalue_type lv = lv_[p];
	if (this->has_changed(v)) {
	    fprintf(f, "%s%*s[NODE CHANGED]\n", prefix, indent + 2, "");
	    break;
	} else if (!lv)
	    fprintf(f, "%s%*s%.*s = []%s\n", prefix, indent + 2, "", l, keybuf, xbuf);
	else if (value_is_layer(p)) {
	    fprintf(f, "%s%*s%.*s = SUBTREE%s\n", prefix, indent + 2, "", l, keybuf, xbuf);
#ifdef HACK_SILO
	    node_base<P> *n = this->fetch_node(lv.layer())->unsplit_ancestor();
#else
	    node_base<P> *n = lv.layer()->unsplit_ancestor();
#endif
	    n->print(f, prefix, indent + 4, kdepth + key_type::ikey_size);
	} else {
	    typename P::value_type tvx = lv.value();
            P::value_print_type::print(tvx, f, prefix, indent + 2, Str(keybuf, l), initial_timestamp, xbuf);
	}
    }

    if (v.deleted())
	fprintf(f, "%s%*s[DELETED]\n", prefix, indent + 2, "");
}

template <typename P>
void internode<P>::print(FILE *f, const char *prefix, int indent, int kdepth)
{
    f = f ? f : stderr;
    prefix = prefix ? prefix : "";
    internode<P> copy(*this);
    for (int i = 0; i < 100 && (copy.has_changed(*this) || this->inserting() || this->splitting()); ++i)
	memcpy(&copy, this, sizeof(copy));

    char keybuf[MASSTREE_MAXKEYLEN];
#ifdef HACK_SILO
    fprintf(f, "%s%*sinternode %p%s: %d keys, version %x, parent %llu",
	    prefix, indent, "", this, this->deleted() ? " [DELETED]" : "",
	    copy.size(), copy.version_value(), copy.parent_oid_);
#else
    fprintf(f, "%s%*sinternode %p%s: %d keys, version %x, parent %p",
	    prefix, indent, "", this, this->deleted() ? " [DELETED]" : "",
	    copy.size(), copy.version_value(), copy.parent_);
#endif
    if (P::debug_level > 0) {
	kvtimestamp_t cts = timestamp_sub(created_at_[0], initial_timestamp);
	fprintf(f, " @" PRIKVTSPARTS, KVTS_HIGHPART(cts), KVTS_LOWPART(cts));
    }
    fputc('\n', f);
    for (int p = 0; p < copy.size(); ++p) {
#ifdef HACK_SILO
	if (copy.child_oid_[p])
	    copy.fetch_node(copy.child_oid_[p])->print(f, prefix, indent + 4, kdepth);
#else
	if (copy.child_[p])
	    copy.child_[p]->print(f, prefix, indent + 4, kdepth);
#endif
	else
	    fprintf(f, "%s%*s[]\n", prefix, indent + 4, "");
        int l;
        if (P::printable_keys)
            l = copy.get_key(p).unparse_printable(keybuf, sizeof(keybuf));
        else
            l = copy.get_key(p).unparse(keybuf, sizeof(keybuf));
	fprintf(f, "%s%*s%.*s\n", prefix, indent + 2, "", l, keybuf);
    }
#ifdef HACK_SILO
    if (copy.child_oid_[copy.size()])
	copy.fetch_node(copy.child_oid_[copy.size()])->print(f, prefix, indent + 4, kdepth);
#else
    if (copy.child_[copy.size()])
	copy.child_[copy.size()]->print(f, prefix, indent + 4, kdepth);
#endif
    else
	fprintf(f, "%s%*s[]\n", prefix, indent + 4, "");
}

template <typename P>
void basic_table<P>::print(FILE *f, int indent) const {
#ifndef HACK_SILO
    root_->print(f ? f : stdout, "", indent, 0);
#endif
}

} // namespace Masstree
#endif
