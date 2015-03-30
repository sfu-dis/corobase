#include "serial.h"
#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
namespace TXN {

/* The read optimization for SSN (prob. SSI too)

   Versions with some LSN delta from current are "old" and treated as read-mode
   Readers do not apply SSN to these tuples, and writers (expected to be rare)
   must assume that the tuple has been read by a transaction that committed just
   before the writer. The upside is that readers pay vastly less than normal;
   the downside is this effectively means that any transaction that overwrites
   an old version cannot read under other committed overwrites: any meaningful
   sstamp would violate the exclusion window.

   ^^^^ Note: the above makes sure any transaction that clobbers an old version
   and read under **committed** overwrites will be doom (because a transaction
   can see a meaningful sstamp in the version only after the overwrite is
   committed, ie, after post-commit). However, such a transaction might also
   read under **uncommitted** overwriters. If such an overwriter committed
   **before** the reader, then the schedule might not be serializable.

   The center of this problem is, under the read optimization, a reader will
   not be aware any write that has happened, is happening, or will happen after
   the read. A writer might might come and go and do not leave any clue for the
   reader to tell if somebody clobbered the value it read before.

   Example: T1 reads an old version v1; then T2 comes to clobber v1 by
   installing a new version v2. When T1 was reading the version, it saw an
   empty sstamp -- indeed nobody clobbered this version; but later T2 came and
   clobbered it. There's no way for T1 to know this, not even after T1 entered
   precommit: recall that T1 will not keep an entry for v1 in its read set!

   Since it's not easy for readers to tell if their values are clobbered without
   tracking reads, we guarantee them to succeed. So if a transaction reads an
   old version, it is guaranteed to commit, unless it hits some other conflicts
   (e.g., w-w). To make this work, in each version we maintain a "bstamp", which
   records the maximum begin timestamp of all readers that have read this
   version. We only set bstamp for old versions. Readers update bstamp when they
   see 1) v.bstamp < reader.begin_stamp and 2) the version is an old one. The
   reader also needs to register itself in the version's readers bitmap, and does
   **not** deregister itself from the bitmap after commit (there's no way to do
   so for old versions, as they're not even in the read set). At precommit,
   writers will check the versions they clobbered. For each clobbered old
   version, the writer follows the bitmap in the version to find out the xid
   context that corresponds to the reader in the centralized redaers list--this
   is subtle - the real reader that set the bit might have already long gone,
   committed, aborted; the context stored there now might already belong to
   some other transaction. So the right condition under which the writer must
   abort to backoff for the reader is: 1) the xid context is still valid;
   2) the xid context's owner is serialized **after** the writer; 3) the xid
   context's begin stamp is <= the old version's bstamp. Among these three
   conditions, 2) indicates a possible r-w conflict, and 3) indicates that the
   tx represented by xid context **might** have read the old version in
   question. False positives will come from 2+3: the "reader" we found there
   might be totally a different guy and not related to us - it's just unlucky
   to have used this xid context that was used by some evil reader before.
   Hopefully 2) will be able to eliminate some false positives.
 */
int64_t OLD_VERSION_THRESHOLD = 0xa0000000ll;
//int64_t OLD_VERSION_THRESHOLD = 0x10000000ll;
//int64_t OLD_VERSION_THRESHOLD = 0x50000000ll; // good for TPCC++ at 12 threads at crossfire
//int64_t OLD_VERSION_THRESHOLD = 0x1000000ll;
//int64_t OLD_VERSION_THRESHOLD = 0x100ll;
//int64_t OLD_VERSION_THRESHOLD = INT64_MAX;
//int64_t OLD_VERSION_THRESHOLD = 0;

readers_list rlist;

typedef dbtuple::rl_bitmap_t rl_bitmap_t;
static __thread rl_bitmap_t tls_bitmap_entry = 0;
static rl_bitmap_t claimed_bitmap_entries = 0;

/* Return a bitmap with 1's representing active readers.
 */
readers_list::bitmap_t serial_get_tuple_readers(dbtuple *tup, bool exclude_self)
{
    if (exclude_self)
        return volatile_read(tup->rl_bitmap) & ~tls_bitmap_entry;
    return volatile_read(tup->rl_bitmap);
}

void assign_reader_bitmap_entry() {
    if (tls_bitmap_entry)
        return;

    rl_bitmap_t old_bitmap = volatile_read(claimed_bitmap_entries);
 retry:
    rl_bitmap_t new_bitmap = old_bitmap | (old_bitmap+1);
    rl_bitmap_t cur_bitmap = __sync_val_compare_and_swap(&claimed_bitmap_entries, old_bitmap, new_bitmap);
    if (old_bitmap != cur_bitmap) {
        old_bitmap = cur_bitmap;
        goto retry;
    }

    tls_bitmap_entry = new_bitmap ^ old_bitmap;
    rl_bitmap_t forbidden_bits = -(rl_bitmap_t(1) << readers_list::XIDS_PER_READER_KEY);
    ALWAYS_ASSERT(not (tls_bitmap_entry & forbidden_bits));
}

void deassign_reader_bitmap_entry() {
    ALWAYS_ASSERT(tls_bitmap_entry);
    ALWAYS_ASSERT(claimed_bitmap_entries & tls_bitmap_entry);
    __sync_fetch_and_xor(&claimed_bitmap_entries, tls_bitmap_entry);
    tls_bitmap_entry = 0;
}


bool
serial_register_reader_tx(dbtuple *t, XID xid)
{
    rl_bitmap_t old_bitmap = volatile_read(t->rl_bitmap);
    if (old_bitmap & tls_bitmap_entry)
        return false;
#if CHECK_INVARIANTS
    int xid_pos = __builtin_ctz(tls_bitmap_entry);
    ASSERT(xid_pos >= 0 and xid_pos < readers_list::XIDS_PER_READER_KEY);
#endif
    __sync_fetch_and_or(&t->rl_bitmap, tls_bitmap_entry);
    ASSERT(t->rl_bitmap & tls_bitmap_entry);
    return true;
}

void
serial_deregister_reader_tx(dbtuple *t)
{
    ASSERT(tls_bitmap_entry);
    __sync_fetch_and_xor(&t->rl_bitmap, tls_bitmap_entry);
    ASSERT(not (t->rl_bitmap & tls_bitmap_entry));
}

// register tx in the global rlist (called at tx start)
void
serial_register_tx(XID xid)
{
    ASSERT(not rlist.xids[__builtin_ctz(tls_bitmap_entry)]._val);
    volatile_write(rlist.xids[__builtin_ctz(tls_bitmap_entry)]._val, xid._val);
}

// deregister tx in the global rlist (called at tx end)
void
serial_deregister_tx(XID xid)
{
    volatile_write(rlist.xids[__builtin_ctz(tls_bitmap_entry)]._val, 0);
    ASSERT(not rlist.xids[__builtin_ctz(tls_bitmap_entry)]._val);
}

#ifdef USE_PARALLEL_SSN
rc_t
ssn_read(xid_context *xc, dbtuple *tuple)
{
    auto tuple_sstamp = volatile_read(tuple->sstamp);
    if (not tuple->is_old(xc)) {
        auto v_clsn = tuple->clsn.offset();
        // \eta - largest predecessor. So if I read this tuple, I should commit
        // after the tuple's creator (trivial, as this is committed version, so
        // this tuple's clsn can only be a predecessor of me): so just update
        // my \eta if needed.
        if (xc->pstamp < v_clsn)
            xc->pstamp = v_clsn;

        // Now if this tuple was overwritten by somebody, this means if I read
        // it, that overwriter will have anti-dependency on me (I must be
        // serialized before the overwriter), and it already committed (as a
        // successor of mine), so I need to update my \pi for the SSN check.
        // This is the easier case of anti-dependency (the other case is T1
        // already read a (then latest) version, then T2 comes to overwrite it).
        if (tuple_sstamp == NULL_PTR) {   // no overwrite so far
            serial_register_reader_tx(tuple, xc->owner);
            xc->read_set.emplace_back(tuple);
        }
        else if (tuple_sstamp.asi_type() == fat_ptr::ASI_LOG) {
            if (xc->sstamp > tuple_sstamp.offset())
                xc->sstamp = tuple_sstamp.offset(); // \pi
        }

#ifdef DO_EARLY_SSN_CHECKS
        if (not ssn_check_exclusion(xc))
            return {RC_ABORT_SERIAL};
#endif
    }
    else {
        if (tuple_sstamp != NULL_PTR and tuple_sstamp.asi_type() == fat_ptr::ASI_LOG) {
            if (xc->sstamp > tuple_sstamp.offset())
                xc->sstamp = tuple_sstamp.offset(); // \pi
        }

        uint64_t bs = 0;
        do {
            bs = volatile_read(tuple->bstamp);
        }
        while (tuple->bstamp < xc->begin.offset() and
               not __sync_bool_compare_and_swap(&tuple->bstamp, bs, xc->begin.offset()));
        serial_register_reader_tx(tuple, xc->owner);
    }
    return {RC_TRUE};
}
#endif  // SSN

#ifdef USE_PARALLEL_SSI
rc_t
ssi_read(xid_context *xc, dbtuple *tuple)
{
    // Consider the dangerous structure that could lead to non-serializable
    // execution: T1 r:w T2 r:w T3 where T3 committed first. Read() needs
    // to check if I'm the T1 and do bookeeping if I'm the T2 (pivot).
    // See tuple.h for explanation on what s2 means.
    if (volatile_read(tuple->s2)) {
        ASSERT(tuple->sstamp.asi_type() == fat_ptr::ASI_LOG);    // sstamp will be valid too if s2 is valid
        return rc_t{RC_ABORT_SERIAL};
    }

    fat_ptr tuple_s1 = volatile_read(tuple->sstamp);
    // see if there was a guy with cstamp=tuple_s1 who overwrote this version
    if (tuple_s1.asi_type() == fat_ptr::ASI_LOG) { // I'm T2
        // remember the smallest sstamp so that I can re-check at precommit
        if (xc->ct3 > tuple_s1.offset())
            xc->ct3 = tuple_s1.offset();
    }

    if (not tuple->is_old(xc))
        read_set.emplace_back(tuple);
    else {
        uint64_t bs = 0;
        do {
            bs = volatile_read(tuple->bstamp);
        } while (tuple->bstamp < xc->begin.offset() and
                 not __sync_bool_compare_and_swap(&tuple->bstamp, bs, xc->begin.offset()));
    }

    // survived, register as a reader
    serial_register_reader_tx(tuple, xc->owner);
    return {RC_TRUE};
}
#endif  // SSI

};  // end of namespace
#endif
