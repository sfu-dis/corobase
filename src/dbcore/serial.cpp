#include "serial.h"
#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
namespace TXN {

/* Versions with more than 2.5 billion LSN delta from current are
   "old" and treated as read-mode. Readers do not apply SSN to these
   tuples, and writers (expected to be rare) must assume that the
   tuple has been read by a transaction that committed just before the
   writer. The upside is that readers pay vastly less than normal; the
   downside is this effectively means that any transaction that
   overwrites an old version cannot read under other committed
   overwrites: any meaningful sstamp would violate the exclusion
   window.
 */
#ifdef USE_PARALLEL_SSN
//int64_t OLD_VERSION_THRESHOLD = 0xa0000000ll;
//int64_t OLD_VERSION_THRESHOLD = 0x10000000ll;
//int64_t OLD_VERSION_THRESHOLD = 0xffffffffll;
int64_t OLD_VERSION_THRESHOLD = 0;
#endif

// for SSN if USE_PARALLEL_SSN, for SSI if USE_PARALLEL_SSI
uint64_t serial_abort_count = 0;
uint64_t __thread tls_serial_abort_count;

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
    summarize_serial_aborts();
}

void summarize_serial_aborts()
{
    __sync_fetch_and_add(&serial_abort_count, tls_serial_abort_count);
    if (not claimed_bitmap_entries) {
#ifdef USE_PARALLEL_SSN
        printf("--- SSN aborts: %lu\n", serial_abort_count);
#else
        printf("--- SSI aborts: %lu\n", serial_abort_count);
#endif
    }
}

bool
serial_register_reader_tx(dbtuple *t, XID xid)
{
    rl_bitmap_t old_bitmap = volatile_read(t->rl_bitmap);
    if (old_bitmap & tls_bitmap_entry)
        return false;
    
    int xid_pos = __builtin_ctz(tls_bitmap_entry);
    ASSERT(xid_pos >= 0 and xid_pos < readers_list::XIDS_PER_READER_KEY);
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

};  // end of namespace
#endif
