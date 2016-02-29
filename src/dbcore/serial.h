#include "../macros.h"
#pragma once
#include <unordered_map>
#include "sm-rc.h"
#include "xid.h"

#if defined(USE_PARALLEL_SSN) || defined(USE_PARALLEL_SSI)
namespace TXN {
void assign_reader_bitmap_entry();
void deassign_reader_bitmap_entry();    

#ifdef USE_PARALLEL_SSN
// returns true if serializable, false means exclusion window violation
inline bool ssn_check_exclusion(xid_context *xc) {
    auto ss = xc->sstamp.load(std::memory_order_acquire) & (~xid_context::sstamp_final_mark);
#if CHECK_INVARIANTS
    if (xc->pstamp >= ss) printf("ssn exclusion failure\n");
#endif
    // if predecessor >= sucessor, then predecessor might depend on sucessor => cycle
    // note xc->sstamp is initialized to ~0, xc->pstamp's init value is 0,
    // so don't return xc->pstamp < xc->sstamp...
    if (xc->pstamp >= ss) {
        return false;
    }
    return true;
}
#endif

struct readers_list {
    /*
     * A bitmap to account for readers in a tuple. One bit per reader thread.
     * Having four uint64_t enables us to support 256 concurrent readers at most.
     * 256 / 8 = 64 bytes = one cache line.
     */
    struct bitmap_t {
        static const uint32_t CAPACITY = 256;   // must be a multiple of 64
        static const uint32_t ARRAY_SIZE = CAPACITY / 64;
        uint64_t array[ARRAY_SIZE];

        bitmap_t() {
            memset(array, '\0', sizeof(uint64_t) * ARRAY_SIZE);
        }

        bool is_empty(bool exclude_self);
    };

    struct tls_bitmap_info {
        uint64_t entry;  // the entry with my bit set
        uint32_t index;  // which uint64_t in bitmap_t.array
        inline uint32_t xid_index() {
            ASSERT(entry);
            return index * 64 + __builtin_ctzll(entry);
        }
    };

    bitmap_t bitmap;
    XID xids[bitmap_t::CAPACITY];  // one xid per bit position
    LSN last_read_mostly_clsns[bitmap_t::CAPACITY];

    readers_list() {
        memset(xids, '\0', sizeof(XID) * bitmap_t::CAPACITY);
        memset(last_read_mostly_clsns, '\0', sizeof(LSN) * bitmap_t::CAPACITY);
    }
};

uint64_t serial_get_last_read_mostly_cstamp(int xid_idx);
void serial_stamp_last_committed_lsn(LSN lsn);
void serial_deregister_reader_tx(readers_list::bitmap_t* tuple_readers_bitmap);
void serial_register_reader_tx(XID xid, readers_list::bitmap_t* tuple_readers_bitmap);
void serial_register_tx(XID xid);
void serial_deregister_tx(XID xid);

extern readers_list rlist;

struct readers_bitmap_iterator {
    readers_bitmap_iterator(readers_list::bitmap_t* bitmap) :
        array(bitmap->array), cur_entry_index(0), cur_entry(volatile_read(array[0])) {}

    int32_t next(bool skip_self = true);
    uint64_t *array;
    uint32_t cur_entry_index;
    uint64_t cur_entry;
};
};  // end of namespace
#endif
