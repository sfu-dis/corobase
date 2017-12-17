// -*- mode:c++ -*-
#pragma once
#include <atomic>
#include <mutex>
#include <vector>

#include "epoch.h"
#include "sm-common.h"
#include "sm-config.h"
#include "../macros.h"

class transaction;
namespace TXN {

enum txn_state {
  TXN_ACTIVE,
  TXN_COMMITTING,
  TXN_CMMTD,
  TXN_ABRTD,
  TXN_INVALID
};

struct xid_context {
  epoch_mgr::epoch_num begin_epoch;  // tx start epoch, not owner.local()
  XID owner;
  uint64_t begin;
  uint64_t end;
#ifdef SSN
  uint64_t pstamp;               // youngest predecessor (\eta)
  std::atomic<uint64_t> sstamp;  // oldest successor (\pi)
  bool set_sstamp(uint64_t s);
#endif
#ifdef SSI
  uint64_t ct3;  // smallest commit stamp of T3 in the dangerous structure
  uint64_t last_safesnap;
#endif
  transaction *xct;
  txn_state state;

#ifdef SSN
  const static uint64_t sstamp_final_mark = 1UL << 63;
  inline void finalize_sstamp() {
    std::atomic_fetch_or(&sstamp, sstamp_final_mark);
  }
  inline void set_pstamp(uint64_t p) {
    volatile_write(pstamp, std::max(pstamp, p));
  }
#endif
  inline bool verify_owner(XID assumed) {
    return volatile_read(owner) == assumed;
  }
};

static uint16_t const NCONTEXTS = 32768;
extern xid_context contexts[NCONTEXTS];

/* Request a new XID and an associated context. The former is globally
   unique and the latter is distinct from any other transaction whose
   lifetime overlaps with this one.
 */
XID xid_alloc();

static size_t const NBITMAPS = 4;
struct xid_bitmap {
  static size_t const constexpr BITS_PER_WORD = 8 * sizeof(uint64_t);
  static size_t const constexpr NWORDS = NCONTEXTS / 2 / BITS_PER_WORD;
  static_assert(not(NCONTEXTS % (2 * BITS_PER_WORD)),
                "NCONTEXTS must must divide cleanly by 128");

  uint64_t data[NWORDS];
  size_t widx;
};
extern xid_bitmap xid_bitmaps[NBITMAPS];

/* Release an XID and its associated context. The XID will no longer
   be associated with any context after this call returns.
 */
inline void xid_free(XID x) {
  auto id = x.local();
  ASSERT(id < NCONTEXTS);
  auto *ctx = &contexts[id];
  ASSERT(ctx->state == TXN_CMMTD or ctx->state == TXN_ABRTD);
  THROW_IF(ctx->owner != x, illegal_argument, "Invalid XID");
  // destroy the owner field (for SSN read-opt, which might
  // read very stale XID and try to find its context)
  ctx->owner._val = 0;

  auto &b = xid_bitmaps[x.epoch() % NBITMAPS];
  auto &w = b.data[(id / xid_bitmap::BITS_PER_WORD) % xid_bitmap::NWORDS];
  auto bit = id % xid_bitmap::BITS_PER_WORD;
  __sync_fetch_and_or(&w, uint64_t(1) << bit);
}

/* Return the context associated with the givne XID.

   throw illegal_argument if [xid] is not currently associated with a context.
 */
inline xid_context *xid_get_context(XID x) {
  auto *ctx = &contexts[x.local()];
  // read a consistent copy of owner (in case xid_free is destroying
  // it while we're trying to use the epoch() fields)
  XID owner = volatile_read(ctx->owner);
  if (!owner._val) {
    return nullptr;
  }
  ASSERT(owner.local() == x.local());
  if (owner.epoch() < x.epoch() or owner.epoch() >= x.epoch() + 3) {
    return nullptr;
  }
  return ctx;
}

struct thread_data {
  epoch_mgr::epoch_num epoch;
  uint64_t bitmap;
  uint16_t base_id;
  bool initialized;
};

inline XID take_one(thread_data *t) {
  ASSERT(t->bitmap);
  DEFER(t->bitmap &= (t->bitmap - 1));
  auto id = t->base_id + __builtin_ctzll(t->bitmap);
  auto x = contexts[id].owner = XID::make(t->epoch, id);
#ifdef SSN
  contexts[id].sstamp = 0;
  contexts[id].pstamp = 0;
  contexts[id].xct = NULL;
#endif
#ifdef SSI
  contexts[id].ct3 = 0;
  contexts[id].last_safesnap = 0;
  contexts[id].xct = NULL;
#endif
  // Note: transaction needs to initialize xc->begin in ctor
  contexts[id].end = 0;
  ASSERT(contexts[id].state != TXN_COMMITTING);
  contexts[id].state = TXN_ACTIVE;
  contexts[id].xct = nullptr;
  return x;
}
#if defined(SSN) or defined(SSI)
inline txn_state spin_for_cstamp(XID xid, xid_context *xc) {
  txn_state state;
  do {
    state = volatile_read(xc->state);
    if (volatile_read(xc->owner) != xid) {
      return TXN_INVALID;
    }
  } while (state != TXN_CMMTD and state != TXN_ABRTD);
  return state;
}
#endif
};  // end of namespace
