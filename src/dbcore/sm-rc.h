#pragma once
#include <stdint.h>

// 8 bits for return code:
// bit  meaning
//  8   user requested abort
//  7   there's phantom, tx should abort
//  6   abort due to rw conflict with the read optimization
//  5   SSN/SSI determines tx should abort
//  4   should abort due to SI conflict (first writer wins)
//  3   tx should abort due to internal error (eg got an invalid cstamp)
//  2   abort marker - must be used in conjunction with one of the detailed
//  reasons
//  1   false (e.g., read a deleted tuple)
//  0   true (success)
//
//  NOTE: SSN/SSI/SI abort code will also have
//  the ABORT bit set for easier checking

#define RC_INVALID 0x0
#define RC_TRUE 0x1
#define RC_FALSE 0x2
#define RC_ABORT 0x4
#define RC_ABORT_INTERNAL (RC_ABORT | 0x8)
#define RC_ABORT_SI_CONFLICT (RC_ABORT | 0x10)
#define RC_ABORT_SERIAL (RC_ABORT | 0x20)
#define RC_ABORT_RW_CONFLICT (RC_ABORT | 0x40)
#define RC_ABORT_PHANTOM (RC_ABORT | 0x80)
#define RC_ABORT_USER (RC_ABORT | 0x100)

// Operation (e.g., r/w) return code
struct rc_t {
  uint16_t _val;

  rc_t() : _val(RC_INVALID) {}
  rc_t(uint16_t v) : _val(v) {}

  inline bool IsUserAbort() { return _val == RC_ABORT_USER; }
  inline bool IsInvalid() { return _val == RC_INVALID; }
  inline bool IsAbort() { return _val & RC_ABORT; }
};


