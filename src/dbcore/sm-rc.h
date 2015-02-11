#pragma once
struct rc_t {
    uint8_t _val;
};
//typedef uint8_t rc_t;   // txn operation (e.g., r/w) return code

// 8 bits for return code:
// bit  meaning
//  7   fatal error, db should stop
//  6   abort due to rw conflict with the read optimization
//  5   SSI determines tx should abort
//  4   should abort due to SI conflict (first writer wins)
//  3   SSN determines tx should abort
//  2   tx should abort, don't care reason
//  1   false (e.g., read a deleted tuple)
//  0   true (success)
//
//  NOTE: SSN/SSI/SI abort code will also have
//  the ABORT bit set for easier checking

#define RC_INVALID              0x0
#define RC_TRUE                 0x1
#define RC_FALSE                0x2
#define RC_ABORT                0x4
#define RC_ABORT_SSN_EXCLUSION  (RC_ABORT | 0x8)
#define RC_ABORT_SI_CONFLICT    (RC_ABORT | 0x10)
#define RC_ABORT_SSI            (RC_ABORT | 0x20)
#define RC_ABORT_RW_CONFLICT    (RC_ABORT | 0x40)
#define RC_FATAL                0x80

inline bool rc_is_abort(rc_t rc)
{
    return rc._val & RC_ABORT;
}

