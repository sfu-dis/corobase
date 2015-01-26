#pragma once
struct rc_t {
    uint8_t _val;
};
//typedef uint8_t rc_t;   // txn operation (e.g., r/w) return code

// 8 bits for return code:
// bit  meaning
//  7   fatal error, db should stop
//  6   SSI determines tx should abort
//  5   should abort due to SI conflict (first writer wins)
//  4   SSN determines tx should abort
//  3   tx should abort, don't care reason
//  2   false (e.g., read a deleted tuple)
//  1   true (success)
//  0   invalid
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
#define RC_FATAL                0x80

inline bool rc_is_abort(rc_t rc)
{
    return rc._val & RC_ABORT;
}

