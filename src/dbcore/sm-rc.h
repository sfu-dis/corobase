#pragma once
struct rc_t {
    uint8_t _val;
};
//typedef uint8_t rc_t;   // txn operation (e.g., r/w) return code
#define RC_INVALID 0x0
#define RC_TRUE    0x1  // e.g., tuple found etc.
#define RC_FALSE   0x2  // e.g., not found, read a deleted tuple, etc.
#define RC_ABORT   0x4  // tx should abort, don't care reason
#define RC_ABORT_SSN_EXCLUSION 0x8 // tx should abort due to SSN window excl.
#define RC_ABORT_SI_CONFLICT    0x10    // tx should abort due to SI conflict (first writer wins)
#define RC_ABORT_SSI 0x20
#define RC_FATAL    0x30    // sth is wrong, whole db system should stop

inline bool rc_is_abort(rc_t rc)
{
    return rc._val == RC_ABORT or
           rc._val == RC_ABORT_SSN_EXCLUSION or
           rc._val == RC_ABORT_SI_CONFLICT or
           rc._val == RC_ABORT_SSI;
}

