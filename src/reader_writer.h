#pragma once
#include "macros.h"
#include "str_arena.h"

class key_reader {
public:
    inline ALWAYS_INLINE const varstr &
    operator()(const varstr &s)
    {
        return s;
    }

#ifdef MASSTREE
    inline ALWAYS_INLINE lcdf::Str operator()(lcdf::Str s) {
        return s;
    }
#endif
};

