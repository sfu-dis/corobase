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

class key_writer {
public:
    constexpr key_writer(const varstr *k) : k(k) {}

    inline const varstr *
    fully_materialize(bool stable_input, str_arena &sa)
    {
        if (stable_input || !k)
            return k;
        varstr * const ret = sa(k->size());
        ret->copy_from(k->data(), k->size());
        // copy_from will set size to readsz
        ASSERT(ret->size() == k->size());
        return ret;
    }

private:
    const varstr *k;
};

// does not bother to interpret the bytes from a record
class value_reader {
public:
    constexpr value_reader(size_t max_bytes_read, bool single)
      : px(nullptr), max_bytes_read(max_bytes_read), single(single) {}

    constexpr value_reader(varstr *px, size_t max_bytes_read, bool single)
      : px(px), max_bytes_read(max_bytes_read), single(single) {}

    inline bool
    operator()(const uint8_t *data, size_t sz, str_arena &sa)
    {
        if (not single)
            px = sa(sz);
        const size_t readsz = std::min(sz, max_bytes_read);
        px->copy_from((const char *) data, readsz);
        // copy_from will set size to readsz
        ASSERT(px->size() == readsz);
        return true;
    }

    inline varstr &
    results()
    {
        return *px;
    }

    inline const varstr &
    results() const
    {
        return *px;
    }

private:
    varstr *px;
    size_t max_bytes_read;

    // NOTE (tzwang): the "single" field comes from the single_value_reader
    // that existed in Silo's original codebase. Different from value_reader,
    // single_value_reader needs a varstr given by the user (eg, tpcc) to
    // store the returned value; value_reader, on the other hand, is usually
    // for scans which the user will *not* give a buffer and needs to allocate
    // a *new* one each time do_tuple_read() is invoked so the user can have
    // a list of returned values. So for point queries, single should be true,
    // and for scans it should be false (then the () operator will allocate a
    // new varstr each for each tuple read).
    bool single;
};

class value_writer {
public:
    constexpr value_writer(const varstr *v) : v(v) {}

    inline size_t
    compute_needed(const uint8_t *buf, size_t sz)
    {
        return v ? v->size() : 0;
    }

    inline const varstr *
    fully_materialize(bool stable_input, str_arena &sa)
    {
        if (stable_input || !v)
            return v;
        varstr * const ret = sa(v->size());
        ret->copy_from(v->data(), v->size());
        // copy_from will set size to readsz
        ASSERT(ret->size() == v->size());
        return ret;
    }

    // input [buf, buf+sz) is old value
    inline void
    operator()(uint8_t *buf, size_t sz)
    {
        if (!v)
            return;
        NDB_MEMCPY(buf, v->data(), v->size());
    }
private:
    const varstr *v;
};

