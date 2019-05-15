#pragma once

#include "sm-defs.h"

namespace ermia {

/* An implementation of second-chance hashing, courtesy of Kirsch and
   Mitzenmacher [1].

   There are several things going on in a table with N slots:

   First: wide buckets. We aggregate slots into buckets of
   M slots each, and allow an entry to reside in any of the slots of a
   bucket it maps to. This allows the table to absorb more collisions,
   and each doubling of the bucket size halves the collision rate
   (thus doubling the effective occupancy of the table before we even
   try to resolve collisions).

   Second: multiple choice hashing. By computing 2+ hash functions for
   the value (or partitioning the bits of a single hash), we can map
   it to multiple buckets, again reducing the chances of a
   collision. This optimization more than doubles effective occupancy.

   Third: cascading levels. We partition our table into K subtables,
   or levels, and use one of our hashes for each level. We prefer to
   add to lower-numbered tables, and so we only add an entry to
   level[i] if the value's spot is already taken in levels 0, 1, ...,
   i-1. This cascading method allows significantly higher occupancy
   than a "free for all" arrangement (15-40% higher in my
   experiments). It also improves locality in probes of underfilled
   tables, because filled slots tend to cluster together.

   Fourth: a restricted form of cuckoo hashing. It makes intuitive
   sense that there is something to be gained if you are willing to
   move some existing values around. However, we only allow *ONE* move
   per insertion, thus avoiding the infinite loop fun that cuckolding
   expeditions usually risk. We invoke our move whenever we are about
   to cascade from level[i] to level[i+1]. Before doing so, we check
   whether any of the current occupants at level[i-1], who caused us
   to cascade to level[i] in the first place, can themselves be
   evicted to level[i]. The intuition is that the table is "full" when
   the first entry fails to find a home in any level. An insert at
   level[i] is always preferable to one at level[i+1], because it
   slows the growth of higher levels. This last trick can boost
   occupancy by more than 40%.

   The optimizations are cumulative, though with some overlap. In a
   table with 256 slots and uniformly random insertions from a much
   larger domain, we measure how many elements can be inserted into
   the table before a collision prevents the insertion. The average
   over 1000 runs tells the story:

   vanilla hashing	 20/256
   add 4-wide buckets	100/256
   add 2-choice hashing	150/256
   add level cascading	165/256
   add second-chance	223/256

   Again, all five of the above examples use the same size table, but
   combining all the optimizations improves effective occupancy from
   8% to 87%. Meanwhile, the average cost to access an element remains
   low, at 1.1 hash functions computed and 1.6 cache misses (anything
   above 1.0 in either category is due to either level cascades or
   attempted moves).

   [1] http://dl.acm.org/citation.cfm?id=1959422

   Implementation note: right now, we only accept T if has a suitable
   operator==, rather than accepting a comparator as a template
   parameter. We also do not store precomputed hash codes in the table
   with their elements, on the assumption that equality testing is
   cheap enough, and cuckoldings rare enough, that the extra space and
   complexity are not worth it. In any case, the user is free to embed
   a hash code in T, with a "hash function" that just returns it.
 */
template <size_t SLOTS, typename T, typename Hash, typename Equals = void,
          size_t SUBTABLES = 2, size_t BUCKET_SIZE = 4, bool USE_LEVELS = true,
          bool SECOND_CHANCE = true>
struct sc_hash_set {
  /* std::equal_to requires both objects to have the same type. We
     want to relax that a bit to accept any type that T claims it
     can compare against.
   */
  struct cmpeq {
    template <typename K>
    decltype(std::declval<T>() == std::declval<K>()) operator()(
        T const &t, K const &k) const {
      return t == k;
    }
  };

  typedef T value_type;
  typedef Hash hash_function;
  typedef typename std::conditional<std::is_same<void, Equals>::value, cmpeq,
                                    Equals>::type equals_function;
  typedef decltype(std::declval<hash_function>()(std::declval<T>())) hash_type;
  typedef uint64_t bitmap_word;

  enum : size_t { N = SLOTS };
  enum : size_t { K = SUBTABLES };
  enum : size_t { M = BUCKET_SIZE };

  /* How many bits of hash do we need? For a one-level table, we
     need just lg N bits. For a K-level table, though, we need to
     have lg N/K bits per level, or K lg N/K in total.
   */
  enum : int { H_BITS = 8 * sizeof(hash_type) };

  enum : int { HP_BITS = __builtin_ctzll(N / M / K) };
  enum : hash_type { HP_MASK = (hash_type(1) << HP_BITS) - 1 };

  enum : size_t { W_BITS = 8 * sizeof(bitmap_word) };

  enum : size_t { NP = align_up(N, W_BITS) / W_BITS };

  static constexpr bool IS_MOVABLE = (std::is_move_constructible<T>::value and
                                      std::is_move_assignable<T>::value);
  static_assert(IS_MOVABLE, "T must be movable");

  static_assert(N and not(N & (N - 1)), "SLOTS must be a power of two");
  static_assert(K and not(K & (K - 1)), "SUBTABLES must be a power of two");
  static_assert(M and not(M & (M - 1)), "BUCKET_SIZE must be a power of two");
  static_assert(not(N % (K * M)),
                "SLOTS must be a multiple of SUBTABLES*BUCKET_SIZE");
  static_assert(HP_BITS * K <= H_BITS, "Not enough bits in the hash function");
  static_assert(USE_LEVELS or not SECOND_CHANCE,
                "Second chance feature requires levels to be enabled");

  /* In order to avoid constructor/destructor problems, the bucket
     array holds untyped slabs of bytes for storage. Then, we use
     emplacement, std::swap and move-construction to change an
     element's location.
  */
  struct __attribute__((aligned(alignof(value_type)))) value_space {
    char data[sizeof(value_type)];
    operator char *() { return data; }
  };

  struct iterator {
    sc_hash_set *owner;
    size_t pos;

    value_type *operator->() { return _get(); }
    value_type &operator*() { return *_get(); }

    iterator &operator++() {
      ++pos;
      _find_valid();
      return *this;
    }

    bool operator!=(iterator const &other) const {
      return pos != other.pos or owner != other.owner;
    }
    bool operator==(iterator const &other) const { return not(*this != other); }

    void _find_valid() {
      /* Find the next unseen bit in a non-empty presence word,
         possibly including the current one. Mask out
         already-seen bits for the current word so we don't
         revisit them (or visit new entries added after we
         pass). If a valid bit is found, don't mark it as seen
         here (that happens in operator++).
       */
      while (pos < N) {
        auto bit = bitmap_word(1) << (pos % W_BITS);
        auto pending = owner->_presence[pos / W_BITS] & -bit;
        auto i = pending ? __builtin_ctzll(pending) : W_BITS;
        pos = align_down(pos, W_BITS) + i;
        if (pending) break;
      }
    }

    value_type *_get() { return owner->_get_value(pos); }
  };

  hash_function _hash;
  equals_function _equal;
  bitmap_word _presence[NP];
  value_space _buckets[N];

 private:
  struct dummy_struct {};

 public:
  /* No-arg constructor only available if the hash function allows it
   */
  sc_hash_set(typename std::enable_if<
                  std::is_default_constructible<hash_function>::value,
                  dummy_struct>::type = dummy_struct{}) {
    objzero(_presence);
  }

  template <typename H>
  sc_hash_set(
      H &&hash,
      typename std::enable_if<std::is_convertible<H, hash_function>::value, dummy_struct>::type dummy = dummy_struct{})
      : _hash(std::forward<H>(hash)) {
    MARK_REFERENCED(dummy);
    objzero(_presence);
  }

  ~sc_hash_set() { clear(); }

  void clear() {
    /* Have to destruct all entries. Abuse our normal iterator to
       get the job done. If there is no destructor, this loop
       becomes a no-op and should be optimized away.
    */
    if (not std::is_trivially_destructible<value_type>::value) {
      for (auto it = begin(); it != end(); ++it) _destroy_value(it.pos);
    }

    objzero(_presence);
  }

  /* Not especially cheap, but gets the job done */
  size_t size() {
    size_t rval = 0;
    for (auto x : _presence) rval += __builtin_popcountll(x);
    return rval;
  }

  iterator begin() {
    iterator it{this, 0};
    it._find_valid();
    return it;
  }
  iterator end() { return iterator{this, N}; }

  template <typename U>
  std::pair<iterator, int> insert(U &&elem) {
    return emplace(std::forward<U>(elem));
  }

  /* Construct a new element for the set, using the [args] provided,
     and attempt to move it into the set.

     If that element was already present, return an iterator to the
     existing element, and the error code 1.

     If the element was newly added to the set, return an iterator
     to it and error code 0.

     If the element could not be inserted due to an unresolved
     collision, return an iterator to the offending element and the
     error code -1. The caller may choose to swap the two elements

     NOTE: The probability of a collision starts is nonzero even for
     an underfilled set, and approaches one as the available space
     fills.

     NOTE: the newly constructed element will have to be moved into
     location, but this should not be too onerous as elements must
     be movable in any case.
   */
  template <typename... Args>
  std::pair<iterator, int> emplace(Args &&... args) {
    /* It's more than a little scary to pass the same thing twice,
       once as an rvalue reference, but our callee knows this
       could happen and will not access the key after draining the
       rvalue reference.
     */
    value_type elem{std::forward<Args>(args)...};
    return find_and_emplace(elem, std::move(elem));
  }

  /* Like emplace(), but uses [k] for the initial probing and---only
     if the insert succeeds---constructs a new element in place
     using the provided [args]. If second-chance moves are not
     enabled, the inserted element will never be moved.
   */
  template <typename Key, typename... Args>
  std::pair<iterator, int> find_and_emplace(Key const &x, Args &&... args) {
    auto h = _hash(x);
    {
      /* First, check whether it's there already */
      auto it = _find(x, h);
      if (it != end()) return std::make_pair(it, 1);
    }

    /* Now that we know it's not there, probe again looking for
       space to put it in.
     */
    size_t old_pos;  // let's see if gcc complains :)
    for (size_t k = 0; k < K; k++) {
      auto pos = _get_bucket(h, k);
      for (size_t i = pos; i < pos + M; i++) {
        auto it = iterator{this, i};
        if (not _is_present(i)) {
          // eureka!
          _init_value(i, std::forward<Args>(args)...);
          return std::make_pair(it, 0);
        }
        ASSERT(not _equal(*it, x));
      }

      // bucket at level[k] is full

      if (SECOND_CHANCE and k) {
        /* Try to evict somebody else from level[k-1] to this
           level before moving to level[k+1]?
        */
        for (size_t j = old_pos; j < old_pos + M; j++) {
          auto &y = *_get_value(j);
          auto hy = _hash(y);
          auto py = _get_bucket(hy, k);

          for (size_t i = py; i < py + M; i++) {
            if (not _is_present(i)) {
              // eureka!
              _init_value(i, std::move(y));
              y = value_type(std::forward<Args>(args)...);
              return std::make_pair(iterator{this, j}, 0);
            }
          }
        }
      }

      // remember for next time
      old_pos = pos;
    }

    // no space!
    return std::make_pair(end(), -1);
  }

  /* Probe the set for an element that matches [k]. If found, return
     and iterator to that element; if not found, return end().

     This function is useful if the element is expensive (or
     impossible) to construct or copy, and can be uniquely
     identified by a subset of its fields.
   */
  template <typename Key>
  iterator _find(Key const &x, hash_type h) {
    for (size_t k = 0; k < K; k++) {
      size_t pos = _get_bucket(h, k);
      for (size_t i = pos; i < pos + M; i++) {
        if (_is_present(i) and _equal(*_get_value(i), x))
          return iterator{this, i};
      }
    }
    return end();
  }

  template <typename Key>
  iterator find(Key const &x) {
    auto h = _hash(x);
    return _find(x, h);
  }
  template <typename Key>
  bool erase(Key const &k) {
    auto rval = find(k);
    if (rval != end()) {
      erase(rval);
      return true;
    }
    return false;
  }

  /* Erase the element pointed to by [it].

     Dereferencing the iterator after this operation invokes
     undefined behavior, but the iterator can still be advanced
     safely (erasing the current element does not change which
     element will be visited next).
   */
  void erase(iterator const &it) {
    ASSERT(it != end());
    ASSERT(_is_present(it.pos));
    _destroy_value(it.pos);
    _unset_present(it.pos);
  }

  template <typename... Args>
  value_type *_init_value(size_t pos, Args &&... args) {
    auto *rval = new (_get_value(pos)) value_type(std::forward<Args>(args)...);
    _set_present(pos);
    return rval;
  }

  value_type *_get_value(size_t pos) {
    return (value_type *)_buckets[pos].data;
  }

  void _destroy_value(size_t pos) {
    if (not std::is_trivially_destructible<value_type>::value)
      _get_value(pos)->~value_type();
  }

  std::pair<bitmap_word *, bitmap_word> _presence_info(size_t pos) {
    return std::make_pair(&_presence[pos / W_BITS], bitmap_word(1)
                                                        << (pos % W_BITS));
  }

  bool _is_present(size_t pos) {
    auto pi = _presence_info(pos);
    return *pi.first & pi.second;
  }

  void _set_present(size_t pos) {
    auto pi = _presence_info(pos);
    *pi.first |= pi.second;
  }

  void _unset_present(size_t pos) {
    auto pi = _presence_info(pos);
    *pi.first &= ~pi.second;
  }

  size_t _get_bucket(hash_type h, int i) {
    h = _hash_part(h, i);
    auto nbuckets = N / M;
    size_t idx;
    if (USE_LEVELS) {
      auto subtsize = nbuckets / K;
      idx = subtsize * i + (h & (subtsize - 1));
    } else {
      idx = h & (nbuckets - 1);
    }
    return M * idx;
  }

  hash_type _hash_part(hash_type h, size_t i) {
    i *= HP_BITS;
    return (h >> i) & HP_MASK;
  }
};
}  // namespace ermia
