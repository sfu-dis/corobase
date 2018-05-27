#pragma once

/* Common definitions for the storage manager.
 */

#include "sm-defs.h"
#include "sm-exceptions.h"

#include "size-encode.h"
#include "defer.h"

#include <cstddef>
#include <stdarg.h>
#include <stdint.h>
#include <pthread.h>

#include <dirent.h>
#include <sys/stat.h>
#include <cerrno>

namespace ermia {

typedef uint32_t OID;
typedef uint32_t FID;

static size_t const NUM_LOG_SEGMENT_BITS = 4;
static size_t const NUM_LOG_SEGMENTS = 1 << NUM_LOG_SEGMENT_BITS;

/* A "fat" pointer that carries an address, augmented with encoded
   allocation size information, plus 8 bits for implementations to use
   as they see fit (e.g. to identify pointers that refer to data
   stored in a file rather than memory).

   Fat pointers are primarily used to identify version objects stored
   in various OID arrays (and corresponding log records), but are
   useful any time it is helpful to know the size of an object the
   pointer refers to.

   The size and flag information occupy 16 bits, leaving 48 bits for
   the actual address. On x86_64 architectures, this fits well with
   "canonical" addresses [1], which are interpreted as signed 48-bit
   values. On other systems, some other mechanism for reducing the
   address space to 48 bits might be required.

   In case 48 bits (= 256 TB) is not enough, we could exploit the fact
   that all loggable data types are 16-byte aligned and gain another 4
   bits of address space. 4 PB should keep a main-memory system happy
   for the foreseeable future. For now, though 256TB is plenty.

   [1] See http://en.wikipedia.org/wiki/X86-64#Canonical_form_addresses

   Incidentally, AMD appears to have patented the idea of enforcing
   canonical addresses as 48-bit pointers sign-extended 64 bits:
   http://www.google.com/patents/US6807616. Not particularly relevant
   here, but still an interesting factoid.
 */
struct fat_ptr {
  /* The enums below carve up the range 0x0000 to 0xffff, comprising
     the 16 bits that fat_ptr makes available for non-pointer data.

     The mask 0x00ff is dedicated to a size code. Implementations
     can use this (perhaps in conjunction with other metadata) to
     determine the size of the object this fat_ptr references.

     The mask 0x7f00 is the "address space identifier" (ASI) for
     this fat_ptr (see details below).

     The mask 0x8000 is a dirty flag (the meaning of which is
     implementation-defined, but probably implies a need to write
     the data to disk soon).

     The 7-bit ASI space is divided as follows:

     0x00 -- main memory

     0x01...0x0f -- currently unused

     0x10...0x1f -- LSN; low 4 bits give phsyical log segment file


     0x20...0x2f -- heap file; low 4 bits give heap segment file

     0x30...0x3f -- indirect LSN (points to a fat_pointer of the true location)

     0x40...0x4f -- an XID encoded as a fat_ptr.

     0x50...0x5f -- pointer to some location in the chkpt file.

     0x60...0x7f -- currently unused.
   */
  static uint64_t const VALUE_START_BIT = 16;

  static uint64_t const SIZE_BITS = 8;
  static uint64_t const SIZE_MASK = (1 << SIZE_BITS) - 1;

  static uint64_t const FLAG_START_BIT = SIZE_BITS;
  static uint64_t const FLAG_BITS = VALUE_START_BIT - SIZE_BITS;
  static uint64_t const FLAG_MASK = ((1 << FLAG_BITS) - 1) << FLAG_START_BIT;

  static uint64_t const ASI_START_BIT = FLAG_START_BIT;
  static uint64_t const ASI_BITS = FLAG_BITS - 1;
  static uint64_t const ASI_MASK = (1 << ASI_BITS) - 1;
  static uint64_t const ASI_FLAG_MASK = ASI_MASK << ASI_START_BIT;

  static uint64_t const DIRTY_BIT = VALUE_START_BIT - 1;
  static uint64_t const DIRTY_MASK = 1 << DIRTY_BIT;

  static uint64_t const ASI_LOG = 0x10;
  static uint64_t const ASI_HEAP = 0x20;
  static uint64_t const ASI_EXT = 0x30;
  static uint64_t const ASI_XID = 0x40;
  static uint64_t const ASI_CHK = 0x50;

  static uint64_t const ASI_LOG_FLAG = ASI_LOG << ASI_START_BIT;
  static uint64_t const ASI_HEAP_FLAG = ASI_HEAP << ASI_START_BIT;
  static uint64_t const ASI_EXT_FLAG = ASI_EXT << ASI_START_BIT;
  static uint64_t const ASI_XID_FLAG = ASI_XID << ASI_START_BIT;
  static uint64_t const ASI_CHK_FLAG = ASI_CHK << ASI_START_BIT;

  static uint64_t const ASI_SEGMENT_MASK = 0x0f;

  static_assert(not((ASI_LOG | ASI_HEAP | ASI_EXT | ASI_XID | ASI_CHK) &
                    ~ASI_MASK),
                "Go fix ASI_MASK");
  static_assert(NUM_LOG_SEGMENTS == 16, "The constant above is out of sync");
  static_assert(FLAG_BITS >= 1 + 1 + NUM_LOG_SEGMENT_BITS, "Need more bits");

  /* Make a fat_ptr that points to an address in memory.
   */
  static fat_ptr make(void *ptr, uint8_t sz_code, uint16_t flags = 0) {
    union {
      void *v;
      intptr_t n;
    } u = {ptr};
    return make(u.n, sz_code, flags);
  }

  static fat_ptr make(uintptr_t n, uint8_t sz_code, uint16_t flags = 0) {
    ASSERT(not(flags & SIZE_MASK));
    n <<= VALUE_START_BIT;
    n |= sz_code;
    n |= flags;
    return (fat_ptr){n};
  }

  uint64_t _ptr;

  template <typename T>
  operator T *() const {
    ASSERT(not asi());
    return (T *)offset();
  }

  uintptr_t offset() const { return _ptr >> VALUE_START_BIT; }

  uint8_t size_code() const { return _ptr & SIZE_MASK; }

  // return the ASI as a 16-bit number
  uint16_t asi() const { return (_ptr >> ASI_START_BIT) & ASI_MASK; }

  uint16_t asi_type() const { return asi() & ~ASI_SEGMENT_MASK; }

  uint16_t asi_segment() const { return asi() & ASI_SEGMENT_MASK; }

  uint16_t is_dirty() const { return _ptr & DIRTY_MASK; }

  // return dirty + ASI without any shifting
  uint16_t flags() const { return _ptr & FLAG_MASK; }

  /* Return the log segment of this fat_ptr, or -1 if not in the log
     ASI.
   */
  int log_segment() const {
    return (asi_type() == ASI_LOG) ? asi_segment() : -1;
  }

  int heap_segment() const {
    return (asi_type() == ASI_HEAP) ? asi_segment() : -1;
  }

  int ext_segment() const {
    return (asi_type() == ASI_EXT) ? asi_segment() : -1;
  }

  bool operator==(fat_ptr const &other) const { return _ptr == other._ptr; }

  bool operator!=(fat_ptr const &other) const { return _ptr != other._ptr; }
};

static inline fat_ptr volatile_read(fat_ptr volatile &p) {
  return fat_ptr{volatile_read(p._ptr)};
}

static inline void volatile_write(fat_ptr volatile &x, fat_ptr const &y) {
  volatile_write(x._ptr, y._ptr);
}

// The equivalent of a NULL pointer
static fat_ptr const NULL_PTR = {0};

/* Log sequence numbers are strange beasts. They increase
   monotonically, in order to give a total order to all logged events
   in the system, but they must also identify the location of a log
   record on disk. We accomplish this in a similar fashion to fat_ptr,
   exploiting the fact that we have to be able to embed an LSN in a
   fat_ptr. That means each LSN can only have 48 significant bits, but
   it also means we can embed the physical segment number. As long as
   we know the starting LSN of each segment, an LSN can be converted
   to a fat_ptr and vice-versa in constant time.

   LSN embeds a size_code, just like fat_ptr does, but it is not
   directly used by the log. Instead, the LSN size_code is used when
   converting between LSN and fat_ptr, and corresponds to a size for
   the object that was encoded (identified by its FID, with associated
   scaling factor). An LSN that is not associated with any FID will
   use INVALID_SIZE_CODE as its size_code.

   For comparison purposes, we can leave all the extra bits in place:
   if two LSN differ, they do so in the most significant bits and the
   trailing extras won't matter. If two LSN are the same, they should
   have the same trailing bits as well (if they do not, undefined
   behaviour should be expected).
 */
struct LSN {
  static LSN make(uintptr_t val, int segnum,
                  uint8_t size_code = INVALID_SIZE_CODE) {
    ASSERT(not(segnum & ~fat_ptr::ASI_SEGMENT_MASK));
    uintptr_t flags = segnum << fat_ptr::ASI_START_BIT;
    return (LSN){(val << fat_ptr::VALUE_START_BIT) | flags | size_code};
  };

  static LSN from_ptr(fat_ptr const &p) {
    THROW_IF(
        p.asi_type() != fat_ptr::ASI_LOG and p.asi_type() != fat_ptr::ASI_EXT,
        illegal_argument, "Attempt to convert non-LSN fat_ptr to LSN");

    return LSN::make(p.offset(), p.asi_segment());
  }

  fat_ptr to_ptr() const { return fat_ptr{_val}; }

  uint64_t _val;

  fat_ptr to_log_ptr() const { return fat_ptr{_val | fat_ptr::ASI_LOG_FLAG}; }
  fat_ptr to_ext_ptr() const { return fat_ptr{_val | fat_ptr::ASI_EXT_FLAG}; }

  uintptr_t offset() const { return _val >> fat_ptr::VALUE_START_BIT; }
  uint16_t flags() const { return _val & fat_ptr::ASI_FLAG_MASK; }
  uint32_t segment() const {
    return (_val >> fat_ptr::ASI_START_BIT) & fat_ptr::ASI_SEGMENT_MASK;
  }
  uint8_t size_code() const { return _val & fat_ptr::SIZE_MASK; }

  /* Create a new LSN that has advanced by [delta] bytes in the
     current segment. The caller is responsible to deal with the
     case where advancing [delta] bytes would overflow the segment.
   */
  LSN advance_within_segment(uint64_t delta) {
    delta <<= fat_ptr::VALUE_START_BIT;
    return (LSN){_val + delta};
  }

  // true comparison operators
  bool operator<(LSN const &other) const { return _val < other._val; }
  bool operator==(LSN const &other) const { return _val == other._val; }

  // synthesized comparison operators
  bool operator>(LSN const &other) const { return other < *this; }
  bool operator<=(LSN const &other) const { return not(*this > other); }
  bool operator>=(LSN const &other) const { return other <= *this; }
  bool operator!=(LSN const &other) const { return not(*this == other); }
};

static inline LSN volatile_read(LSN volatile &x) {
  return LSN{volatile_read(x._val)};
}

static LSN const INVALID_LSN = {0};

/* Transaction ids uniquely identify each transaction in the system
   without implying any particular ordering. Over time they show an
   increasing trend, but the sequence can have gaps and significant
   local disorder.

   Although they occupy 64 bits, XIDs are actually the composition of
   a 16-bit local identifier and a 32-bit epoch number (with 16 bits
   unused). The combination is globally unique (overflow would take
   roughly 7 years at 1Mtps), and any two transactions that coexist
   are guaranteed to have different local identifiers.
 */
struct XID {
  static XID make(uint32_t e, uint16_t i) {
    uint64_t x = e;
    x <<= 16;
    x |= i;
    x <<= 16;
    x |= fat_ptr::ASI_XID_FLAG;
    x |= INVALID_SIZE_CODE;
    return XID{x};
  }

  static XID from_ptr(fat_ptr const &p) {
    LOG_IF(FATAL, p.asi_type() != fat_ptr::ASI_XID)
      << "Attempt to convert non-XID fat_ptr to XID";
    return XID{p._ptr};
  }

  uint64_t _val;

  fat_ptr to_ptr() const { return fat_ptr{_val | INVALID_SIZE_CODE}; }
  uint32_t epoch() const { return _val >> 32; }
  uint16_t local() const { return _val >> 16; }
  uint16_t flags() const { return _val & fat_ptr::FLAG_MASK; }

  // true comparison operators
  bool operator==(XID const &other) const { return _val == other._val; }

  // synthesized comparison operators
  bool operator!=(XID const &other) const { return not(*this == other); }
};

static inline XID volatile_read(XID volatile &x) {
  return XID{volatile_read(x._val)};
}

static XID const INVALID_XID = {fat_ptr::ASI_XID_FLAG};

/* All memory associated with an OID uses this standard
   header. Standardizing allows garbage collection and other
   background tasks to work with minimal domain-specific knowledge.
 */
struct version {
  /* the address of this version on disk, if durable.

     Durable versions can be evicted and their memory reclaimed for
     other uses;
   */
  fat_ptr disk_addr;

  // the previous version of this object, if any.
  fat_ptr prev;

  // commit LSN of the transaction that created this version
  LSN begin;

  /* Commit LSN of the transaction that destroyed this version

     Updating a record sets [end] and links the version as [prev] to
     its replacement.

     Deleting a record sets [end] without creating a newer
     version. The object is officially dead at that point, and no
     more versions can be created until the OID is recycled.
   */
  LSN end;

  char data[];
};

/* Wrapper for pthread mutex lock/unlock functions. It is still POD.
 */
struct os_mutex_pod {
  static constexpr os_mutex_pod static_init() {
    return os_mutex_pod{PTHREAD_MUTEX_INITIALIZER};
  }

  void lock() {
    int err = pthread_mutex_lock(&_mutex);
    DIE_IF(err, "pthread_mutex_lock returned %d", err);
  }

  // return true if the mutex was uncontended
  bool try_lock() {
    int err = pthread_mutex_trylock(&_mutex);
    if (err) {
      DIE_IF(err != EBUSY, "pthread_mutex_trylock returned %d", err);
      return false;
    }
    return true;
  }

  void unlock() {
    int err = pthread_mutex_unlock(&_mutex);
    DIE_IF(err, "pthread_mutex_unlock returned %d", err);
  }

  pthread_mutex_t _mutex;
};

struct os_mutex : os_mutex_pod {
  os_mutex() : os_mutex_pod(static_init()) {}
  ~os_mutex() {
    int err = pthread_mutex_destroy(&_mutex);
    DIE_IF(err, "pthread_mutex_destroy returned %d", err);
  }
};

struct os_condvar_pod {
  static constexpr os_condvar_pod static_init() {
    return os_condvar_pod{PTHREAD_COND_INITIALIZER};
  }

  void wait(os_mutex_pod &mutex) {
    int err = pthread_cond_wait(&_cond, &mutex._mutex);
    DIE_IF(err, "pthread_cond_wait returned %d", err);
  }

  int timedwait(os_mutex_pod &mutex, struct timespec *abstime) {
    return pthread_cond_timedwait(&_cond, &mutex._mutex, abstime);
  }

  void signal() {
    int err = pthread_cond_signal(&_cond);
    DIE_IF(err, "pthread_cond_signal returned %d", err);
  }

  void broadcast() {
    int err = pthread_cond_broadcast(&_cond);
    DIE_IF(err, "pthread_cond_broadcast returned %d", err);
  }

  pthread_cond_t _cond;
};

struct os_condvar : os_condvar_pod {
  os_condvar() : os_condvar_pod(static_init()) {}

  ~os_condvar() {
    int err = pthread_cond_destroy(&_cond);
    DIE_IF(err, "pthread_cond_destroy returned %d", err);
  }
};

/* Like the normal asprintf, but either return the new string or
   throws the error that prevented creating it.
 */
char *os_asprintf(char const *msg, ...) __attribute__((format(printf, 1, 2)));

int os_open(char const *path, int flags);

int os_openat(int dfd, char const *fname, int flags);

void os_write(int fd, void const *buf, size_t bufsz);
size_t os_pwrite(int fd, char const *buf, size_t bufsz, off_t offset);
size_t os_pread(int fd, char *buf, size_t bufsz, off_t offset);

void os_truncate(char const *path, size_t size = 0);
void os_truncateat(int dfd, char const *path, size_t size = 0);

void os_renameat(int tofd, char const *from, int fromfd, char const *to);

void os_unlinkat(int dfd, char const *fname, int flags = 0);

void os_fsync(int fd);

void os_close(int fd);

int os_dup(int fd);

/* like POSIX snprintf, but throws on error (return what-if size on overflow).

   WARNING: unlike snprintf, this function sets the last byte of [buf]
   to NUL. Callers who forget to test the return value will find a
   truncated string rather instead of going into la-la land.
 */
size_t os_snprintf(char *buf, size_t size, char const *fmt, ...)
    __attribute__((format(printf, 3, 4)));

/* A class for iterating over file name entries in a directory using a
   range-based for loop.

   WARNING: the class only supports one iteration at a time, and
   implement just enough of the iterator protocol to work with
   range-based for loops. In particular, this means that operator!=
   can only distinguish between "end" and "not end."
 */
struct dirent_iterator {
  struct iterator {
    DIR *_d;
    dirent *_dent;
    void operator++();
    char const *operator*() { return _dent->d_name; }
    bool operator!=(iterator const &other) { return _d != other._d; }
  };

  dirent_iterator(char const *dname);

  iterator begin();
  iterator end();

  int dup();

  ~dirent_iterator();

  // sorry, can't copy or move
  dirent_iterator(dirent_iterator const &other) = delete;
  dirent_iterator(dirent_iterator &&other) = delete;
  void operator=(dirent_iterator other) = delete;

  DIR *_d;
  bool used;
};

struct tmp_dir {
  char dname[24];
  tmp_dir();
  ~tmp_dir();
  operator char const *() { return dname; }
  char const *operator*() { return *this; }
};

}  // namespace ermia
