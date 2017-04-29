// -*- mode:c++ -*-
#ifndef __SM_LOG_DEFS_H
#define __SM_LOG_DEFS_H

/***
    Internal definitions used by the various bits of the log manager.

    Should not be exposed to other parts of the system, because
    sm-log.h is the official interface to the log.
 */
#include "sm-log.h"

#include "adler.h"
#include "cslist.h"
#include "rcu-slist.h"
#include "stub-impl.h"
#include "window-buffer.h"

#include <pthread.h>

/* The system's storage management strategy assumes that space becomes
   free for one of two reasons: "young" allocations are freed because
   they either fail to commit or are deleted while still "hot." "Old"
   allocations are also freed when they are archived to the OLAP
   store.

   We therefore have two sets of files:

   The "log" is the site of all new storage allocation, and is divided
   into sixteen regions. From oldest to newest:

   [clean] [cleaning] [cold] ... [cooling] [active]

   Whenever the active segment fills, the system will "open" a new
   segment by taking over the space occupied by the oldest segment
   (which must be [clean]). Cleaning involves moving all live records
   from the segment to the "heap." These cannot be write-hot (if they
   are still live after this long), but may still be read-hot (and
   therefore unsuitable for archiving). We move these records to the
   heap to avoid unnecessary write amplification during log cleaning.
   
   In order to prevent the log from becoming wedged, we maintain the
   invariant that there must always be enough space in the [active]
   segment (plus any [clean] segments) to absorb the checkpoint and
   relocation records that would be required to clean an entire
   [cold] segment. To avoid blocking log insertions during bursts of
   log traffic, the system attempts to clean old segments fast enough
   to maintain a tunable number of empty segments. Keeping more empty
   segments allows larger bursts to be absorbed, but wastes space and
   increases write amplification (because objects have less time to be
   invalidated before they have to be moved).

   The heap is also divided into regions, similar to the log. The only
   significant change is that cleaning moves records to the head of
   the heap, rather than to a third location. The archiving process,
   which is completely independent, will eventually drain records from
   the heap. In order to detect that the heap is filling faster than
   the archiving process drains it, we maintain two "active" segments:
   the proper [active] segment is the destination for records
   reclaimed from the log. A secondary [receiving] segment records
   reclaimed from the end of the heap. When the [active] segment
   fills, it takes over the [receiving] segment; if the latter is over
   half full at the time it becomes [active], it would indicate that
   new records are arriving faster than they are being archived.

   NOTE: the system allows changes to the log and heap file sizes, but
   the changes take effect gradually: Whenever a new segment is
   opened, it takes the size then configured. With 16 segments to each
   file, it would make little difference (while adding significant
   complexity) to truncate or expand an already-active segment, as the
   latter represents only 6% of the total log/heap space.
 */


enum log_record_flags : uint8_t {
    /* This log record has a payload stored somewhere in the log
     */
    
    LOG_FLAG_HAS_PAYLOAD = 0x80,

    /* This log record's payload area contains a fat_ptr that
       identifies the actual location of the payload. Note that
       LOG_FLAG_HAS_PAYLOAD must be set if this flag is present,
       because the payload of an external log entry stores the actual
       location of the payload.
     */
    LOG_FLAG_IS_EXT = 0x40,

    /* Indicates that this record stores points to another log block
       (via an LSN) rather than a version. Used for skip and overflow
       records.
     */
    LOG_FLAG_HAS_LSN = 0x20,
};
    

enum log_record_type : uint8_t {
    /* used when a preceding log record needed more space. Right now,
       only LOG_RELOCATE needs this.
     */
    LOG_NOP = 0x0,

    /* A record that does not correspond to any update in the system,
       but whose payload embeds extra information (comments, debug
       hints, etc.) that might be useful later. 
     */
    LOG_COMMENT = LOG_NOP | LOG_FLAG_HAS_PAYLOAD, 

    /* Insert a new record, with the version either embedded directly
       or stored in a separate "external" block. The external block is
       a log block that precedes this one, thus ensuring that the
       payload becomes persistent before the log record does.
     */
    LOG_INSERT = LOG_FLAG_HAS_PAYLOAD | 0x1,
    LOG_INSERT_EXT = LOG_FLAG_IS_EXT | LOG_INSERT,
    LOG_INSERT_INDEX = LOG_FLAG_HAS_PAYLOAD | 0x2,

    /* Update a record. Version may be embedded or external. */
    LOG_UPDATE = LOG_FLAG_HAS_PAYLOAD | 0x3,
    LOG_UPDATE_EXT = LOG_FLAG_IS_EXT | LOG_UPDATE,

    /* Delete a record. No payload */
    LOG_DELETE = 0x4,

    /* "Enhanced" delete record that contains a pointer to the overwritten
     * version. Has "payload", essentially an update
     */
    LOG_ENHANCED_DELETE = LOG_DELETE | LOG_FLAG_HAS_PAYLOAD,

    /* Record the relocation of a record's contents to somewhere other
       than the log. The data must be persistent before logging the
       change (no further checks or delays will be made). Used by
       system threads to move cold records to heap storage.
     */
    LOG_RELOCATE = LOG_FLAG_HAS_PAYLOAD | LOG_FLAG_IS_EXT | 0x5,

    /* Identify an earlier log block in this transaction's block
       chain, which must be processed before this one. Optional, but
       always the first record in a log block if present.
     */
    LOG_OVERFLOW = LOG_FLAG_HAS_LSN | 0x6,
    
    /* Skip to the next log block. Always (and only) used by the last
       record in a log block. 
    */
    LOG_SKIP = LOG_FLAG_HAS_LSN | 0x7,

    /* Sometimes we need to store a chunk of data in the log that is
       not a normal log block. To avoid confusing log scanners,
       disguise the data as a skip record that has a payload.
     */
    LOG_FAT_SKIP = LOG_SKIP | LOG_FLAG_HAS_PAYLOAD, 

    /* A key for an update.
     */
    LOG_UPDATE_KEY = LOG_FLAG_HAS_PAYLOAD | 0x8,

    /* Records the creation of an FID with a given table name
     */
    LOG_FID = LOG_FLAG_HAS_PAYLOAD | 0x9,
};

// log records are 16B sans payload
struct LOG_ALIGN log_record {
    /* What kind of log record is this, anyway? */
    log_record_type type;

    /* What size code to embed in the fat_ptr? */
    uint8_t size_code;

    /* Want to be able to scan the log without referencing metadata.

       NOTE: for external payloads, size_code and size_align_bits
       refer to the external object's size, not the size of the
       pointer that's actually stored in the log (the latter is fixed
       and known at compile time, so we don't need to store it).
     */
    int16_t size_align_bits;

    /* Where does this record's payload end? Payloads are packed with
       no gaps, so the front of the payload is simply the end of
       whatever came before it.
     */
    uint32_t payload_end;

    /* Type-dependent stuff */
    union {
        // skip
        LSN next_lsn;
        
        // overflow
        LSN prev_lsn;
        
        // insert, update, delete, relocate
        struct {
            FID fid;
            OID oid;
        };
    };

    // only useful for the last record in the block
    char data[];
};

static_assert(sizeof(log_record) == 16, "log_record must be 16 bytes");


/* All log writes occur in blocks, with each block containing one or
   more log records. 

   When writing a log block to disk, either the whole block is written
   successfully, or the whole block is rejected. There's no point
   going finer-grained, because we can't accept a "committed"
   transaction unless all of its log records are present and usable.

   We have two mechanisms for detecting log blocks that are truncated
   or otherwise corrupt. First, the checksum will (with high
   probability) be correct only if there is actually a log block
   here. Second, each log block includes its own LSN, and that must
   match the LSN the log scanner thought it was fetching. This
   protects against reading a "valid"---but long-dead---log block from
   a recycled file that was not overwritten properly, even if the old
   block happened to land at exactly the same starting offset in the
   file; unlike files, LSN are never reused.
*/

// log block header is 16B (sans records and payload)
struct LOG_ALIGN log_block {
    /* The checksum covers everything from the start of the log block
       (excluding itself), through the end of the payload area; bytes
       in the "dead" space between the end of the payload area and the
       start of the next log block are unprotected.

       Dead space may be unused (e.g. a transaction that failed
       pre-commit) or could be an overflow block whose contents will
       be checked separately if/when the block is accessed. If an
       overflow block is corrupt, deferring the check means we can
       truncate the log at the committing transaction, thus reducing
       the amount of work lost after a crash or media failure.
     */
    uint32_t checksum;

    /* How many log records in this block?

       There is always a skiplog at records[nrec], which tells
       recovery where to go next. In addition, the payload area for
       this block starts at records[nrec].data and ends at
       records[nrec].data + records[nrec].payload_end.
     */
    uint32_t nrec;

    /* The LSN we think we are. If the checksum is valid, but this
       doesn't match what the log scanner expected, we could have
       tripped over a (recycled) log block from the distant past that
       happens to start where the most recent valid log block ended.
     */
    LSN lsn;

    /* A variable-length array of records, plus one for the skip.
     */
    log_record records[1];

    /* Compute the size of a log block containing the specified number
       of records and aggregate payload size. Replaces sizeof().
     */
    static constexpr
    size_t size(uint32_t nrec, size_t payload_bytes) {
        return OFFSETOF(log_block, records[nrec].data[payload_bytes]);
    }

    /* Compute the size of a log block that disguises a nested log
       block as a skip record. This is used to hide external records,
       overflow blocks, and checkpoint commit records from scans.
     */
    static constexpr
    size_t wrapped_size(uint32_t nrec, size_t payload_bytes)
    {
        return size(0, size(nrec, payload_bytes));
    }
    
    char *payload_begin() {
        return records[nrec].data;
    }

    char *payload_end() {
        return records[nrec].data + records[nrec].payload_end;
    }

    // get the starting offset of the payload for record [i]
    uint32_t payload_offset(size_t i) {
        THROW_IF(not (i <= nrec), illegal_argument,
                 "Log record %zd is out of bounds\n", i);
        return i? records[i-1].payload_end : 0;
    }

    // get a pointer to the payload for record [i]
    char *payload(size_t i) {
        return payload_begin() + payload_offset(i);
    }

    size_t payload_size(size_t i) {
        THROW_IF(not (i <= nrec), illegal_argument,
                 "Log record %zd is out of bounds\n", i);
        return records[i].payload_end - payload_offset(i);
    }

    char const *checksum_begin() {
        return (char const*) &nrec;
    }
    
    /* Compute a checksum for the main part of the log block,
       excluding the payload area.
     */
    uint32_t body_checksum() {
        auto *begin = checksum_begin();
        return adler32(begin, payload_begin()-begin);
    }

    uint32_t full_checksum() {
        auto *begin = checksum_begin();
        return adler32(begin, payload_end()-begin);
    }

    /* Return the LSN that identifies the payload for record [i]. 

       The LSN points directly to the log record's payload, and uses
       the size_code recorded in the log record; once converted to a
       fat_ptr, it should be directly usable as an OID array entry.
     */
    LSN payload_lsn(size_t i) {
        /* Start with the log block's LSN, and add in the bytes
           between log block start and start of this payload.
        */
        uintptr_t offset = lsn.offset();
        offset += payload(i) - (char*) this;
        return LSN::make(offset, lsn.segment(), records[i].size_code);
    }

    LSN next_lsn() {
        return records[nrec].next_lsn;
    }

};

struct log_allocation {
    // the offset of this allocation
    uintptr_t lsn_offset;
    
    /* points to "live" buffer space.

       WARNING: this field is private to the owner of the
       log_allocation and other threads cannot safely rely on it
       having, or keeping, any particular value. 
     */
    log_block *block;
};

struct LOG_ALIGN log_request {
    /* space to hold the payload of relocate and *_ext records

       WARNING: this field must be 16B aligned; the 8B after it will
       end up as part of the log record and so must not change.
     */
    fat_ptr extra_ptr;
    
    FID fid;
    OID oid;

    log_request *next;
    
    /* Where is the payload, and how large is it? The pointer doesn't
       include the FID-specific alignment factor, so embed that as
       well to avoid trawling through metadata all the time.
     */
    fat_ptr payload_ptr;

    /* Memoize the payload_size to avoid repeated calls to
       decode_size_aligned. Also, in case of external payloads, this
       value reflects the size of the pointer that ends up in the log,
       rather than the size of the pointed-to object.
     */
    uint32_t payload_size;
    int16_t size_align_bits;
    uint8_t __padding;
    
    // log record type?
    log_record_type type;

    // where to write the record's location (once assigned)
    fat_ptr *pdest;
};

/* The smallest meaningful log block carries no payload. Like all log
   blocks, however, it ends with a skip record.
 */
static size_t const MIN_LOG_BLOCK_SIZE = log_block::size(0, 0);

/* Format a skip record.

   NOTE: Even though the skip record itself often has no payload, it
   could still have a non-zero payload_end because of records before
   it in the block. Skip records can also have payloads.
 */
static inline
void
fill_skip_record(log_record *r, LSN next_lsn, size_t payload_end, bool has_payload) {
    r->type = has_payload? LOG_FAT_SKIP : LOG_SKIP;
    r->size_code = INVALID_SIZE_CODE;
    r->size_align_bits = -1;
    r->payload_end = payload_end;
    r->next_lsn = next_lsn;
}

#endif
