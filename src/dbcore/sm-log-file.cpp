#include "sm-log-file.h"

#include "rcu.h"

#include <new>
#include <sys/fcntl.h>
#include <algorithm>

using namespace RCU;

// segment, start offset, end offset
#define SEGMENT_FILE_NAME_FMT "log-%08x-%012zx-%012zx"
#define SEGMENT_FILE_NAME_BUFSZ sizeof("log-01234567-0123456789ab-0123456789ab")

// start, end LSN
#define CHKPT_FILE_NAME_FMT "chk-%016zx-%016zx"
#define CHKPT_FILE_NAME_BUFSZ sizeof("chk-0123456789abcdef-0123456789abcdef")

// LSN
#define DURABLE_FILE_NAME_FMT "dur-%016zx"
#define DURABLE_FILE_NAME_BUFSZ sizeof("dur-0123456789abcdef")

// segment
#define NXT_SEG_FILE_NAME_FMT "nxt-%08x"
#define NXT_SEG_FILE_NAME_BUFSZ sizeof("nxt-01234567")

#warning Crash durability is NOT fully guaranteed by this implementation
/* ^^^

   I know that sounds bad, but it turns out this is a fiendishly
   difficult thing to make bulletproof. The reasons are twofold:

   1. POSIX makes no guarantees that fsync actually does anything, and
      in fact explicitly allows a no-op. Mac OS X is a known
      offender. The implications should be obvious.

   2. POSIX makes no guarantees about when data reaches disk, in
      particular vs. the metadata that describe it. Few file systems
      specify this either (ext3 is the only one I know of, and then
      only if you mount in ordered data mode). Even if fysnc works, it
      only guarantees that certain data *have* reached disk. There's
      no way to request that certain data *not* reach disk quite yet.

   This implementation does take several measures to avoid the worst
   problems, though:

   1. The most important files are empty. Few decent filesystems will
      corrupt file names operations on crash, because metadata changes
      are journaled. We call fsync on the directory fd after all
      file-name operations.

   2. We create each new log segment as soon as the system starts
      using its predecessor. That gives a much wider time window for
      things to reach disk, in the event of a crash. Once the system
      starts using a segment, it renames the file. The file's contents
      should be unaffected by the rename, however, and we redo the
      rename during recovery if necessary.

   3. File descriptors are opened with O_SYNC, so all writes should go
      do disk immediately. There's also some evidence (on linux at
      least) that it's significantly faster than calling fsync
      separately. It's not clear whether O_SYNC is useful on systems
      where fsync is a no-op, however.

   4. When there should only be one of a file (checkpoint, durable
      mark), we rename rather than creating and deleting. POSIX
      requires the rename operation to be atomic at runtime, and a
      journalling file system makes it extremely likely that it will
      also be atomic across a crash (though the change may be lost if
      the relevant journal entries weren't durable yet).
 */

/*
   Segments need to be large enough that we can actually fit a
   reasonable number of log records in them, but small enough that we
   can test them easily.

   Enforce the segment alignment given here, by fiat.
*/
static size_t const LOG_SEGMENT_ALIGN = 1024;

struct segment_file_name {
    char buf[SEGMENT_FILE_NAME_BUFSZ];
    segment_file_name(sm_log_file_mgr::segment_id *sid)
        : segment_file_name(sid->segnum, sid->start_offset, sid->end_offset)
    {
    }
    segment_file_name(uint32_t segnum, uint64_t start, uint64_t end) {
        size_t n = os_snprintf(buf, sizeof(buf),
                               SEGMENT_FILE_NAME_FMT, segnum, start, end);
        ASSERT(n < sizeof(buf));
    }
    operator char const *() { return buf; }
    char const *operator*() { return buf; }
};

struct cmark_file_name {
    char buf[CHKPT_FILE_NAME_BUFSZ];
    cmark_file_name(LSN start, LSN end) {
        size_t n = os_snprintf(buf, sizeof(buf),
                               CHKPT_FILE_NAME_FMT, start._val, end._val);
        ASSERT(n < sizeof(buf));
    }
    operator char const *() { return buf; }
    char const *operator*() { return buf; }
};

struct dmark_file_name {
    char buf[CHKPT_FILE_NAME_BUFSZ];
    dmark_file_name(LSN start) {
        size_t n = os_snprintf(buf, sizeof(buf),
                               DURABLE_FILE_NAME_FMT, start._val);
        ASSERT(n < sizeof(buf));
    }
    operator char const *() { return buf; }
    char const *operator*() { return buf; }
};

struct nxt_seg_file_name {
    char buf[NXT_SEG_FILE_NAME_BUFSZ];
    nxt_seg_file_name(uint32_t segnum) {
        size_t n = os_snprintf(buf, sizeof(buf),
                               NXT_SEG_FILE_NAME_FMT, segnum);
        ASSERT(n < sizeof(buf));
    }
    operator char const *() { return buf; }
    char const *operator*() { return buf; }
};


sm_log_file_mgr::segment_id *
sm_log_file_mgr::_oldest_segment() {
    return segments[volatile_read(oldest_segnum)];
}

sm_log_file_mgr::segment_id *
sm_log_file_mgr::_newest_segment() {
    return volatile_read(active_segment);
}

void
sm_log_file_mgr::_pop_oldest() {
    ASSERT(oldest_segnum < _newest_segment()->segnum);
    auto *sid = _oldest_segment();
    os_close(sid->fd);
    sid->fd = -1;
    rcu_free(sid);
    segments[oldest_segnum] = NULL;
    oldest_segnum++;
}

void
sm_log_file_mgr::_pop_newest() {
    ASSERT(oldest_segnum < _newest_segment()->segnum);
    segment_id *sid = _newest_segment();
    segments[sid->segnum] = NULL;
    active_segment = segments[sid->segnum-1];
    os_close(sid->fd);
    sid->fd = -1;
    rcu_free(sid);
}

sm_log_file_mgr::segment_array::segment_array() {
    segment_id *sd = NULL;
    std::uninitialized_fill(arr, arr+NUM_LOG_SEGMENTS, sd);
}

sm_log_file_mgr::segment_array::~segment_array()
{
    /* Free segment ids and close all file handles. Do *not* delete
       the files. Technically, we should wait to close files until RCU
       has reclaimed the sid---otherwise, the system might recycle the
       just-closed fd and a straggler who intends to access the
       original file will get the new one instead. However, we won't
       worry about it because any straggler that tries to access the
       old file after it has been discarded is likely to break things
       regardless of which file it accesses in the end!
    */
    for (auto &sid : arr) {
        if (sid) {
            os_close(sid->fd);
            sid->fd = -1;
            rcu_free(sid);
            sid = NULL;
        }
    }
}

/* The log consists of 18 files:

   Sixteen log segment files, of the form log-$SEGNO-$BEGIN-$END. Each
   segment's name encodes the segment number, as well as the range of
   LSN offsets that map to it (some LSN offsets do not map to any
   segment). The file contains up to $END-$BEGIN bytes of data.
   
   One durable marker, an emtpy file named durable-$LSN. The writer
   periodically renames this file to reflect the latest known-durable
   LSN. The system can start verifying checksums at this point when
   searching for end-of-log (the file system is responsible for
   dealing with media errors).
   
   One checkpoint marker, an empty file named
   checkpoint-$BEGIN-$END. Whenever a new checkpoint is confirmed to
   be durable, the checkpoint code renames this file to point to
   it. The actual checkpoint data reside at $END, but recovery starts
   from $BEGIN.
   
   A checkpoint is just a special transaction:

   1. Consisting of "updates" to all OID tables and allocators in the
   system, such that every change preceding the start LSN is accounted
   for (changes after may or may not be accounted for).

   2. Whose commit block is wrapped in a "skip" record (similar to an
   overflow block) and invisible to a normal log scan.

   3. Identified by the checkpoint marker file that tells the system
   where to find the checkpoint transaction, and also identifies the
   LSN from which recovery should begin.

   Log bootstrap does three things:

   1. Verify that directory [dname] contains (only) valid log files
   2. Find the most recent log checkpoint from which to begin recovery
   3. Find the end of the log, truncating torn records if necessary

   If [dname] is empty, create a new (empty) log there. Otherwise, use
   on-disk data to initialize in-memory log structures. Either way,
   return the checkpoint and start LSN to the caller so they can begin
   recovery.
   
 */

/* Create a new log in directory [dir_name]. The caller must ensure
   that the directory exists and is empty.
 */
void sm_log_file_mgr::_make_new_log() {
    // create and open the first segment file
    uint32_t segnum = oldest_segnum = 1;
    nxt_segment_fd = 0;
    _create_nxt_seg_file(true);
    auto *sid = active_segment = _prepare_new_segment(segnum, 0, 0);

    /* Prime the segment with an empty checkpoint

       All checkpoints are nested inside the payload of a skip block;
       in our case the checkpoint itself is *also* a skip block.
     */
    union LOG_ALIGN {
        char buf[log_block::size(0, MIN_LOG_BLOCK_SIZE)];
        log_block b;
    };

    auto fill_skip_block = [](log_block *b, LSN lsn, size_t payload_size) {
        b->nrec = 0;
        b->lsn = lsn;
        size_t blocksz = log_block::size(0, payload_size);
        LSN next_lsn = lsn.advance_within_segment(blocksz);
        fill_skip_record(b->records, next_lsn, payload_size, payload_size);
    };

    // fill both blocks
    _chkpt_start_lsn = sid->make_lsn(0);
    fill_skip_block(&b, _chkpt_start_lsn, MIN_LOG_BLOCK_SIZE);
    
    log_block *b2 = (log_block*) b.payload(0);
    _chkpt_end_lsn = b.payload_lsn(0);
    fill_skip_block(b2, _chkpt_end_lsn, 0);

    // compute inner checksum first (outer depends on it)
    b2->checksum = b2->full_checksum();
    b.checksum = b.full_checksum();

    // write out the block
    int fd = open_for_write(sid);
    DEFER(os_close(fd));
    os_pwrite(fd, buf, sizeof(buf), 0);
    _durable_lsn = b.next_lsn();

    // create the checkpoint and durable mark files
    os_truncateat(dfd, cmark_file_name(_chkpt_start_lsn, _chkpt_end_lsn));
    os_truncateat(dfd, dmark_file_name(_durable_lsn));
    os_fsync(dfd);
}

sm_log_file_mgr::sm_log_file_mgr(char const *dname, size_t ssize)
{
    set_segment_size(ssize);

    /* The code below does not close open segment file descriptors if
       anything goes wrong. There is no meaningful way to recover from
       a log bootstrap failure, so we would expect execution to
       terminate soon anyway should such a failure occur.

       There are also some log_segment_desc leaks.
     */
    bool durable_found = false;
    bool chkpt_found = false;
    bool nxt_seg_found = false;

    std::vector<segment_id*> tmp;
    dirent_iterator dir(dname);
    dfd = dir.dup();
    for (char const *fname : dir) {
        switch(fname[0]) {
        case '.': {
            // skip files matching .*
            continue;
        }
        case 'c': {
            // allowed: one checkpoint marker
            char canary;
            int n = sscanf(fname, CHKPT_FILE_NAME_FMT "%c",
                           &_chkpt_start_lsn._val, &_chkpt_end_lsn._val, &canary);
            if (n == 2) {
                // valid checkpoint marker file name
                THROW_IF(chkpt_found, log_file_error,
                         "Multiple log checkpoint markers found");
                THROW_IF(not (_chkpt_start_lsn < _chkpt_end_lsn), log_file_error,
                         "Corrupt checkpoint marker: %s", fname);
                THROW_IF(not is_aligned(_chkpt_start_lsn.offset()), log_file_error,
                         "Misaligned checkpoint start: %zx", size_t(_chkpt_start_lsn.offset()));
                THROW_IF(not is_aligned(_chkpt_end_lsn.offset()), log_file_error,
                         "Misaligned checkpoint end: %zx", size_t(_chkpt_end_lsn.offset()));
                chkpt_found = true;
                continue;
            }
            break;
        }
        case 'd': {
            // allowed: one durability marker
            char canary;
            int n = sscanf(fname, DURABLE_FILE_NAME_FMT "%c",
                           &_durable_lsn._val, &canary);
            if (n == 1) {
                // valid durability marker
                THROW_IF(durable_found, log_file_error,
                         "Multiple durable markers found");
                THROW_IF(not is_aligned(_durable_lsn.offset()), log_file_error,
                         "Misaligned durable marker: %zx", size_t(_durable_lsn.offset()));
                durable_found = true;
                continue;
            }
            break;
        }
        case 'l': {
            // allowed: log segment
            char canary;
            segment_id *sid = rcu_alloc();
            DEFER_UNLESS(success, rcu_free(sid));
            
            int n = sscanf(fname, SEGMENT_FILE_NAME_FMT "%c",
                   &sid->segnum, &sid->start_offset, &sid->end_offset, &canary);
            if (n == 3) {
                THROW_IF(not is_aligned(sid->start_offset), log_file_error,
                         "Misaligned start for log segment %u: %zx",
                         sid->segnum, size_t(sid->start_offset));
                size_t ssize = sid->end_offset - sid->start_offset;
                THROW_IF(not is_aligned(ssize, LOG_SEGMENT_ALIGN), log_file_error,
                         "Invalid size for log segment %u: %zx",
                         sid->segnum, ssize);

                /* During recovery/startup, segments are added to the
                   list out of order (we sort later) and with byte
                   offset zero. The latter works because exactly one
                   segment will be writable once startup completes,
                   and the possibility of a hole in the log means we
                   don't know yet which segment that will be.
                 */
                sid->byte_offset = 0;
                sid->fd = os_openat(dfd, fname, O_RDONLY);
                tmp.push_back(sid);
                success = true;
                continue;
            }
            break;
        }
        case 'n': {
            // allowed: new log segment file
            char canary;
            uint32_t segnum;
            int n = sscanf(fname, NXT_SEG_FILE_NAME_FMT "%c",
                           &segnum, &canary);
            if (n == 1) {
                THROW_IF(nxt_seg_found, log_file_error,
                         "Multiple new segments found");
                
                uint64_t fd = os_openat(dfd, fname, O_RDONLY);
                nxt_segment_fd = (fd << 32) | segnum;
                nxt_seg_found = true;
                continue;
            }
            break;
        }
        default:
            break;
        }

        // we only get here if we hit a bad file name
        throw log_file_error("Invalid log file name `%s'", fname);
    }

    // Empty/missing log?
    if (tmp.empty()) {
        THROW_IF(chkpt_found or durable_found or nxt_seg_found, log_file_error,
                 "Found checkpoint, durable marker and/or new segment file, but no log segments");
        _make_new_log();
        sm_log::need_recovery = false;
        return;
    }

    sm_log::need_recovery = true;
    
    THROW_IF(tmp.size() > NUM_LOG_SEGMENTS, log_file_error,
             "Log directory contains too many segment files: %zd",
             tmp.size());
    THROW_IF(not chkpt_found, log_file_error,
             "Found log segments, but no checkpoint marker");
    THROW_IF(not durable_found, log_file_error,
             "Found log segments, but no durable marker");

    /* Now we have a list of segments and markers. Sort the
       checkpoints into their proper order (newest first) and
       cross-check with the checkpoint and durable markers.
     */
    auto ssort = [](segment_id *a, segment_id *b) {
        THROW_IF(a->segnum == b->segnum, log_file_error,
                 "Duplicate log segment files for segment %u", a->segnum);
        return a->segnum < b->segnum;
    };
    std::sort(tmp.begin(), tmp.end(), ssort);


    auto *slo = tmp.front();
    auto *shi = tmp.back();
    THROW_IF(shi->segnum - slo->segnum != tmp.size() - 1, log_file_error,
             "Found gap(s) in the log segment file sequence");

    oldest_segnum = slo->segnum;
    active_segment = shi;
    for (auto *sid : tmp)
        segments[sid->segnum] = sid;
    
    /* Validate checkpoint and durable LSN */
    THROW_IF(_durable_lsn.offset() < slo->start_offset, log_file_error,
             "Durable marker before first log segment");
    THROW_IF(shi->end_offset < _durable_lsn.offset(), log_file_error,
             "Durable marker after last log segment");
    THROW_IF(_chkpt_start_lsn.offset() < slo->start_offset, log_file_error,
             "Checkpoint before first log segment");
    THROW_IF(shi->end_offset < _chkpt_end_lsn.offset(), log_file_error,
             "Checkpoint end after last log segment");

    if (nxt_seg_found) {
        /* Make sure the segment numbers agree */
        THROW_IF(uint32_t(nxt_segment_fd) != shi->segnum+1, log_file_error,
                 "Wrong segment number for new segment file: %u (should be %u)",
                 uint32_t(nxt_segment_fd), shi->segnum+1);
    }
    else {
        /* Crash must have happened between opening of one segment and
           creation of the new file for the next. Schedule its creation.
         */
        nxt_segment_fd = shi->segnum;
    }
}

/* Set the segment size, forcing to the log segment's minimum alignment

 */
void
sm_log_file_mgr::set_segment_size(size_t ssize) {
    ASSERT(ssize > 0);
    segment_size = align_up(ssize, LOG_SEGMENT_ALIGN);
}

/* We try to keep a segment file ready to go at all times in order to
   allow a latch-free segment change (the fd is already open and
   usable by the time the atomic CAS completes). However, once a new
   segment file has actually been activated, we need to rename it
   fairly quickly to follow the normal log segment naming
   convention... and create a new segment file for the next segment so
   the cycle can repeat.
 */
void
sm_log_file_mgr::_create_nxt_seg_file(bool force)
{
    uint32_t segnum = uint32_t(nxt_segment_fd)+1;
    bool doit = force;
    if (not doit) {
        segment_id *sid = _newest_segment();
        if (uint32_t(nxt_segment_fd) == sid->segnum) {
            ASSERT(segnum == sid->segnum+1);
            ASSERT(not segments[sid->segnum]);
            segments[sid->segnum] = sid;
            
            nxt_seg_file_name oldname(sid->segnum);
            segment_file_name newname(sid);
            os_renameat(dfd, oldname, dfd, newname);
            os_fsync(dfd);
            doit = true;
        }
    }
    if (doit) {
        nxt_seg_file_name sname(segnum);
        uint64_t fd = os_openat(dfd, sname, O_CREAT|O_EXCL|O_RDONLY);
        nxt_segment_fd = (fd << 32) | segnum;
    }
}

void
sm_log_file_mgr::update_durable_mark(LSN dlsn) {
    file_mutex.lock();
    DEFER(file_mutex.unlock());
    _create_nxt_seg_file(false);

    ASSERT(_durable_lsn < dlsn);
    ASSERT(_oldest_segment()->start_offset <= dlsn.offset());
    ASSERT(dlsn.offset() < _newest_segment()->end_offset);
    
    dmark_file_name
        oldname(_durable_lsn),
        newname(dlsn);
    
    os_renameat(dfd, oldname, dfd, newname);
    os_fsync(dfd);
    _durable_lsn = dlsn;
}

void
sm_log_file_mgr::update_chkpt_mark(LSN cstart, LSN cend)
{
    file_mutex.lock();
    DEFER(file_mutex.unlock());
    _create_nxt_seg_file(false);

    ASSERT(_chkpt_end_lsn < _durable_lsn);
    ASSERT(_chkpt_end_lsn < cstart);
    ASSERT(cstart < cend);
    ASSERT(_oldest_segment()->start_offset <= cstart.offset());
    ASSERT(cend.offset() < _newest_segment()->end_offset);

    cmark_file_name
        oldname(_chkpt_start_lsn, _chkpt_end_lsn),
        newname(cstart, cend);
    
    os_renameat(dfd, oldname, dfd, newname);
    os_fsync(dfd);
    _chkpt_start_lsn = cstart;
    _chkpt_end_lsn = cend;
}

int
sm_log_file_mgr::open_for_write(segment_id *sid)
{
    file_mutex.lock();
    DEFER(file_mutex.unlock());
    _create_nxt_seg_file(false);

    segment_file_name sname(sid);
    return os_openat(dfd, sname, O_WRONLY|O_SYNC);
}

sm_log_file_mgr::segment_id*
sm_log_file_mgr::prepare_new_segment(uint64_t start)
{
    auto *psid = _newest_segment();
    if (start + MIN_LOG_BLOCK_SIZE < psid->end_offset)
        return 0;

    auto pssize = psid->end_offset - psid->start_offset;
    return _prepare_new_segment(psid->segnum+1, start, psid->byte_offset+pssize);
}

sm_log_file_mgr::segment_id*
sm_log_file_mgr::_prepare_new_segment(uint32_t segnum, uint64_t start, uint64_t byte_offset)
{
    /* In order to avoid I/O on the critical path, we rely on a daemon
       thread to have created a special "new segment" file and opened
       the appropriate file descriptors. Once a new segment is
       actually created, the daemon will rename the file to match the
       normal convention and create a "new segment" file for the next
       segment. If the new segment file is not ready yet, then we have
       to do the work manually, on the critical path.
     */
    auto *sid = segments[segnum];
    THROW_IF(sid and sid->segnum != segnum, log_is_full);
    
    auto fd_info = volatile_read(nxt_segment_fd);
    if (uint32_t(fd_info) < segnum) {
        // segment file isn't ready yet
        file_mutex.lock();
        DEFER(file_mutex.unlock());
        _create_nxt_seg_file(false);

        fd_info = volatile_read(nxt_segment_fd);
        ASSERT(segnum <= uint32_t(fd_info));
    }

    if (uint32_t(fd_info) > segnum) 
        return 0; // lost the race
        
    ASSERT(uint32_t(fd_info) == segnum);
    int fd = fd_info >> 32;
    auto end = start + volatile_read(segment_size);
    return rcu_new(fd, segnum, start, end, byte_offset);
}

bool
sm_log_file_mgr::create_segment(segment_id *sid)
{
    DEFER_UNLESS(success, rcu_free(sid));
    auto *psid = _newest_segment();
    if (sid->segnum == psid->segnum+1) {
        ASSERT(psid->end_offset <= sid->start_offset + MIN_LOG_BLOCK_SIZE);
        
        /* Looks good so far! */
        auto *nsid = __sync_val_compare_and_swap(&active_segment, psid, sid);
        if (nsid == psid) {
            success = true;
            return true;
        }
    }
    
    return false;
}

void
sm_log_file_mgr::truncate_after(uint32_t segnum, uint64_t new_end)
{
    file_mutex.lock();
    DEFER(file_mutex.unlock());
    
    THROW_IF(segnum < _oldest_segment()->segnum, illegal_argument,
             "Attempt to truncate beginning of log");

 again:
    auto *sid = _newest_segment();
    segment_file_name sname(sid);
    if (segnum < sid->segnum) {
        THROW_IF(sid->start_offset <= _chkpt_end_lsn.offset(), illegal_argument,
                 "Attempt to truncate most recent checkpoint");
        THROW_IF(sid->start_offset <= _durable_lsn.offset(), illegal_argument,
                 "Attempt to truncate durable mark");
        os_unlinkat(dfd, sname);
        _pop_newest();
        goto again;
    }

    if (uint32_t(nxt_segment_fd) > segnum+1) {
        // fun: replace those curlies with parens => compiler error
        nxt_seg_file_name sname{uint32_t(nxt_segment_fd)};
        os_unlinkat(dfd, sname);
        nxt_segment_fd = segnum;
        _create_nxt_seg_file(true);
    }
    
    THROW_IF(new_end < sid->start_offset, log_file_error,
             "Truncation offset %zd precedes start of segment %d",
             size_t(new_end), segnum);
    THROW_IF(sid->end_offset < new_end, log_file_error,
             "Truncation offset %zd past end of segment %d",
             size_t(new_end), segnum);
    os_truncateat(dfd, sname, new_end - sid->start_offset);
    os_fsync(dfd);
}

void
sm_log_file_mgr::reclaim_before(uint32_t segnum)
{
    file_mutex.lock();
    DEFER(file_mutex.unlock());

    THROW_IF(_newest_segment()->segnum < segnum, illegal_argument,
             "Attempt to reclaim end of log");

 again:
    auto *sid = _oldest_segment();
    if (sid->segnum < segnum) {
        THROW_IF(_durable_lsn.offset() < sid->end_offset, illegal_argument,
                 "Attempt to reclaim durable mark");
        THROW_IF(_chkpt_start_lsn.offset() < sid->end_offset, illegal_argument,
                 "Attempt to reclaim most recent checkpoint");
        
        segment_file_name sname(sid);
        os_unlinkat(dfd, sname);
        _pop_oldest();
        goto again;
    }

    os_fsync(dfd);
}
