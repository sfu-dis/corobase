#include "sm-log.h"
#include "w_rand.h"
#include "stopwatch.h"

#include <cstring>
#include <map>
#include <set>
#include <unordered_set>

#include <unistd.h>
#include <fcntl.h>

using namespace RCU;

struct db_record {
    FID fid;
    OID oid;
    uint64_t vnum;
    size_t payload_size;
    char data[];

    size_t size() { return sizeof(*this) + payload_size; }
};

#define PAYLOAD_FMT " f:0x%08x o:0x%08x v:0x%016zx s:0x%08zx"
static size_t const MIN_PAYLOAD_SIZE = sizeof(" f:0x01234567 o:0x01234567 v:0x0123456789abcdef s:0x01234567");

typedef std::pair<db_record*, uint8_t> NewRecord;

static
NewRecord
create_record(FID f, OID o, uint64_t vnum, size_t payload_size)
{
    if (payload_size < MIN_PAYLOAD_SIZE)
        payload_size = MIN_PAYLOAD_SIZE;

    auto record_size = align_up(sizeof(db_record) + payload_size);
    uint8_t szcode = encode_size_aligned(record_size);
    
    payload_size = record_size - sizeof(db_record);
    
    auto *r = (db_record*) rcu_alloc(record_size);
    r->fid = f;
    r->oid = o;
    r->vnum = vnum;
    r->payload_size = payload_size;
    auto n = os_snprintf(r->data, payload_size,
                         "%*s" PAYLOAD_FMT,
                         int(payload_size - MIN_PAYLOAD_SIZE), "",
                         f, o, vnum, payload_size);
    DIE_IF(n+1 != payload_size,
           "Wrong payload size: %zd bytes needed (%zd available)",
           n+1, payload_size);
    
    return std::make_pair(r, szcode);
}

static
void
verify_record(db_record *r, bool verbose)
{
    DIE_IF(r->data[r->payload_size-1], "Payload is not NUL-terminated");
    
    if (verbose) {
        char *str = r->data;
        while (*str == ' ')
            ++str;
        
        fprintf(stderr, "Payload: %s (preceded by %zd spaces)\n", str, str-r->data);
    }
    
    size_t nbytes = std::strlen(r->data)+1;
    DIE_IF(nbytes != r->payload_size,
           "Payload size wrong. Expected %zd, got %zd",
           r->payload_size, nbytes);
    
    FID f;
    OID o;
    uint64_t vnum;
    size_t psz;
    char canary;
    int n = std::sscanf(r->data, PAYLOAD_FMT "%c",
                      &f, &o, &vnum, &psz, &canary);
    DIE_IF(n != 4, "Wrong number of entries found in payload: %d", n);
    DIE_IF(f != r->fid, "Bad FID");
    DIE_IF(o != r->oid, "Bad OID");
    DIE_IF(vnum != r->vnum, "Bad vnum");
    DIE_IF(psz != r->payload_size, "Bad payload size");
}

struct db_file {
    typedef std::pair<fat_ptr, db_record*> Entry;
    std::map<OID, Entry> records;
    
    typedef decltype(records.begin()) iterator;
    
    std::set<OID> free_oids;
    
    db_file() {
        // OID 0 intentionally unused
        free_oids.insert(1);
    }

    ~db_file() {
        for (auto &entry : records) {
            rcu_free(entry.second.second);
        }
    }        
    static
    Entry
    null_entry() { return std::make_pair(NULL_PTR, nullptr); }
    
    iterator alloc_oid() {
        OID x;
        if (free_oids.empty()) {
            x = 1 + records.rbegin()->first;
        }
        else {
            auto it = free_oids.begin();
            x = *it;
            free_oids.erase(it);
        }

        auto v = null_entry();
        auto rval = records.insert(std::make_pair(x, v));
        ASSERT(rval.second); // inserted (not already present)
        return rval.first;
        
    }
    
    void free_oid(iterator it) {
        ASSERT(it != records.end());
        rcu_free(it->second.second);
        free_oids.insert(it->first);
        records.erase(it);
    }
};

void doit(w_rand &rng) {
    auto no_recover = [](void*, sm_log_scan_mgr*, LSN, LSN)->void {
        SPAM("Log recovery is a no-op here\n");
    };

    // a quick sanity test
    auto rval = create_record(0x123, 0x456, 0x789, 50);
    SPAM("Allocated size of record is %zd bytes", decode_size_aligned(rval.second));
    DIE_IF(rval.first->size() != decode_size_aligned(rval.second), "Bad size code %u", rval.second);
    verify_record(rval.first, true);
    rcu_free(rval.first);

    tmp_dir log_dir;
    SPAM("logging to %s", *log_dir);
    sm_log *lm = sm_log::new_log(log_dir, 60*1000*1000, no_recover, NULL, 64*1024*1024);

    tmp_dir data_dir;
    SPAM("data stored in %s", *data_dir);
    int dfd = dirent_iterator(data_dir).dup();
    DEFER(os_close(dfd));
    int fd = os_openat(dfd, "data.db", O_CREAT|O_EXCL|O_RDWR);
    DEFER(os_close(fd));

    static size_t const BUFSZ = 8*1024*1024;
    char *buf = new char[BUFSZ];
    DEFER(delete[] buf);
        
    uint64_t data_end = 0;
    bool heap_needs_sync = false;
    auto heap_write = [&](db_record *r)->fat_ptr {
        os_pwrite(fd, (char*) r, r->size(), data_end);
        heap_needs_sync = true;
        DEFER(data_end += r->size());
        size_t tmp = r->size();
        auto szcode = encode_size_aligned(tmp);
        ASSERT(tmp == r->size());
        return fat_ptr::make(data_end, szcode, fat_ptr::ASI_HEAP_FLAG);
    };

    auto print_payload = [](char const *label, db_record *r) {
        char *str = r->data;
        while (*str == ' ')
            ++str;
        
        fprintf(stderr, "%s: %s (preceded by %zd spaces)\n", label, str, str - r->data);
    };
    auto print_no_payload = [](char const *label, FID f, OID o, fat_ptr *where=NULL) {
        if (where)
            fprintf(stderr, "%s: F:%d O:%d P:%zd\n", label, f, o, where->_ptr);
        else
            fprintf(stderr, "%s: F:%d O:%d\n", label, f, o);
    };

    std::map<FID, db_file> files;
    typedef decltype(*files.begin()) Entry;

    // FID 0 intentionally unused
    size_t const NFILES = 4;
    for (size_t fid=1; fid <= NFILES; fid++) 
        (void) files[fid];

    auto insert_record = [&](sm_tx_log *tx, Entry &entry, size_t min_sz, size_t max_sz, bool verbose=true) {
        auto &fid = entry.first;
        auto &f = entry.second;
        auto it = f.alloc_oid();
        auto &oid = it->first;
        auto &rptr = it->second.first;
        auto &r = it->second.second;
            
        auto rval = create_record(fid, oid, 1, rng.randn(min_sz, max_sz));
        r = rval.first;
        auto rsz_code = rval.second;
        if (verbose)
            print_payload("insert", r);
        tx->log_insert(fid, oid, fat_ptr::make(r, rsz_code), DEFAULT_ALIGNMENT_BITS, &rptr);
    };
    
    auto update_record = [&](sm_tx_log *tx, Entry &entry, OID oid, size_t min_sz, size_t max_sz, bool verbose=true) {
        auto &fid = entry.first;
        auto &f = entry.second;
        auto it = oid? f.records.find(oid) : f.records.begin();
        ASSERT(it != f.records.end());
        oid = it->first;
        auto &rptr = it->second.first;
        auto &r = it->second.second;
        ASSERT(fid == r->fid);
        ASSERT(oid == r->oid);
            
        auto rval = create_record(r->fid, r->oid, r->vnum+1, rng.randn(min_sz, max_sz));
        rcu_free(r);
        r = rval.first;
        auto rsz_code = rval.second;
        if (verbose)
            print_payload("update", r);
        tx->log_update(r->fid, r->oid, fat_ptr::make(r, rsz_code), DEFAULT_ALIGNMENT_BITS, &rptr);
    };
    
    auto relocate_record = [&](sm_tx_log *tx, Entry &entry, OID oid, bool verbose=true) {
        auto &fid = entry.first;
        auto &f = entry.second;
        auto it = oid? f.records.find(oid) : f.records.begin();
        ASSERT(it != f.records.end());
        oid = it->first;
        auto &rptr = it->second.first;
        auto &r = it->second.second;
        ASSERT(fid == r->fid);
        ASSERT(oid == r->oid);
            
        rptr = heap_write(r);
        if (verbose)
            print_no_payload("relocate", fid, oid, &rptr);
        tx->log_relocate(fid, oid, rptr, DEFAULT_ALIGNMENT_BITS);
    };

    auto delete_record = [&](sm_tx_log *tx, Entry &entry, OID oid, bool verbose=true) {
        auto &fid = entry.first;
        auto &f = entry.second;
        auto it = oid? f.records.find(oid) : f.records.begin();
        ASSERT(it != f.records.end());
        oid = it->first;
        auto r = it->second.second;
        ASSERT(fid == r->fid);
        ASSERT(oid == r->oid);

        if (verbose)
            print_no_payload("delete", fid, oid);
        tx->log_delete(fid, oid);
        f.free_oid(it);
    };
    
    auto commit_and_verify_tx = [&](sm_tx_log *tx, int nrec, bool verbose=true) {
        if (heap_needs_sync) {
            fdatasync(fd);
            heap_needs_sync = false;
        }
        
        LSN blsn;
        auto lsn = tx->commit(&blsn);
        lm->wait_for_durable_lsn(lsn);
        rcu_quiesce();
        
        auto *scan = lm->get_scan_mgr()->new_tx_scan(blsn);
        DEFER(delete scan);

        int i=0;
        for (; scan->valid(); scan->next()) {
            ++i;
            
            switch(scan->type()) {
            case sm_log_scan_mgr::LOG_INSERT: 
            case sm_log_scan_mgr::LOG_UPDATE: {
                scan->load_object(buf, BUFSZ);
                auto *r = (db_record*) buf;
                ASSERT(r->size() == scan->payload_size());
                verify_record(r, verbose);
                auto &entry = files[scan->fid()].records[scan->oid()];
                ASSERT(entry.first == scan->payload_ptr());
                ASSERT(not strcmp(r->data, entry.second->data));
                break;
            }
            case sm_log_scan_mgr::LOG_RELOCATE: {
                auto ptr = scan->payload_ptr();
                ASSERT(ptr.asi_type() == fat_ptr::ASI_HEAP);
                auto nbytes = scan->payload_size();
                ASSERT(nbytes <= BUFSZ);
                size_t n = os_pread(fd, buf, nbytes, ptr.offset());
                ASSERT(n == nbytes);
                auto *r = (db_record*) buf;
                if (verbose)
                    print_no_payload("relocate", scan->fid(), scan->oid());
                verify_record(r, verbose);
                
                auto &entry = files[scan->fid()].records[scan->oid()];
                ASSERT(entry.first.offset() == ptr.offset());
                ASSERT(not strcmp(r->data, entry.second->data));
                break;
            }
            case sm_log_scan_mgr::LOG_DELETE: {
                if (verbose)
                    print_no_payload("delete", scan->fid(), scan->oid());
                break;
            }
            default:
                DIE("unreachable");
            }
        }
        ASSERT(i == nrec);
    };

    auto start_lsn = lm->durable_lsn();
    {
        /* 1. Insert one record into each table.

           This serves as a first sanity test of the log (e.g. do
           inserts work?), and partly it helps set the stage for our
           second sanity test.
         */
        sm_tx_log *tx = lm->new_tx_log();
        printf("*** TEST 1 ***\n");

        for (auto &entry : files)
            insert_record(tx, entry, 20, 200);

        commit_and_verify_tx(tx, NFILES);
    }

    {
        /* 2. Test out update, relocate, and delete.

           This checks that those operations work as expected. Do it
           now because we have enogh records that deleting one won't
           leave an empty table.
         */
        sm_tx_log *tx = lm->new_tx_log();
        printf("\n\n*** TEST 2 ***\n");

        {
            // update
            auto it = files.find(1);
            ASSERT(it != files.end());
            update_record(tx, *it, 0, 20, 200);
        }
        {
            /* delete

               Do a compensating insert first. Effect is to replace
               the deleted record with a new one having a different
               OID, so the table remains non-empty afterward.
             */
            auto it = files.find(2);
            ASSERT(it != files.end());
            insert_record(tx, *it, 20, 200);
            delete_record(tx, *it, 0);
        }
        {
            // relocate
            auto it = files.find(3);
            ASSERT(it != files.end());
            relocate_record(tx, *it, 0);
        }

        commit_and_verify_tx(tx, 4);
    }
    
    {
        /* 3. Extreme case: individual records that do not fit nicely
           in a block. System should create external insert/update
           records for them.
         */
        sm_tx_log *tx = lm->new_tx_log();
        printf("\n\n*** TEST 3 ***\n");

        for (auto &entry : files) {
            update_record(tx, entry, 0, 2*1000*1000, 8*1000*1000);
            insert_record(tx, entry, 2*1000*1000, 8*1000*1000);
        }

        commit_and_verify_tx(tx, 2*NFILES);
    }

    {
        /* 4. Extreme case: individual records fit nicely, but in
           aggregate are too large for a single log block. System
           should create overflow blocks to hold them.
         */
        sm_tx_log *tx = lm->new_tx_log();
        printf("\n\n*** TEST 4 ***\n");

        int nrec = 100;
        for (size_t i=0; i < nrec/NFILES; i++) {
            for (auto &entry : files) {
                insert_record(tx, entry, 200*1000, 800*1000);
            }
        }

        commit_and_verify_tx(tx, nrec);
    }

    {
        /* 5. Extreme case: individual records are tiny, but are too
           numerous for a single log block. System should create
           overflow blocks to hold them.
         */
        sm_tx_log *tx = lm->new_tx_log();
        printf("\n\n*** TEST 5 ***\n");

        int nrec = 1000;
        printf("creating %d records...\n", nrec); 
        for (size_t i=0; i < nrec/NFILES; i++) {
            for (auto &entry : files) {
                insert_record(tx, entry, 20, 200, false);
            }
        }

        printf("Read back and verify records\n");
        commit_and_verify_tx(tx, nrec, false);
    }

    /* 6. Make a large number of changes, then "recover" a copy of
       the database from the log. Blow up if the recovered
       database does not match the master.

       NOTE: because this test is single-versioned, Bad Things
       happen if we muddle with the same record more than once per
       transaction. To avoid this, we generate a random sample of
       FID to work with in each tx, thus avoiding duplicates.

       Going in, the "database" contains roughly 1100 records,
       evenly distributed over the four files. To keep a healthy
       mix of inserts and deletes, we'll target 400 records per
       file (1600 total).
    */
    printf("\n\n*** TEST 6 ***\n");
    auto const FSIZE = 400;
    size_t max_segnum = 2;
    printf("Running transactions until segment %zd overflows...\n", max_segnum);
    size_t count = 1;
    for (; lm->cur_lsn().segment() <= max_segnum; count++) {
        if (not (count % 100))
            printf(".\n");
            
        sm_tx_log *tx = lm->new_tx_log();
            
        auto n = rng.randn(3,20);
        n *= n; // tx size varies quadratically
        std::set<int> ids;
        while (ids.size() < n)
            ids.insert(rng.randn(FSIZE*NFILES));

        /* avoid recycling: do inserts before deletes, so we don't
           reuse the OID of a just-deleted record. Also, insert in
           sorted order. Otherwise, we might allocate a free OID
           that happens to appear later in the list, causing us to
           modify or delete it accidentally.

           WARNING: fids and oids use 1-based numbering!
        */
        for (auto i=ids.begin(); i != ids.end(); ) {
            auto fid = 1 + *i/FSIZE;
            auto oid = 1 + *i % FSIZE;
            auto it = files.find(fid);
            ASSERT(it != files.end());
            auto tmp = i++;
            if (it->second.records.find(oid) == it->second.records.end()) {
                // not there -> insert
                insert_record(tx, *it, 20, 200, false);
                ids.erase(tmp);
            }
        }
            
        /* Now do update/relocate/delete in a 2:1:1 ratio.

           Because 25% of all non-inserts are deletes, we will
           tend to stabilize at 20% insert and 20% delete in each
           tx, leaving 20% relocate and 40% update.

           WARNING: fids and oids use 1-based numbering!
        */
        for (auto i : ids) {
            auto fid = 1 + i/FSIZE;
            auto oid = 1 + i % FSIZE;
            auto it = files.find(fid);
            ASSERT(it != files.end());
            switch(rng.randn(4)) {
            case 0:
                // delete
                delete_record(tx, *it, oid, false);
                break;
            case 1:
                // relocate
                relocate_record(tx, *it, oid, false);
                break;
            default:
                // update
                update_record(tx, *it, oid, 20, 200, false);
                break;
            }
        }

        commit_and_verify_tx(tx, n, false);
    }

    auto verify = [&](sm_log_scan_mgr *scanner, LSN start) {
        printf("\nVerifying and replaying %zd transactions from the log...\n", count);
        auto *scan = scanner->new_header_scan(start);
        DEFER(delete scan);

        std::map<int,fat_ptr> recovered;
        
        count = 0;
        for (; scan->valid(); scan->next()) {
            if (not (++count % 10000))
                printf(".\n");

            int id = (scan->fid()-1)*FSIZE + (scan->oid()-1);
            auto it = recovered.find(id);
            switch (scan->type()) {
            case sm_log_scan_mgr::LOG_DELETE:
                ASSERT(it != recovered.end());
                recovered.erase(it);
                break;
            case sm_log_scan_mgr::LOG_INSERT:
                ASSERT(it == recovered.end());
                recovered[id] = scan->payload_ptr();
                break;
            case sm_log_scan_mgr::LOG_UPDATE:
            case sm_log_scan_mgr::LOG_RELOCATE:
                ASSERT(it != recovered.end());
                it->second = scan->payload_ptr();
                break;
            default:
                DIE("unreachable");
            }
        }

        printf("\nRecovered database has %zd records and %zd log entries.\n",
               recovered.size(), count);

        printf("\nVerify that all records in the original database were recovered correctly...\n");
        for (auto &fentry : files) {
            auto &fid = fentry.first;
            auto &f = fentry.second;
            for (auto &entry : f.records) {
                auto &oid = entry.first;
                auto id = (fid-1)*FSIZE + (oid-1);
                auto it = recovered.find(id);
                ASSERT(it != recovered.end());
                
                fat_ptr p;
                auto atype = it->second.asi_type();
                if (atype == fat_ptr::ASI_LOG)
                    p = it->second;
                else if (atype == fat_ptr::ASI_EXT)
                    p = scanner->load_ext_pointer(it->second);
                else
                    ASSERT(not "unreachable");
                
                ASSERT(p == entry.second.first);
            }
        }
        
        printf("\nVerify that all records in the recovered database match those in the original...\n");
        for (auto &entry : recovered) {
            auto fid = 1 + entry.first/FSIZE;
            auto oid = 1 + entry.first % FSIZE;
            auto fit = files.find(fid);
            ASSERT(fit != files.end());
            auto &f = fit->second;
            auto it = f.records.find(oid);
            ASSERT(it != f.records.end());
            
            fat_ptr p;
            auto atype = entry.second.asi_type();
            if (atype == fat_ptr::ASI_LOG)
                p = entry.second;
            else if (atype == fat_ptr::ASI_EXT)
                p = scanner->load_ext_pointer(entry.second);
            else
                ASSERT(not "unreachable");
            
            ASSERT(it->second.first == p);
        }
    };
    typedef decltype(verify) verify_fn;

    printf("\nVerify the live log\n");
    verify(lm->get_scan_mgr(), start_lsn);

    printf("\nTry to recover from a newly instantiated log\n");
    auto verify_wrapper = [](void *arg, sm_log_scan_mgr *srm, LSN chkpt_start, LSN chkpt_end) {
        verify_fn *fn = (verify_fn*) arg;
        (*fn)(srm, chkpt_start);
    };
    
    delete lm;
    lm = NULL;
    lm = sm_log::new_log(log_dir, 60*1000*1000, verify_wrapper, &verify, 64*1024*1024);

    printf("\n\n*** All tests finished ***\n");
}

int main() {
    rcu_register();
    rcu_enter();
    DEFER(rcu_exit());

    uint64_t __attribute__((unused)) now = stopwatch_t::now();
    //uint32_t seed[] = {uint32_t(getpid()), uint32_t(now), uint32_t(now>>32)};
    uint32_t seed[] = {0x00002ee8, 0x9714d618, 0x13839593};
    w_rand rng(seed);
    fprintf(stderr, "RNG seed: {0x%08x, 0x%08x, 0x%08x}\n", seed[0], seed[1], seed[2]);
    
    try {
        doit(rng);
    }
    catch (os_error &err) {
        DIE("Yikes! Caught OS error %d: %s", err.err, err.msg);
    }
    catch (log_file_error &err) {
        DIE("Yikes! Log file error: %s", err.msg);
    }
    catch (illegal_argument &err) {
        DIE("Yikes! Illegal argument: %s", err.msg);
    }
    catch (log_is_full &err) {
        DIE("Yikes! Log is full");
    }
    
}
