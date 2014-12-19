#ifdef TRACE_FOOTPRINT
#include <fcntl.h>
#include <unistd.h>
#include <mutex>
#include <string.h>
#include "sm-trace.h"
#include "sm-common.h"

namespace TRACER {
bool loading = true;
std::mutex trace_mutex;
std::map<uintptr_t, std::string> oid_arrays;    // map oid array to table name
std::string trace_filename;
int trace_fd;
enum { TLS_BUFSIZE=128 };
static __thread char tls_record_buffer[TLS_BUFSIZE];

struct trace_record_header {
    uint64_t xid;            // identifies the tx
    char op;            // r(ead), u(pdate), or i(nsert)
    uint64_t oid_array; // ptr to oid array, need a mapping table from oid_array to table name
    uint32_t oid;
};

// each access forms a record below
struct read_trace_record {
    uint32_t size;      // data size (excluing dbtuple header)
    uint64_t xid_age;   // reader's xc->begin
    uint64_t version_age;   // age of version, actually LSN._val
    uint32_t pos;       // relative position in the chain, 0 being the head
    uint64_t version;
};

struct update_trace_record {
    uint32_t old_size;
    uint32_t new_size;
    uint64_t old_age;
    uint64_t new_age;
    uint64_t old_version;
    uint64_t new_version;
};

struct insert_trace_record {
    uint32_t size;
    uint64_t version;
};

void
start()
{
    loading = false;
    __sync_synchronize();
}

void
init()
{
    // FIXME: also use tls write_fd (i.e., multiple trace files)
    trace_filename = std::string("/tmpfs/tzwang/ermia-profile/trace-") + std::to_string(time(0));
    printf("trace file: %s", trace_filename.c_str());
    trace_fd = open(trace_filename.c_str(), O_CREAT|O_WRONLY, S_IRUSR|S_IWUSR);
    // write oid array - table name mappings
    uint64_t size = oid_arrays.size();
    DIE_IF(not write(trace_fd, (char *)&size, sizeof(uint64_t)), "");
    for (auto &m : oid_arrays) {
        std::string array = std::to_string(m.first);
        DIE_IF(not write(trace_fd, array.c_str(), array.size()), "");
        DIE_IF(not write(trace_fd, " ", 1), "");
        DIE_IF(not write(trace_fd, m.second.c_str(), m.second.size()), "");
        DIE_IF(not write(trace_fd, "\n", 1), "");
    }
}

void
write_trace(size_t len)
{
    static int tls_fd;
    if (not tls_fd) {
        tls_fd = open(trace_filename.c_str(), O_CREAT|O_WRONLY, S_IRUSR|S_IWUSR);
    }
    trace_mutex.lock();
    DIE_IF(not write(trace_fd, tls_record_buffer, len), "trace write failed\n");
    trace_mutex.unlock();
}

void
fill_header(uint64_t xid, char op, uintptr_t oid_array, uint32_t oid)
{
    memset(&tls_record_buffer, '\0', TLS_BUFSIZE);
    
    trace_record_header *hdr = (trace_record_header *)tls_record_buffer;
    hdr->xid = xid;
    hdr->op = op;
    hdr->oid_array = (uint64_t)oid_array;
    hdr->oid = oid;
}

// for reads
void
record(uint64_t xid, char op, uintptr_t oid_array, uint32_t oid,
       uintptr_t version, uint32_t size, uint32_t pos,
       uint64_t version_age, uint64_t xid_age)
{
    if (loading)
        return;
    fill_header(xid, op, oid_array, oid);
    write_trace(trace_read(version, size, pos, version_age, xid_age));
}

void
record(uint64_t xid, char op, uintptr_t oid_array, uint32_t oid,
       uintptr_t version, uint32_t size)
{
    if (loading)
        return;
    fill_header(xid, op, oid_array, oid);
    write_trace(trace_insert(version, size));
}

void
record(uint64_t xid, char op, uintptr_t oid_array, uint32_t oid,
       uintptr_t old_version, uintptr_t new_version,
       uint64_t old_size, uint64_t new_size,
       uint64_t old_age, uint64_t new_age)
{
    if (loading)
        return;
    fill_header(xid, op, oid_array, oid);
    write_trace(trace_update(old_version, new_version,
                             old_size, new_size,
                             old_age, new_age));
}

size_t
trace_read(uintptr_t version, uint32_t size, uint32_t pos,
           uint64_t version_age, uint64_t xid_age)
{
    read_trace_record *rr = (read_trace_record *)(tls_record_buffer + sizeof(trace_record_header));
    rr->version = (uint64_t)version;
    rr->size = size;
    rr->version_age = version_age;
    rr->xid_age = xid_age;
    rr->pos = pos;
    rr->version = version;
    return sizeof(read_trace_record) + sizeof(trace_record_header);
}

size_t
trace_insert(uintptr_t version, uint32_t size)
{
    insert_trace_record *ir = (insert_trace_record *)(tls_record_buffer + sizeof(trace_record_header));
    ir->version = (uint64_t)version;
    ir->size = size;
    return sizeof(insert_trace_record) + sizeof(trace_record_header);
}

size_t
trace_update(uintptr_t old_version, uintptr_t new_version,
             uint32_t old_size, uint32_t new_size,
             uint64_t old_age, uint64_t new_age)
{
    update_trace_record *ur = (update_trace_record *)(tls_record_buffer + sizeof(trace_record_header));
    ur->old_version = old_version;
    ur->new_version = new_version;
    ur->old_size = old_size;
    ur->new_size = new_size;
    ur->old_age = old_age;
    ur->new_age = new_age;
    return sizeof(update_trace_record) + sizeof(trace_record_header);
}

void
register_table(uintptr_t array, std::string name)
{
    oid_arrays[array] = name;
}

};  // end of namespace
#endif
