#pragma once
#include <map>

namespace TRACER {
extern std::map<uintptr_t, std::string> oid_arrays;    // map oid array to table name
void init();
void register_table(uintptr_t array, std::string name);
void start();
void write_trace(size_t len);
void fill_header(uint64_t xid, char op, uintptr_t oid_array, uint32_t oid);

void record(uint64_t xid, char op, uintptr_t oid_array, uint32_t oid,
            uintptr_t version, uint32_t size, uint32_t pos,
            uint64_t version_age, uint64_t xid_age);
void record(uint64_t xid, char op, uintptr_t oid_array, uint32_t oid,
            uintptr_t version, uint32_t size);
void record(uint64_t xid, char op, uintptr_t oid_array, uint32_t oid,
            uintptr_t old_version, uintptr_t new_version,
            uint64_t old_size, uint64_t new_size,
            uint64_t old_age, uint64_t new_age);
size_t trace_read(uintptr_t version, uint32_t size, uint32_t pos,
                  uint64_t version_age, uint64_t xid_age);
size_t trace_insert(uintptr_t version, uint32_t size);
size_t trace_update(uintptr_t old_version, uintptr_t new_version,
                    uint32_t old_size, uint32_t new_size,
                    uint64_t old_age, uint64_t new_age);
};  // end of namespace
