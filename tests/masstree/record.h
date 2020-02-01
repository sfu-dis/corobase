#pragma once

#include <dbcore/sm-oid.h>
#include <string>
#include <vector>

struct Record {
    Record() : key(""), value(0) {}
    Record(const std::string & k, ermia::OID v) : key(k), value(v) {}
    std::string key;
    ermia::OID value;
};

// Without setting seed, the default seed is used in all Records generation.
// Sometimes you may want to use the default seed, for example, in small
// perf tests.
void setRandomSeed(uint32_t seed);

std::vector<Record> genSequentialRecords(uint32_t record_num, uint32_t key_len);

std::vector<Record> genRandRecords(uint32_t record_num,
                                   uint32_t key_len_avg=128);

std::vector<Record> genDisjointRecords(const std::vector<Record>& ref_records,
                                       uint32_t record_num,
                                       uint32_t key_len=128);

std::string genKeyNotInRecords(const std::vector<Record>& records,
                               uint32_t key_len=128);

