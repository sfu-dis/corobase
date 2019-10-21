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

std::vector<Record> genRandRecords(uint32_t record_num,
                                   uint32_t key_len_avg=128);

std::vector<Record> genDisjointRecords(const std::vector<Record>& ref_records,
                                       uint32_t record_num,
                                       uint32_t key_len_avg=128);

std::string genKeyNotInRecords(const std::vector<Record>& records,
                               uint32_t key_len_avg=128);

std::vector<Record> recordsSearchRange(const std::vector<Record>& records,
                                       const std::string& beg,
                                       const std::string& end);

