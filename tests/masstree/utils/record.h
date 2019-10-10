#pragma once

#include <dbcore/sm-oid.h>
#include <string>
#include <vector>

struct Record {
    std::string key;
    ermia::OID value;
};

std::vector<Record> genRandRecords(uint32_t record_num, uint32_t key_len_avg);

std::string genKeyNotInRecords(const std::vector<Record>& records);

std::vector<Record> recordsSearchRange(const std::vector<Record>& records,
                                       const std::string& beg,
                                       const std::string& end);

