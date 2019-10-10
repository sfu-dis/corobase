#include "record.h"

#include <algorithm>
#include <random>
#include <unordered_set>

static std::default_random_engine generator;

static std::string randString(int len) {
    std::uniform_int_distribution<int> distribution{'0', 'z'};

    std::string rand_str(len, '\0');
    for (char& ch : rand_str) {
        ch = distribution(generator);
    }

    return rand_str;
}

static uint32_t randUint() { return static_cast<uint32_t>(std::rand()); }

std::vector<Record> genRandRecords(uint32_t record_num, uint32_t key_len_avg) {
    std::normal_distribution<int> distribution(key_len_avg, 10);

    std::vector<Record> records;
    records.resize(record_num);
    for (Record& record : records) {
        uint32_t key_len = distribution(generator);
        record = Record{randString(key_len), randUint()};
    }

    return records;
}

std::string genKeyNotInRecords(const std::vector<Record>& records) {
    std::unordered_set<std::string> key_set;
    for (const Record& record : records) {
        key_set.insert(record.key);
    }

    while (true) {
        std::string str = randString(128);
        if (key_set.find(str) == key_set.end()) {
            return str;
        }
    }
}

std::vector<Record> recordsSearchRange(
    const std::vector<Record>& records, const std::string& beg,
    const std::string& end) {
    // TODO
    return std::vector<Record>();
}

