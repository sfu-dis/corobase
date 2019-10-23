#include "record.h"

#include <algorithm>
#include <random>
#include <unordered_set>

static std::default_random_engine generator;

static inline std::string randString(int len) {
    std::uniform_int_distribution<int> distribution{'0', 'z'};

    std::string rand_str(len-1, '\0');
    for (char& ch : rand_str) {
        ch = distribution(generator);
    }

    return rand_str;
}

static inline uint32_t randUint() { return static_cast<uint32_t>(std::rand()); }

static inline std::string genKeyNotInKeysSet(
        const std::unordered_set<std::string> & keys_set,
        uint32_t gen_key_len_avg) {

    // Have a lower bound of key length, so that it can easily
    // find a key that is not in the given keys set
    uint32_t actual_len_avg = std::min(128u, gen_key_len_avg);

    while (true) {
        std::string str = randString(actual_len_avg);
        if (keys_set.find(str) == keys_set.end()) {
            return str;
        }
    }
}

std::vector<Record> genRandRecords(uint32_t record_num, uint32_t key_len_avg) {
    std::normal_distribution<int> distribution(key_len_avg, 10);

    std::unordered_set<std::string> key_set;
    while(key_set.size() != record_num) {
        uint32_t key_len = distribution(generator);
        key_set.insert(randString(key_len));
    }

    std::vector<Record> records;
    records.reserve(record_num);
    for (const std::string & key : key_set) {
        records.emplace_back(key, randUint());
    }

    return records;
}

std::string genKeyNotInRecords(const std::vector<Record>& records, uint32_t key_len_avg) {
    std::unordered_set<std::string> key_set;
    for (const Record& record : records) {
        key_set.insert(record.key);
    }

    return genKeyNotInKeysSet(key_set, key_len_avg);
}

std::vector<Record> genDisjointRecords(const std::vector<Record>& ref_records,
                                       uint32_t disjoint_records_num,
                                       uint32_t key_len_avg) {
    std::unordered_set<std::string> key_set;
    for (const Record& record : ref_records) {
        key_set.insert(record.key);
    }

    std::vector<Record> disjoint;
    disjoint.reserve(disjoint_records_num);
    for(uint32_t i = 0; i < disjoint_records_num; i++) {
        disjoint.emplace_back(genKeyNotInKeysSet(key_set, key_len_avg), randUint());
    }

    return disjoint;
}

std::vector<Record> recordsSearchRange(
    const std::vector<Record>& records, const std::string& beg,
    const std::string& end) {
    // TODO
    return std::vector<Record>();
}

