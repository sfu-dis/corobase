#include "record.h"

#include <cmath>
#include <algorithm>
#include <random>
#include <sstream>
#include <unordered_set>

static std::default_random_engine generator;

static inline std::string randString(int len) {
    std::uniform_int_distribution<uint8_t> distribution{1, 255};

    std::string rand_str(len - 1, '\0');
    for (char& ch : rand_str) {
        ch = distribution(generator);
    }

    return rand_str;
}

static inline uint32_t randUint() { return static_cast<uint32_t>(std::rand()); }

static inline std::string genKeyNotInKeysSet(
        const std::unordered_set<std::string> & keys_set,
        uint32_t key_len) {

    // Have a lower bound of key length, so that it can easily
    // find a key that is not in the given keys set
    uint32_t actual_key_len = std::min(128u, key_len);

    while (true) {
        std::string str = randString(actual_key_len);
        if (keys_set.find(str) == keys_set.end()) {
            return str;
        }
    }
}

void setRandomSeed(uint32_t seed) {
    generator.seed(seed);
    std::srand(seed);
}

std::vector<Record> genSequentialRecords(uint32_t record_num, uint32_t key_len) {
    ASSERT(key_len <= 8);
    ASSERT((double)record_num < pow(2, 8));

    std::vector<Record> records;
    records.reserve(record_num);

    // reference: http://rosettacode.org/wiki/Combinations#C.2B.2B
    std::vector<bool> bitmask(key_len, true);
    bitmask.resize(127, false);
    do {
        // use bitmask to generate combinations
        std::string perm_begin;
        perm_begin.reserve(key_len);
        for (uint8_t i = 0; i < bitmask.size(); i++) {
            if (bitmask[i]) {
                perm_begin.push_back(static_cast<char>(i + 1));
            }
        }

        // make records from all permutations of a combination
        std::string cur_perm = perm_begin;
        do {
            if(records.size() >= record_num) {
                goto JUMP_OUT;
            }
            records.emplace_back(cur_perm, randUint());
        } while (std::prev_permutation(cur_perm.begin(), cur_perm.end()));

    } while (std::prev_permutation(bitmask.begin(), bitmask.end()));

JUMP_OUT:
    ASSERT(records.size() == record_num);

    for(uint32_t i = 0; i < 2; i++) {
        std::shuffle(records.begin(), records.end(), generator);
    }

    return records;
}

std::vector<Record> genRandRecords(uint32_t record_num, uint32_t key_len_avg) {
    ASSERT((double)record_num < pow(50, key_len_avg));

    constexpr float deviation = 1;
    std::normal_distribution<float> distribution(key_len_avg, deviation);

    std::unordered_set<std::string> key_set;
    while(key_set.size() != record_num) {
        int32_t key_len = static_cast<int32_t>(distribution(generator));
        if(key_len < 1) {
            continue;
        }
        key_set.insert(randString(key_len));
    }

    std::vector<Record> records;
    records.reserve(record_num);
    for (const std::string & key : key_set) {
        records.emplace_back(key, randUint());
    }

    return records;
}

std::string genKeyNotInRecords(const std::vector<Record>& records, uint32_t key_len) {
    std::unordered_set<std::string> key_set;
    for (const Record& record : records) {
        key_set.insert(record.key);
    }

    return genKeyNotInKeysSet(key_set, key_len);
}

std::vector<Record> genDisjointRecords(const std::vector<Record>& ref_records,
                                       uint32_t disjoint_records_num,
                                       uint32_t key_len) {
    std::unordered_set<std::string> key_set;
    for (const Record& record : ref_records) {
        key_set.insert(record.key);
    }

    std::vector<Record> disjoint_records;
    disjoint_records.reserve(disjoint_records_num);
    while (disjoint_records.size() < disjoint_records_num) {
        std::string new_key = genKeyNotInKeysSet(key_set, key_len);
        if (key_set.find(new_key) == key_set.end()) {
            key_set.insert(new_key);
            disjoint_records.emplace_back(new_key, randUint());
        }
    }

    return disjoint_records;
}

