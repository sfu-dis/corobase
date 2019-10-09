#pragma once

#include <string>
#include <random>

std::string randString(int maxLength=256);

inline uint32_t randUInt32() {
    return static_cast<uint32_t>(std::rand());
}
