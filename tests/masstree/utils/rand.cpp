#include "rand.h"

static std::random_device rd;
static std::mt19937 gen(rd());

std::string randString(int maxLength) {
    int str_size = std::rand() % maxLength;

    std::uniform_int_distribution<int> distribution{'0', 'z'};

    std::string rand_str(str_size, '\0');
    for(char & ch : rand_str) {
        ch = distribution(gen);
    }
    
    return rand_str;
}

