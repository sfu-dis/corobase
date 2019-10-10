#pragma once

#include <sstream>

#include <masstree/str.hh>
#include <dbcore/sm-oid.h>

template<typename key_t>
struct Record {
    key_t key;
    ermia::OID value;

    std::string key_to_str() const {
        std::stringstream st;
        st << key;
        return st.str();
    }
};

