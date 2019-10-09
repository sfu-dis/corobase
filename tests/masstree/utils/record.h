#include <sstream>

#include <varstr.h>
#include <masstree/str.hh>
#include <dbcore/sm-oid.h>

template<typename key_t>
struct Record {
    key_t key;
    ermia::OID value;

    ermia::varstr key_to_varstr() const {
        std::stringstream st;
        st << key;
        std::string key_str = st.str();
        return ermia::varstr(key_str.data(), key_str.size());
    }

    lcdf::Str key_to_Str() const {
        std::stringstream st;
        st << key;
        std::string key_str = st.str();
        return lcdf::Str(key_str.data(), key_str.size());
    }
};

