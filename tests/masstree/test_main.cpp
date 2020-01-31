#include <gtest/gtest.h>

#include <dbcore/sm-alloc.h>
#include <dbcore/sm-config.h>
#include <dbcore/sm-thread.h>

int main(int argc, char **argv) {
    ermia::config::threads = 40;

    ermia::thread::Initialize();
    ermia::config::init();
    ermia::MM::prepare_node_memory();

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

