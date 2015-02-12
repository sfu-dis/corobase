ERMIA
=====

Fast and Robust OLTP using Epoch-based Resource Management and Indirection Array

### Choosing CC schemes

Currently we support RC, SI, SSI, and SI/RC + SSN (with/without read optimization)
Switches are defined in src/macros.h:

1. #define USE_PARALLEL_SSI -> use SSI
2. #define USE_PARALLEL_SSN -> use SSN
   * Extra options for SSN:
     (1) #define DO_EARLY_SSN_CHECKS -> check for SSN window exclusions at normal r/w
     (2) In txn_impl.h: OLD_VERSION_THRESHOLD determines the threshold for "old" versions.
         Setting it to 0 means treat every version as "old", and skip tracking read.
3. #define USE_READ_COMMITTED -> use RC
4. undef USE_PARALLEL_SSI and USE_PARALLEL_SSN -> vanilla SI

Notes: USE_PARALLEL_SSI and USE_PARALLEL_SSN should be mutually exclusive. Defining both
is undefined.

Memory allocation settings
--------------------------

1. Prefault size
   Defined in src/benchmarks/bench.h, heap_prefault() function.
   The default value is 40GB, to have a 20GB prefaulted area:
       uint64_t FAULT_SIZE = (((uint64_t)1<<30)*40);
   Note this is only available with tcmalloc.

2. mlock limits
   Add the following to /etc/security/limits.conf:
   [username] soft memlock unlimited
   [username] hard memlock unlimited
   * Re-login to apply.

3. Environment variables needed by tcmalloc:
   TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES="2147483648"
   LD_PRELOAD="/usr/lib/libtcmalloc.so"

Building
--------

Use src/build.sh to compile Ermia.
For performance runs, use: $ build.sh
For debug runs, use: $ build.sh 1

The '1' argument will enable DEBUG and CHECK_INVARIANTS options. 

Running
-------

Useful options:
1. --bench [benchmark], e.g., --bench tpcc for running TPC-C.
2. --scale-factor [x], specify scaling factor.
3. --num-threads [x], specify number of worker threads.
4. --logdir [d], specify directory to store log files. Put d on tmpfs to avoid I/O bottleneck.
5. --runtime [t], run benchmark for t seconds.
6. --pin-cpu, pin worker threads on cores

7. TPC-C specific options: specify with -o "[options]" (with double quotes)
   (1) --warehouse-spread=[s], chance of choosing a random warehouse = s%, 0 <= s <= 100
   (2) --80-20-dist, focus 80% of accesses on 20% of warehouses; ignores --warehouse-spread
   (3) --workload-mix="[% of NewOrder,Payment,CreditCheck,Delivery,OrderStatus,Microbenchmark,Microbenchmark-simple,Microbenchmark-random]" (with double quotes).
       * TPC-C and TPC-C++: the "microbenchmark*" percents should be 0.
         For example, to run standard TPC-C mix, give:
           --workload-mix="45,43,0,4,4,4,0,0,0".
         To run TPC-C++:
           --workload-mix="43,41,4,4,4,4,0,0,0".
           Note this is the default mix if no --workload-mix option is given.

8. Microbenchmark specific options: specify with -o "[options]" and --bench tpcc
   (1) --microbench-random and --microbench-static
       There are two types of microbenchmakrs: random and static. microbench-random
       will choose random read ranges while static will read a predefined range.
   (2) --microbench-rows=[n], read n rows in each microbenchmark transaction.
   (3) --microbench-wr-rows=[m], write m rows in each microbenchmark transaction.

Examples:
   * Run TPC-C++ with SF=4, 4 threads for 30 seconds and store log files under /tmpfs/logs:
       --bench tpcc --scale-factor 4 --num-threads 4 --runtime 30 \
       --logdir /tmpfs/logs --pin-cpu \
       -o "--warehouse-spread=100 --workload-mix="43,41,4,4,4,4,0,0,0""
   * Run a microbenchmark which reads 10000 rows and writes 10 rows per transaction,
     with 4 threads for 30 seconds:
       --bench tpcc --scale-factor 32 --num-threads 4 --runtime 30 \
       --logdir /tmpfs/logs --pin-cpu \
       -o "--microbench-random --microbench-rows=10000 --microbench-wr-rows=10"

The run.sh script provides simple default parameters for TPC-C++. It accepts three arguments: 
    run.sh [SF] [# of threads] [duration]
It also provides proper tcmalloc parameters.

