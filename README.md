## CoroBase

CoroBase is a research database engine that adopts coroutine-to-transaction paradigm.

CoroBase inherits the shared-everything architecture, synchronization and concurrency control protocol from ERMIA. See our SIGMOD'16 paper [1] for a description of ERMIA, our VLDBJ paper [2] for details in concurrency control, and our VLDB paper [3] for replication.

\[1\] Kangnyeon Kim, Tianzheng Wang, Ryan Johnson and Ippokratis Pandis. [ERMIA: Fast Memory-Optimized Database System for Heterogeneous Workloads](https://github.com/ermia-db/ermia/raw/master/ermia.pdf). SIGMOD 2016.

\[2\] Tianzheng Wang, Ryan Johnson, Alan Fekete and Ippokratis Pandis. [Efficiently making (almost) any concurrency control mechanism serializable](https://link.springer.com/article/10.1007/s00778-017-0463-8). The VLDB Journal, Volume 26, Issue 4. 2017.

\[3\] Tianzheng Wang, Ryan Johnson and Ippokratis Pandis. [Query Fresh: Log Shipping on Steroids](http://www.vldb.org/pvldb/vol11/p406-wang.pdf). VLDB 2018.

#### Environment configurations

* Software dependencies: `libnuma`. Install from your favorite package manager. CoroBase uses `mmap` with `MAP_HUGETLB` to allocate huge pages. `MAP_HUGETLB` is available after Linux 2.6.32.
* Make sure you have enough huge pages. Almost all memory allocations come from the space carved out here. Assuming 2MB pages, the command below will allocate 40GB of memory:
```
sudo sh -c 'echo [x pages] > /proc/sys/vm/nr_hugepages'
```
This limits the maximum for --node-memory-gb to 10 for a 4-socket machine (see below).

* `mlock` limits. Add the following to `/etc/security/limits.conf` (replace "[user]" with your login):
```
[user] soft memlock unlimited
[user] hard memlock unlimited
```
*Re-login to apply.*

#### Adjust maximum concurrent workers

By default we support up to 256 cores. The limit can be adjusted by setting `MAX_THREADS` defined under `config` in `dbcore/sm-config.h.` `MAX_THREADS` must be a multiple of 64.

--------
#### Build it
We do not allow building in the source directory. Suppose we build in a separate directory:

```
$ mkdir build
$ cd build
$ cmake ../ -DCMAKE_BUILD_TYPE=[Debug/Release/RelWithDebInfo]
$ make -jN
```

Currently the code can compile under Clang 8.0+. E.g., to use Clang 8.0, issue the following `cmake` command instead:
```
$ CC=clang-8.0 CXX=clang++-8.0 cmake ../ -DCMAKE_BUILD_TYPE=[Debug/Release/RelWithDebInfo]
```

After `make` there will be two executables under `build`: 

`ermia_SI` that runs CoroBase (optimized 2-level coroutine-to-transaction design) and ERMIA with snapshot isolation (not serializable);

`ermia_adv_coro_SI` that runs CoroBase (fully-nested coroutine-to-transaction design) with snapshot isolation (not serializable);


#### Run it
```
$run.sh \
       [executable] \
       [benchmark] \
       [scale-factor] \
       [num-threads] \
       [duration (seconds)] \
       "[other system-wide runtime options]" \
       "[other benchmark-specific runtime options]"`
```

#### Run example
```
Sequential (baseline):
$./run.sh ./ermia_SI ycsb 10 48 20 \
         "-physical_workers_only=1 -index_probe_only=1 -node_memory_gb=75 -null_log_device=1" \
         "-w C -r 10 -s 1000000000 -t sequential"

CoroBase (optimized 2-level coroutine-to-transaction design)
$./run.sh ./ermia_SI ycsb 10 48 20 \
         "-physical_workers_only=1 -index_probe_only=1 -node_memory_gb=75 -null_log_device=1 -coro_tx=1 -coro_batch_size=8" \
         "-w C -r 10 -s 1000000000 -t simple-coro"

CoroBase (fully-nested coroutine-to-transaction design)
$./run.sh ./ermia_adv_coro_SI ycsb 10 48 20 \
         "-physical_workers_only=1 -index_probe_only=1 -node_memory_gb=75 -null_log_device=1 -coro-tx=1 -coro_batch_size=8" \
         "-w C -r 10 -s 1000000000 -t adv-coro"

Coroutine-based multiget (flattened coroutines)
$./run.sh ./ermia_SI ycsb 10 48 20 \
         "-physical_workers_only=1 -index_probe_only=1 -node_memory_gb=75 -null_log_device=1" \
         "-w C -r 10 -s 1000000000 -t multiget-simple-coro"

Coroutine-based multiget (fully-nested coroutines)
$./run.sh ./ermia_adv_coro_SI ycsb 10 48 20 \
         "-physical_workers_only=1 -index_probe_only=1 -node_memory_gb=75 -null_log_device=1 -coro_tx=1" \
         "-w C -r 10 -s 1000000000 -t multiget-adv-coro

AMAC-based multiget
$./run.sh ./ermia_SI ycsb 10 48 20 \
         "-physical_workers_only=1 -index_probe_only=1 -node_memory_gb=75 -null_log_device=1" \
         "-w C -r 10 -s 1000000000 -t multiget-amac"
```

#### System-wide runtime options

`-node_memory_gb`: how many GBs of memory to allocate per socket.

`-null_log_device`: flush log buffer to `/dev/null`. With more than 30 threads, log flush (even to tmpfs) can easily become a bottleneck because of a mutex in the kernel held during the flush. This option does *not* disable logging, but it voids the ability to recover.

`-tmpfs_dir`: location of the log buffer's mmap file. Default: `/tmpfs/`.

`-enable_gc`: turn on garbage collection. Currently there is only one GC thread.

`-enable_chkpt`: enable checkpointing.

`-phantom_prot`: enable phantom protection.

`-warm-up`: strategy to load versions upon recovery. Candidates are:
- `eager`: load all latest versions during recovery, so the database is fully in-memory when it starts to process new transactions;
- `lazy`: start a thread to load versions in the background after recovery, so the database is partially in-memory when it starts to process new transactions.
- `none`: load versions on-demand upon access.

#### Benchmark-specific runtime options

`-w C`: YCSB-C read-only workload.

`-s 1000000000`: number of records in the database table.

`-r 10`: 10 querys per transaction.

`-t sequential`: 'sequential' for ERMIA implementation, 'simple-coro' for the optimized 2-level coroutine-to-transaction implementation, and 'adv-coro' for the fully-nested coroutines implementation.
