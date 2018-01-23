## ERMIA

Fast and Robust OLTP using Epoch-based Resource Management and Indirection Array

See our SIGMOD'16 paper [1] for a description of the system, our VLDBJ paper [2] for details in concurrency control, and our VLDB paper for replication.

\[1\] Kangnyeon Kim, Tianzheng Wang, Ryan Johnson and Ippokratis Pandis. [ERMIA: Fast Memory-Optimized Database System for Heterogeneous Workloads](https://github.com/ermia-db/ermia/raw/master/ermia.pdf). SIGMOD 2016.

\[2\] Tianzheng Wang, Ryan Johnson, Alan Fekete and Ippokratis Pandis. [Efficiently making (almost) any concurrency control mechanism serializable](https://link.springer.com/article/10.1007/s00778-017-0463-8). The VLDB Journal, Volume 26, Issue 4. 2017. [preprint](https://arxiv.org/pdf/1605.04292.pdf).

\[3\] Tianzheng Wang, Ryan Johnson and Ippokratis Pandis. [Query Fresh: Log Shipping on Steroids](http://www.vldb.org/pvldb/vol11/p406-wang.pdf). VLDB 2018.

#### Environment configurations

* Software dependencies: `libnuma`. Install from your favorite package manager. ERMIA uses `mmap` with `MAP_HUGETLB` to allocate huge pages. `MAP_HUGETLB` is available after Linux 2.6.32.
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

#### Build it
--------

We do not allow building in the source directory. Suppose we build in a separate directory:

```
$ mkdir build
$ cd build
$ cmake ../ -DCMAKE_BUILD_TYPE=[Debug/Release/RelWithDebInfo]
$ make -jN
```

After `make` there will be three executables under `build`: 
`ermia_SI` that runs snapshot isolation (not serializable);
`ermia_SI_SSN` that runs snapshot isolation + Serial Safety Net (serializable)
`ermia_SSI` that runs serializable snapshot isolation *

* Serializable Isolation for Snapshot Databases, M. Cahill, U. Rohm, A. Fekete, SIGMOD 2008.


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

*SSI and SSN specific:*

`--safesnap`: enable safe snapshot for read-only transactions.

*SSN-specific:*

`--ssn-read-opt-threshold`: versions that are read by a read-mostly transaction and older than this value are considered "old" and will not be tracked; setting it to 0 will skip all read tracking for read-mostly transactions (`TXN_FLAG_READ_MOSTLY`).

*SSI-specific:*
`--ssi-read-only-opt`: enable P&G style read-only optimization for SSI.
