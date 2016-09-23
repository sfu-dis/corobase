## ERMIA

Fast and Robust OLTP using Epoch-based Resource Management and Indirection Array

For more information, you may refer to our SIGMOD'16 paper (https://github.com/ermia-db/ermia/raw/master/ermia.pdf)

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

#### Choosing a CC scheme

ERMIA supports Read Committed (RC), Snapshot Isolation (SI), Serializable Snapshot Isolation (SSI), and Serial Safety Net (SSN) with SI/RC. The CMake config will build all variants. For example:

* `ERMIA_SI` - snapshot isolation;
* `ERMIA_SI_SSN_ESC` - SI + SSN + SSN early exclusion window checks without phantom protection;
* `ERMIA_SI_SSN_ESC_PP` - same as `ERMIA_SI_SSN_ESC` but with phantom protection;
* `ERMIA_SSI` - SSI without phantom protection;
* `ERMIA_SSI_PP` - SSI + phantom protection;

#### Adjust maximum concurrent workers

By default we support up to 256 cores. The limit can be adjusted by setting `MAX_THREADS` defined under `sysconf` in `dbcore/sm-config.h.` `MAX_THREADS` must be a multiple of 64.

#### Build it
--------

Use `src/build.sh` to compile ERMIA. For performance runs, `$ build.sh`, `$ build.sh 1` for debugging.

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
*Note the quotation marks for additional options.*

#### System-wide runtime options

`--node-memory-gb`: how many GBs of memory to allocate per socket.

`--null-log-device`: flush log buffer to `/dev/null`. With more than 30 threads, log flush (even to tmpfs) can easily become a bottleneck because of a mutex in the kernel held during the flush. This option does *not* disable logging, but it voids the ability to recover.

`--tmpfs-dir`: location of the log buffer's mmap file. Default: `/tmpfs/`.

`--enable-gc`: turn on garbage collection. Currently there is only one GC thread.

`--enable-chkpt`: enable checkpointing.

`--warm-up`: strategy to load versions upon recovery. Candidates are:
- `eager`: load all latest versions during recovery, so the database is fully in-memory when it starts to process new transactions;
- `lazy`: start a thread to load versions in the background after recovery, so the database is partially in-memory when it starts to process new transactions.
- `none`: load versions on-demand upon access.

*SSI and SSN specific:*

`--safesnap`: enable safe snapshot for read-only transactions.

*SSN-specific:*

`--ssn-read-opt-threshold`: versions that are read by a read-mostly transaction and older than this value are considered "old" and will not be tracked; setting it to 0 will skip all read tracking for read-mostly transactions (`TXN_FLAG_READ_MOSTLY`).

*SSI-specific:*
`--ssi-read-only-opt`: enable P&G style read-only optimization for SSI.
