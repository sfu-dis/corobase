## ERMIA

Fast and Robust OLTP using Epoch-based Resource Management and Indirection Array

#### Environment configurations

* Software dependencies: `libnuma` and `tcmalloc` (part of google perftools). Install from your favorite package manager.
* `mlock` limitation. Add the following to `/etc/security/limits.conf` (replace "[user]" with your login):
```
[user] soft memlock unlimited
[user] hard memlock unlimited
```
*Re-login to apply.*

#### Choosing a CC scheme

ERMIA supports Read Committed (RC), Snapshot Isolation (SI), Serializable Snapshot Isolation (SSI), and Serial Safety Net (SSN) with SI/RC. Switches are defined in src/macros.h:

* `#define USE_PARALLEL_SSI` to use serializable snapshot isolation (SSI);
* `#define USE_PARALLEL_SSN` to use the serial safety net (SSN);
   - SSN may be used in conjunction with RC (`USE_READ_COMMITTED`) or SI (default);
   - `# define DO_EARLY_SSN_CHECKS` enables SSN window exclusions during normal reads and writes.

`USE_PARALLEL_SSI` and `USE_PARALLEL_SSN` are mutually exclusive.

Giving `-D[SCHEME]` to `$make` also works. `SCHEME` can be `RC`, `SI`, `RC_SSN`, `SI_SSN`, or `SSI`.

#### Build it
--------

Use `src/build.sh` to compile ERMIA. For performance runs, `$ build.sh`, `$ build.sh 1` for debugging.

#### Run it
```
$run.sh \
       [executable] \
       [benchmark] \
       [num-threads] \
       [duration (seconds)] \
       "[other system-wide runtime options]" \
       "[other benchmark-specific runtime options]"`
```
*Note the quotation marks for additional options.*

#### System-wide runtime options

`--prefault-gig`: how many GBs of memory to prefault upon start.

`--null-log-device`: flush log buffer to `/dev/null`. With more than 30 threads, log flush (even to tmpfs) can easily become a bottleneck because of a mutex in the kernel held during the flush. This option does *not* disable logging, but it voids the ability to recover.

`--tmpfs-dir`: location of the log buffer's mmap file. Default: `/tmpfs/`.

`--enable-gc`: turn on garbage collection. Currently there is only one GC thread.

*SSI and SSN specific:*

`--safesnap`: enable safe snapshot for read-only transactions.

*SSN-specific:*

`--ssn-read-opt-threshold`: versions that are read by a read-mostly transaction and older than this value are considered "old" and will not be tracked; setting it to 0 will skip all read tracking for read-mostly transactions (TXN_FLAG_READ_MOSTLY).
