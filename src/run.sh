#!/bin/bash
# $1 - num of threads
# $2 - runtime

if [[ $# -lt 2 ]]; then
    echo "Too few arguments. "
    echo "Usage $0 <threads> <runtime>"
    exit
fi

LOGDIR=/tmpfs/$USER/silo-log
mkdir -p $LOGDIR
trap "rm -f $LOGDIR/*" EXIT

export TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES="2147483648"
export LD_PRELOAD="/usr/lib/libtcmalloc.so"
#numactl --interleave=all ./out-perf.masstree/benchmarks/dbtest --verbose --bench tpcc --scale-factor $1 --num-threads $2 --runtime $3 --log-dir $LOGDIR --pin-cpu

#  minimum Customers: 5000, Default SF:500, working-days: 300( it's too long to load and OOM would stop loading also. thus let's try 3days)
#numactl --interleave=all ./out-perf.debug.check.masstree/benchmarks/dbtest --verbose --bench tpce --scale-factor 500 --num-threads $1 --runtime $2 --log-dir $LOGDIR --pin-cpu -o "--egen-dir ./benchmarks/egen/flat/egen_flat_in --customers 5000 --working-days 3"
numactl --interleave=all ./out-perf.masstree/benchmarks/dbtest --verbose --bench tpce --scale-factor 500 --num-threads $1 --runtime $2 --log-dir $LOGDIR --pin-cpu -o "--egen-dir ./benchmarks/egen/flat/egen_flat_in --customers 5000 --working-days 3"
#numactl --interleave=all ./out-perf.debug.check.masstree/benchmarks/dbtest --verbose --bench tpce --scale-factor 500 --num-threads 1 --runtime 2000 --log-dir /tmpfs/knkim/silo-log --pin-cpu -o "--egen-dir ./benchmarks/egen/flat/egen_flat_in --customers 5000 --working-days 3"

