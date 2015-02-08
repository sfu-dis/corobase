#!/bin/bash
# $1 - executable
# $2 - num of threads
# $3 - runtime

if [[ $# -lt 3 ]]; then
    echo "Too few arguments. "
    echo "Usage $0 <executable> <threads> <runtime>"
    exit
fi

LOGDIR=/tmpfs/$USER/silo-log
mkdir -p $LOGDIR
trap "rm -f $LOGDIR/*" EXIT

export TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES="2147483648"
export LD_PRELOAD="/usr/lib/libtcmalloc.so"

#TPCC
#numactl --interleave=all $1 --verbose --bench tpcc --scale-factor $2 --num-threads $2 --runtime $3 --log-dir $LOGDIR --pin-cpu -o --workload-mix="41,43,4,4,4,4,0,0,0"

#TPCE
#original mix
#numactl --interleave=all $1 --verbose --bench tpce --scale-factor 500 --num-threads $2 --runtime $3 --log-dir $LOGDIR --pin-cpu -o "--egen-dir ./benchmarks/egen/flat/egen_flat_in --customers 5000 --working-days 10 --workload-mix="4.9,13,1,18,14,8,10.1,10,19,2,0""

# new query mix
numactl --interleave=all $1 --verbose --bench tpce --scale-factor 500 --num-threads $2 --runtime $3 --log-dir $LOGDIR --pin-cpu -o "--egen-dir ./benchmarks/egen/flat/egen_flat_in --customers 5000 --working-days 10 --workload-mix="4.9,13,1,8,9,8,10.1,10,14,2,20""

#  minimum Customers: 5000, Default SF:500, working-days: 300( it's too long to load and OOM would stop loading also. thus let's try 10 days)

