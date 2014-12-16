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
trap "rm -rf $LOGDIR" EXIT

export TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES="2147483648"
export LD_PRELOAD="/usr/lib/libtcmalloc.so"
numactl --interleave=all ./out-perf.masstree/benchmarks/dbtest --verbose --bench tpcc --scale-factor $1 --num-threads $2 --runtime $3 --log-dir $LOGDIR --pin-cpu

