#!/bin/bash
# $1 - executable
# $2 - benchmark
# $3 - num of threads
# $4 - runtime
# $5 - other parameters like --retry-aborted-transactions
# $6 - other parameters for the workload, e.g., --fast-new-order-id-gen

if [[ $# -lt 4 ]]; then
    echo "Too few arguments. "
    echo "Usage $0 <executable> <benchmark> <threads> <runtime>"
    exit
fi

LOGDIR=/tmpfs/$USER/silo-log
mkdir -p $LOGDIR
trap "rm -f $LOGDIR/*" EXIT

export TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES="2147483648"
export LD_PRELOAD="/usr/lib/libtcmalloc.so"

if [ "$2" == "tpcc_org" ]; then
#TPCC
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="45,43,0,4,4,4,0,0,0" $6"

elif [ "$2" == "tpcc_contention" ]; then
#TPCC
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="45,43,0,4,4,4,0,0,0" --warehouse-spread=100 $6"

elif [ "$2" == "tpcc++" ]; then
#TPCC++ ( /w credit check )
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="41,43,4,4,4,4,0,0,0" --warehouse-spread=100 $6"

elif [ "$2" == "tpce_org" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpce --scale-factor 500 --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--egen-dir ./benchmarks/egen/flat/egen_flat_in --customers 5000 --working-days 10 --workload-mix="4.9,13,1,18,14,8,10.1,10,19,2,0" $6"

elif [ "$2" == "tpce1" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpce --scale-factor 500 --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--egen-dir ./benchmarks/egen/flat/egen_flat_in --customers 5000 --working-days 10 --query-range 1 --workload-mix="4.9,8,1,13,14,8,10.1,10,9,2,20" $6"

elif [ "$2" == "tpce5" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpce --scale-factor 500 --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--egen-dir ./benchmarks/egen/flat/egen_flat_in --customers 5000 --working-days 10 --query-range 5 --workload-mix="4.9,8,1,13,14,8,10.1,10,9,2,20" $6"

elif [ "$2" == "tpce10" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpce --scale-factor 500 --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--egen-dir ./benchmarks/egen/flat/egen_flat_in --customers 5000 --working-days 10 --query-range 10 --workload-mix="4.9,8,1,13,14,8,10.1,10,9,2,20" $6"

elif [ "$2" == "tpce20" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpce --scale-factor 500 --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--egen-dir ./benchmarks/egen/flat/egen_flat_in --customers 5000 --working-days 10 --query-range 20 --workload-mix="4.9,8,1,13,14,8,10.1,10,9,2,20" $6"

elif [ "$2" == "tpce40" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpce --scale-factor 500 --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--egen-dir ./benchmarks/egen/flat/egen_flat_in --customers 5000 --working-days 10 --query-range 40 --workload-mix="4.9,8,1,13,14,8,10.1,10,9,2,20" $6"

elif [ "$2" == "tpce60" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpce --scale-factor 500 --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--egen-dir ./benchmarks/egen/flat/egen_flat_in --customers 5000 --working-days 10 --query-range 60 --workload-mix="4.9,8,1,13,14,8,10.1,10,9,2,20" $6"

elif [ "$2" == "tpce80" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpce --scale-factor 500 --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--egen-dir ./benchmarks/egen/flat/egen_flat_in --customers 5000 --working-days 10 --query-range 80 --workload-mix="4.9,8,1,13,14,8,10.1,10,9,2,20" $6"

else
	echo "wrong bench type, check run.sh"
fi
