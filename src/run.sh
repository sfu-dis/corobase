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

LOGDIR=/tmpfs/$USER/ermia-log
mkdir -p $LOGDIR
trap "rm -f $LOGDIR/*" EXIT

# Use this (not recommended) if you don't want to -ltcmalloc.
#TCMALLOC=`whereis libtcmalloc.so | cut -d ':' -f2`
#if [ "$TCMALLOC" == "" ]; then
#    echo "Couldn't find tcmalloc."
#    exit
#else
#    echo "Found tcmalloc at:""$TCMALLOC"
#fi
#export LD_PRELOAD="$TCMALLOC"
export TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES="2147483648"

if [ "$2" == "tpcc_org" ]; then
#TPCC
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="45,43,0,4,4,4,0,0" $6"

elif [ "$2" == "tpcc_contention" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="45,43,0,4,4,4,0,0" --warehouse-spread=100 $6"

elif [ "$2" == "tpcch_1" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="40,38,0,4,4,4,10,0" --suppliers=100"

elif [ "$2" == "tpcch_5" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="40,38,0,4,4,4,10,0" --suppliers=500"

elif [ "$2" == "tpcch_10" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="40,38,0,4,4,4,10,0" --suppliers=1000"

elif [ "$2" == "tpcch_20" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="40,38,0,4,4,4,10,0" --suppliers=2000"

elif [ "$2" == "tpcch_40" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="40,38,0,4,4,4,10,0" --suppliers=4000"

elif [ "$2" == "tpcch_60" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="40,38,0,4,4,4,10,0" --suppliers=6000"

elif [ "$2" == "tpcch_80" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="40,38,0,4,4,4,10,0" --suppliers=8000"

elif [ "$2" == "tpcch_100" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="40,38,0,4,4,4,10,0" --suppliers=10000"

elif [ "$2" == "microbench_1k_1" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="0,0,0,0,0,0,0,100" --microbench-rows=1000 --microbench-wr-rows=1"

elif [ "$2" == "microbench_1k_10" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="0,0,0,0,0,0,0,100" --microbench-rows=1000 --microbench-wr-rows=10"

elif [ "$2" == "microbench_1k_100" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="0,0,0,0,0,0,0,100" --microbench-rows=1000 --microbench-wr-rows=100"

elif [ "$2" == "microbench_10k_10" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="0,0,0,0,0,0,0,100" --microbench-rows=10000 --microbench-wr-rows=10"

elif [ "$2" == "microbench_10k_100" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="0,0,0,0,0,0,0,100" --microbench-rows=10000 --microbench-wr-rows=100"

elif [ "$2" == "microbench_10k_1000" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="0,0,0,0,0,0,0,100" --microbench-rows=10000 --microbench-wr-rows=1000"

elif [ "$2" == "microbench_100k_100" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="0,0,0,0,0,0,0,100" --microbench-rows=100000 --microbench-wr-rows=100"

elif [ "$2" == "microbench_100k_1000" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="0,0,0,0,0,0,0,100" --microbench-rows=100000 --microbench-wr-rows=1000"

elif [ "$2" == "microbench_100k_10000" ]; then
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="0,0,0,0,0,0,0,100" --microbench-rows=100000 --microbench-wr-rows=10000"

elif [ "$2" == "tpcc++" ]; then
#TPCC++ ( /w credit check )
numactl --interleave=all	$1 --verbose $5 --bench tpcc --scale-factor $3  --num-threads $3 --runtime $4 --log-dir $LOGDIR --pin-cpu -o "--workload-mix="41,43,4,4,4,4,0,0" --warehouse-spread=100 $6"

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
