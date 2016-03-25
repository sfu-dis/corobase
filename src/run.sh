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

exe=$1
workload=$2
bench=${workload:0:4}
threads=$3
sf="0"
runtime=$4

if [ "$bench" == "tpcc" ]; then
  sf=$threads
elif [ "$bench" == "tpce" ]; then
  sf="500"
elif [ "$bench" != "ycsb" ]; then
  echo "Unsupported benchmark $bench."
fi

options="$1 --verbose $5 --bench $bench --num-threads $threads --scale-factor $sf --runtime $runtime \
  --log-dir $LOGDIR --pin-cpu --log-buffer-mb=512 --log-segment-mb=8192"

if [ "$bench" == "tpcc" ]; then
  btype=${workload:4:1}
  wh_spread=0
  if [ "$btype" == "h" ]; then
    suppliers_x=${workload:6}
    echo $suppliers_x
    suppliers=`expr $suppliers_x * 100`
    echo $suppliers
    numactl --interleave=all $options -o "--workload-mix="40,38,0,4,4,4,10,0" --suppliers=$suppliers --warehouse-spread=$wh_spread $6"
  elif [ "$btype" == "+" ]; then
    numactl --interleave=all $options -o "--workload-mix="41,43,4,4,4,4,0,0" --suppliers=$suppliers --warehouse-spread=$wh_spread $6"
  else
    if [ "$workload" == "tpcc_contention" ]; then
      wh_spread="100"
    fi
    numactl --interleave=all $options -o "--workload-mix="45,43,0,4,4,4,0,0" --warehouse-spread=$wh_spread $6"
  fi
elif [ "$bench" == "tpce" ]; then
  if [ "$workload" == "tpce_org" ]; then
    numactl --interleave=all $options -o "--egen-dir ./benchmarks/egen/flat/egen_flat_in --customer 5000 --working-days 10 --workload-mix="4.9,13,1,18,14,8,10.1,10,19,2,0" $6"
  else
    query_rng=${workload:4}
    numactl --interleave=all $options -o "--query-range $query_rng --egen-dir ./benchmarks/egen/flat/egen_flat_in --customer 5000 --working-days 10 --workload-mix="4.9,8,1,13,14,8,10.1,10,9,2,20" $6"
  fi
elif [ "$bench" == "ycsb" ]; then
  numactl --interleave=all $options -o "$6"
else
  echo "Unspported benchmark $bench."
fi
