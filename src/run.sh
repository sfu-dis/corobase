#!/bin/bash
# $1 - executable
# $2 - benchmark
# $3 - scale factor (not applicable to TPC-E*)
# $4 - num of threads
# $5 - runtime
# $6 - other parameters like --retry-aborted-transactions
# $7 - other parameters for the workload, e.g., --fast-new-order-id-gen

if [[ $# -lt 4 ]]; then
    echo "Too few arguments. "
    echo "Usage $0 <executable> <benchmark> <threads> <runtime>"
    exit
fi

LOGDIR=/tmpfs/$USER/ermia-log
mkdir -p $LOGDIR
trap "rm -f $LOGDIR/*" EXIT

exe=$1
workload=$2
bench=${workload:0:4}
sf=$3
threads=$4
runtime=$5

if [ "$bench" == "tpce" ]; then
  sf="500"
elif [[ "$bench" != "ycsb" && "$bench" != "tpcc" ]]; then
  echo "Unsupported benchmark $bench."
fi

options="$1 --verbose $6 --bench $bench --num-threads $threads --scale-factor $sf --runtime $runtime \
  --log-dir $LOGDIR --log-buffer-mb=512 --log-segment-mb=8192 --parallel-loading"
echo $options
if [ "$bench" == "tpcc" ]; then
  btype=${workload:4:1}
  wh_spread=0
  if [ "$btype" == "h" ]; then
    suppliers_x=${workload:5}
    suppliers=`expr $suppliers_x \* 100`
    $options -o "--workload-mix="40,38,0,4,4,4,10,0" --suppliers=$suppliers --warehouse-spread=$wh_spread $7"
  elif [ "$btype" == "+" ]; then
    $options -o "--workload-mix="41,43,4,4,4,4,0,0" --warehouse-spread=$wh_spread $7"
  elif [ "$btype" == "r" ]; then
    $options -o "--workload-mix="0,0,0,0,50,50,0,0" --warehouse-spread=$wh_spread $7"
  else
    if [ "$workload" == "tpcc_contention" ]; then
      wh_spread="100"
    fi
    $options -o "--workload-mix="45,43,0,4,4,4,0,0" --warehouse-spread=$wh_spread $7"
  fi
elif [ "$bench" == "tpce" ]; then
  if [ "$workload" == "tpce_org" ]; then
    $options -o "--egen-dir ./benchmarks/egen/flat/egen_flat_in --customer 5000 --working-days 10 --workload-mix="4.9,13,1,18,14,8,10.1,10,19,2,0" $7"
  else
    query_rng=${workload:4}
    $options -o "--query-range $query_rng --egen-dir ./benchmarks/egen/flat/egen_flat_in --customer 5000 --working-days 10 --workload-mix="4.9,8,1,13,14,8,10.1,10,9,2,20" $7"
  fi
elif [ "$bench" == "ycsb" ]; then
  $options -o "$7"
else
  echo "Unspported benchmark $bench."
fi
