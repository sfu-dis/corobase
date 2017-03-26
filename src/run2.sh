#!/bin/bash
# $1 - executable
# $2 - benchmark
# $3 - num of threads
# $4 - other parameters like --retry-aborted-transactions
# $5 - other parameters for the workload, e.g., --fast-new-order-id-gen

if [[ $# -lt 4 ]]; then
    echo "Too few arguments. "
    echo "Usage $0 <executable> <benchmark> <threads> <runtime>"
    exit
fi

LOGDIR=/dev/shm/$USER/ermia-log
mkdir -p $LOGDIR
trap "rm -f $LOGDIR/*" EXIT

exe=$1
workload=$2
bench=${workload:0:4}
threads=$3

if [[ "$bench" != "tpce" && "$bench" != "ycsb" && "$bench" != "tpcc" ]]; then
  echo "Unsupported benchmark $bench."
fi

options="$1 -verbose $4 -benchmark $bench -threads $threads -log_data_dir $LOGDIR -log_buffer_mb=512" 
echo $options
if [ "$bench" == "tpcc" ]; then
  btype=${workload:4:1}
  wh_spread=0
  if [ "$btype" == "h" ]; then
    suppliers_x=${workload:5}
    suppliers=`expr $suppliers_x \* 100`
    $options -benchmark_options "--workload-mix="40,38,0,4,4,4,10,0" --suppliers=$suppliers --warehouse-spread=$wh_spread $5"
  elif [ "$btype" == "+" ]; then
    $options -benchmark_options "--workload-mix="41,43,4,4,4,4,0,0" --warehouse-spread=$wh_spread $5"
  elif [ "$btype" == "r" ]; then
    $options -benchmark_options "--workload-mix="0,0,0,0,50,50,0,0" --warehouse-spread=$wh_spread $5"
  else
    if [ "$workload" == "tpcc_contention" ]; then
      wh_spread="100"
    fi
    $options -benchmark_options "--workload-mix="0,0,0,0,50,50,0,0" --warehouse-spread=$wh_spread $5"
  fi
elif [ "$bench" == "tpce" ]; then
  if [ "$workload" == "tpce_org" ]; then
    $options -benchmark_options "--egen-dir ./benchmarks/egen/flat/egen_flat_in --customer 5000 --working-days 10 --workload-mix="4.9,13,1,18,14,8,10.1,10,19,2,0" $5"
  else
    query_rng=${workload:4}
    $options -benchmark_options "--query-range $query_rng --egen-dir ./benchmarks/egen/flat/egen_flat_in --customer 5000 --working-days 10 --workload-mix="4.9,8,1,13,14,8,10.1,10,9,2,20" $5"
  fi
elif [ "$bench" == "ycsb" ]; then
  $options -benchmark_options "$5"
else
  echo "Unspported benchmark $bench."
fi
