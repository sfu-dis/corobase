#!/bin/bash
# $1 - executable
# $2 - benchmark
# $3 - scale factor (not applicable to TPC-E*)
# $4 - num of threads
# $5 - runtime
# $6 - other parameters like --retry-aborted-transactions
# $7 - other parameters for the workload, e.g., --fast-new-order-id-gen

if [[ $# -lt 5 ]]; then
    echo "Too few arguments. "
    echo "Usage $0 <executable> <benchmark> <threads> <scalefactor> <runtime>"
    exit
fi

LOGDIR=/dev/shm/$USER/ermia-log
mkdir -p $LOGDIR
trap "rm -f $LOGDIR/*" EXIT

exe=$1; shift
workload=$1; shift
sf=$1; shift  # TPCE default sf=500
threads=$1; shift
runtime=$1; shift

bench=${workload:0:4}

if [[ "$bench" != "tpce" && "$bench" != "ycsb" && "$bench" != "tpcc" ]]; then
  echo "Unsupported benchmark $bench."
fi

if [ -z ${logbuf_mb+x} ]; then
  logbuf_mb=128
  echo "logbuf_mb is unset, using $logbuf_mb";
else
  echo "logbuf_mb is set to $logbuf_mb";
fi

options="$exe -verbose $1 -benchmark $bench -threads $threads -scale_factor $sf -seconds $runtime \
  -log_data_dir $LOGDIR -log_buffer_mb=$logbuf_mb -log_segment_mb=16384 -parallel_loading"
echo $options
if [ "$bench" == "tpcc" ]; then
  btype=${workload:4:1}
  wh_spread=0
  if [ "$btype" == "h" ]; then
    suppliers_x=${workload:5}
    suppliers=`expr $suppliers_x \* 100`
    $options -benchmark_options "--workload-mix="40,38,0,4,4,4,10,0" --suppliers=$suppliers --warehouse-spread=$wh_spread $2"
  elif [ "$btype" == "+" ]; then
    $options -benchmark_options "--workload-mix="41,43,4,4,4,4,0,0" --warehouse-spread=$wh_spread $2"
  elif [ "$btype" == "r" ]; then
    $options -benchmark_options "--workload-mix="0,0,0,0,50,50,0,0" --warehouse-spread=$wh_spread $2"
  else
    if [ "$workload" == "tpcc_contention" ]; then
      wh_spread="100"
    fi
    $options -benchmark_options "--workload-mix="45,43,0,4,4,4,0,0" --warehouse-spread=$wh_spread $2"
  fi
elif [ "$bench" == "tpce" ]; then
  if [ "$workload" == "tpce_org" ]; then
    $options -benchmark_options "--egen-dir ./benchmarks/egen/flat/egen_flat_in --customer 5000 --working-days 10 --workload-mix="4.9,13,1,18,14,8,10.1,10,19,2,0" $2"
  else
    query_rng=${workload:4}
    $options -benchmark_options "--query-range $query_rng --egen-dir ./benchmarks/egen/flat/egen_flat_in --customer 5000 --working-days 10 --workload-mix="4.9,8,1,13,14,8,10.1,10,9,2,20" $2"
  fi
elif [ "$bench" == "ycsb" ]; then
  $options -benchmark_options "$2"
else
  echo "Unspported benchmark $bench."
fi
