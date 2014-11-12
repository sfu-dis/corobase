#!/bin/bash
# $1 - silo exec

# $2 - % of new order
# $3 - % of payment
# $4 - % of delivery
# $5 - % of order status
# $6 - $ of stock level

# $7 - sf/threads
# $8 - output dir

# $9 - any other parameters for dbtest (e.g., --pin-cpu)

# workload mix:
# 12, 11, 38, 38, 1
# 24, 22, 26, 26, 2
# 36, 33, 14, 14, 3

export TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES="2147483648"
export LD_PRELOAD="/usr/lib/libtcmalloc.so"
rm /tmpfs/silo-log/* -rf
$1 $9 --runtime 30 --num-threads $7 --scale-factor $7 --bench tpcc --log-dir /tmpfs/silo-log \
    -o --workload-mix="$2,$3,$4,$5,$6,0,0,0" &> $8/ermia-sf-$7-th-$7-mix-$2-$3-$4-$5-$6.txt

