#!/bin/bash
# $1 - % of new order
# $2 - % of payment
# $3 - % of delivery
# $4 - % of order status
# $5 - $ of stock level

# $6 - sf/threads
# $7 - output dir

# workload mix:
# 12, 11, 38, 38, 1
# 24, 22, 26, 26, 2
# 36, 33, 14, 14, 3

rm /tmpfs/silo-log/* -rf
./out-perf.masstree/benchmarks/dbtest --runtime 30 --numa-memory 10G --num-threads $6 --scale-factor $6 --bench tpcc \
    -o --workload-mix="$1,$2,$3,$4,$5,0,0,0" &> $7/silo-sf-$6-th-$6-mix-$1-$2-$3-$4-$5.txt

