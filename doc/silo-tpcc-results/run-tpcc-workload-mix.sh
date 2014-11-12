#!/bin/bash
# $1 - silo exec

# $2 - % of new order
# $3 - % of payment
# $4 - % of delivery
# $5 - % of order status
# $6 - $ of stock level

# $7 - sf/threads
# $8 - output dir


# workload mix:
# 12, 11, 38, 38, 1
# 24, 22, 26, 26, 2
# 36, 33, 14, 14, 3

$1 --runtime 30 --numa-memory 10G --num-threads $7 --scale-factor $7 --bench tpcc \
    -o --workload-mix="$2,$3,$4,$5,$6,0,0,0" &> $8/silo-sf-$7-th-$7-mix-$2-$3-$4-$5-$6.txt

