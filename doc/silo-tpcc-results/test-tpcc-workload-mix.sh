#!/bin/bash

# $1 - silo exec
# $2 - output dir

# workload mix:
# new order, payment, delivery, order status, stock level
# 12, 11, 1, 38, 38
# 24, 22, 2, 26, 26
# 36, 33, 3, 14, 14

mix_arr=(   \
    "12 11 1 38 38" \
    "24 22 2 26 26" \
    "36 33 3 14 14" \
    "45 43 4 4 4"   \
)

for t in 1 6 12 18 24; do
    for m in "${mix_arr[@]}"; do
        echo "start to run" $m sf/threads=$t
        ./run-tpcc-workload-mix.sh $1 $m $t $2
    done
done

