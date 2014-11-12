#!/bin/bash

# $1 - output dir

mix_arr=(   \
    "12 11 38 38 1" \
    "24 22 26 26 2" \
    "36 33 14 14 3" \
    "45 43 4 4 4"   \
)

for t in 1 6 12 18 24; do
    for m in "${mix_arr[@]}"; do
        echo "start to run" $m sf/threads=$t
        ./run-tpcc-workload-mix.sh $m $t $1
    done
done

