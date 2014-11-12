#!/bin/bash

mix_arr=(   \
    "12-11-1-38-38" \
    "24-22-2-26-26" \
    "36-33-3-14-14" \
    "45-43-4-4-4"   \
)

for t in 1 6 12 18 24
do
    echo -ne "SF/Threads=$t\n"
    echo -ne "Mix\tCommit/s\tAborts/s\n"
    for m in "${mix_arr[@]}"; do
        echo -ne "$m\t"
        echo -ne `tail -2 ermia-sf-$t-th-$t-mix-$m.txt | head -1 | cut -d ' ' -f1`
        echo -ne "\t"
        echo -ne `tail -2 ermia-sf-$t-th-$t-mix-$m.txt | head -1 | cut -d ' ' -f5`
        echo -ne "\n"
    done
    echo ""
done
