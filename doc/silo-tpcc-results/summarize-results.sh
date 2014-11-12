
mix_arr=(   \
    "12-11-38-38-1" \
    "24-22-26-26-2" \
    "36-33-14-14-3" \
    "45-43-4-4-4"   \
)

for t in 1 6 12 18 24
do
    echo -ne "SF/Threads=$t\n"
    echo -ne "Mix\tCommit/s\tAborts/s\n"
    for m in "${mix_arr[@]}"; do
        echo -ne "$m\t"
        echo -ne `tail -2 silo-sf-$t-th-$t-mix-$m.txt | head -1 | cut -d ' ' -f1`
        echo -ne "\t"
        echo -ne `tail -2 silo-sf-$t-th-$t-mix-$m.txt | head -1 | cut -d ' ' -f5`
        echo -ne "\n"
    done
    echo ""
done
