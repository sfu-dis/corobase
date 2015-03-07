# $1: type ("-random", random read-set or "-static", fixed read-set)
type=$1
EXEC="./out-perf.masstree/benchmarks/dbtest --verbose --runtime 30 --bench tpcc --scale-factor 24 --pin-cpu --log-dir /tmpfs/$USER/silo-log/"

export TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES="2147483648"
export LD_PRELOAD="/usr/lib/libtcmalloc.so"

for READ_ROWS in 1000 10000 100000 200000  300000; do
    echo $READ_ROWS
    DIR=./microbench-results
    mkdir -p $DIR
    for w_ratio in 0.1 0.01 0.001 0.0001 0.00001; do
        w=`echo $READ_ROWS \* $w_ratio | bc -l`
        echo read $READ_ROWS rows, write $w rows
        for t in 24; do
            rm -rf /tmpfs/$USER/silo-log/*
            $EXEC --num-threads $t -o "--disable-read-only-snapshot --warehouse-spread=100 --microbench$type --microbench-rows=$READ_ROWS --microbench-wr-rows=$w" &> $DIR/microbench$type-rd-$READ_ROWS-wr-$w-t-$t.txt
        done
    done
done

