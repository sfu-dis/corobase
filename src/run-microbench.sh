#./out-perf.masstree/benchmarks/dbtest --verbose --runtime 30 --numa-memory 4G --bench tpcc --num-threads 32 --scale-factor 32 -o '--microbench --microbench-rows=100 --microbench-wr-ratio=10'
# $1: type ("-random", random read-set or "-static", fixed read-set)
type=$1
EXEC="hog-machine ./out-perf.masstree/benchmarks/dbtest --verbose --runtime 30 --bench tpcc --scale-factor 32 --pin-cpu --log-dir /tmpfs/tzwang/silo-log/"
DIR=../ermia-microbench-results-10k
READ_ROWS=10000
#100000
mkdir -p $DIR

#for w in 2 20 200 2000 20000
for w in 1 10 100 1000
#for w in 1 10 100 1000 10000
#for w in 3 30 300 3000 30000
do
    for t in 6 12 18 24
    do
        rm -rf /tmpfs/tzwang/silo-log/*
        export TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES="2147483648"
        export LD_PRELOAD="/usr/lib/libtcmalloc.so"
        $EXEC --num-threads $t -o "--microbench$type --microbench-rows=$READ_ROWS --microbench-wr-rows=$w" &> $DIR/microbench$type-rd-$READ_ROWS-wr-$w-t-$t.txt
    done
done

