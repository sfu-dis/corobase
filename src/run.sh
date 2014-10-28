rm -rf /tmpfs/silo-log/*
export LD_PRELOAD="/usr/lib/libtcmalloc.so"

./out-perf.masstree/benchmarks/dbtest --verbose --bench tpcc --scale-factor $1 --num-threads $1 --runtime 20 --log-dir /tmpfs/silo-log/ --pin-cpu
#taskset -c "$CPU_RANGE" ./out-perf.debug.check.masstree/benchmarks/dbtest --verbose --bench tpcc --scale-factor 24 --num-threads $1 --runtime 30 --log-dir /tmpfs/silo-log/

