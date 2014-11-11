rm -rf /tmpfs/silo-log/*
export TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES="2147483648"
export LD_PRELOAD="/usr/lib/libtcmalloc.so"
./out-perf.masstree/benchmarks/dbtest --verbose --bench tpcc --scale-factor 24 --num-threads $1 --runtime $2 --log-dir /tmpfs/silo-log/ --pin-cpu
rm -rf /tmpfs/silo-log/*
