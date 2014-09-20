rm -rf /tmpfs/silo-log/* 
export LD_PRELOAD="/usr/lib/libtcmalloc.so" 
./out-perf.masstree/benchmarks/dbtest --verbose --bench tpcc --scale-factor 32 --num-threads 32 --runtime 30 --log-dir /tmpfs/silo-log/

