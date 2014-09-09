rm -rf /run/silo-log/* 
export LD_PRELOAD="/usr/lib/libtcmalloc.so" 
./out-perf.debug.check.masstree/benchmarks/dbtest --verbose --bench tpcc --scale-factor 2 --num-threads 4 --runtime 20 --log-dir /run/silo-log/

