rm -rf /tmpfs/tzwang/silo-log/* 
export LD_PRELOAD="/usr/lib/libtcmalloc.so" 
./out-perf.masstree/benchmarks/dbtest --verbose --bench tpcc --scale-factor 12 --num-threads 12 --runtime 15 --log-dir /tmpfs/tzwang/silo-log/

