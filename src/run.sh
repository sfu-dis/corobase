rm -rf /tmpfs/tzwang/silo-log/* 
#export LD_PRELOAD="/usr/lib/libtcmalloc.so" 
./out-perf.masstree/benchmarks/dbtest --verbose --bench tpcc --scale-factor $1 --num-threads $1 --runtime 20 --log-dir /tmpfs/tzwang/silo-log/

