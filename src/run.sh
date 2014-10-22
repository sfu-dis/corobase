rm -rf /tmpfs/silo-log/*
export LD_PRELOAD="/usr/lib/libtcmalloc.so"

CPU_RANGE="24-`expr $1 + 1 + 24`"			# 2 Background processes
echo $CPU_RANGE
taskset -c "$CPU_RANGE" ./out-perf.masstree/benchmarks/dbtest --verbose --bench tpcc --scale-factor $1 --num-threads $1 --runtime 20 --log-dir /tmpfs/silo-log/
#./out-perf.debug.check.masstree/benchmarks/dbtest --verbose --bench tpcc --scale-factor $1 --num-threads $1 --runtime 20 --log-dir /tmpfs/silo-log/

