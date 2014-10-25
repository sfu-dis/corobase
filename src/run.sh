rm -rf /tmpfs/silo-log/*
export LD_PRELOAD="/usr/lib/libtcmalloc.so"

CPU_RANGE="0-`expr $1 + 4`"			# 2 Background processes
#echo $CPU_RANGE
./out-perf.masstree/benchmarks/dbtest --verbose --bench tpcc --scale-factor 24 --num-threads $1 --runtime 30 --log-dir /tmpfs/silo-log/ --pin-cpu
#taskset -c "$CPU_RANGE" ./out-perf.debug.check.masstree/benchmarks/dbtest --verbose --bench tpcc --scale-factor 24 --num-threads $1 --runtime 30 --log-dir /tmpfs/silo-log/

