OWNER="$(ls -ld /tmpfs/silo-log/* | awk 'NR==1 {print $3}')"
CPU_RANGE="0-`expr $1 + 4`"			# 5 daemons ( 4GCs + 1 Log )

if [ ${OWNER} != "" ] && [ ${OWNER} != ${USER} ]; then
		echo "Unavailable, Wait"
else
	echo "Available! :) "
	rm -rf /tmpfs/silo-log/*
	export LD_PRELOAD="/usr/lib/libtcmalloc.so"
	./out-perf.masstree/benchmarks/dbtest --verbose --bench tpcc --scale-factor 12 --num-threads $1 --runtime 120 --log-dir /tmpfs/silo-log/ --pin-cpu
#	./out-perf.debug.check.masstree/benchmarks/dbtest --verbose --bench tpcc --scale-factor 12 --num-threads $1 --runtime 60 --log-dir /tmpfs/silo-log/ --pin-cpu
	rm -rf /tmpfs/silo-log/*
fi
