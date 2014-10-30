#!/bin/bash
if [ ${OWNER} != "" ] && [ ${OWNER} != ${USER} ]; then
		echo "Unavailable, Wait"
else
	echo "Available! :) "
	rm -rf /tmpfs/silo-log/*
	export LD_PRELOAD="/usr/lib/libtcmalloc.so"
	./out-perf.masstree/benchmarks/dbtest --verbose --bench tpcc --scale-factor 12 --num-threads $1 --runtime 30 --log-dir /tmpfs/silo-log/ --pin-cpu
#	./out-perf.debug.check.masstree/benchmarks/dbtest --verbose --bench tpcc --scale-factor 12 --num-threads $1 --runtime 60 --log-dir /tmpfs/silo-log/ --pin-cpu
	rm -rf /tmpfs/silo-log/*
fi
