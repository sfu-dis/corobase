for bench in tpce-org #tpce5 tpce10 tpce20 tpce40 tpcc-org tpcc-contention tpcc++ tpce-org
do
for i in 1 6 12 18 24
do
	./run.sh out-perf.masstree/benchmarks/dbtest $bench $i 40 > ermia-${bench}-${i}threads 2>&1
done
done
