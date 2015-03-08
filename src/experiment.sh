time=40

#for bench in tpce1 tpce5 tpce10 tpce20 tpce40 tpce60 tpce80 tpce_org tpcc_org tpcc_contention tpcc++ 
for bench in tpce1 tpce5 tpce10 tpce20 tpce40 tpce60 tpce80
do
for i in 1 6 12 18 24
do
	./run.sh out-perf.masstree/benchmarks/dbtest $bench $i $time > ermia-$bench-$i-$time 2>&1
done
done
