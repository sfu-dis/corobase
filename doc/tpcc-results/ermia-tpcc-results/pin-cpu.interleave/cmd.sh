numactl --interleave=all ./test-tpcc-workload-mix.sh ../../src/out-perf.masstree/benchmarks/dbtest ./pin-cpu.interleave/ "--pin-cpu"
