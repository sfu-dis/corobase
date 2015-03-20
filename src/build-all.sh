#!/bin/bash
echo "" > build.log

echo Build: SI
make clean &> /dev/null
MODE=perf DEBUG=0 CHECK_INVARIANTS=0 make -j dbtest &>> build.log
mv ./out-perf.masstree/benchmarks/dbtest ./dbtest-SI

echo Build: SSI
make clean &> /dev/null
MODE=perf DEBUG=0 CHECK_INVARIANTS=0 SSI=1 make -j dbtest &>> build.log
mv ./out-perf.masstree/benchmarks/dbtest ./dbtest-SSI

echo Build: SI+SSN
make clean &> /dev/null
MODE=perf DEBUG=0 CHECK_INVARIANTS=0 SI_SSN=1 make -j dbtest &>> build.log
mv ./out-perf.masstree/benchmarks/dbtest ./dbtest-SI_SSN

echo Build: RC+SSN
make clean &> /dev/null
MODE=perf DEBUG=0 CHECK_INVARIANTS=0 RC_SSN=1 make -j dbtest &>> build.log
mv ./out-perf.masstree/benchmarks/dbtest ./dbtest-RC_SSN

