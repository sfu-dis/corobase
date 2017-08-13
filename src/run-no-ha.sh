#!/bin/bash
for r in 1 2 3; do
  for t in 1 2 4 8 16; do
    echo $t $r
    sf=$t
    logbuf_mb=16 ./run.sh ./ermia_SI tpcc_org $sf $t 10 "-group_commit -group_commit_size_mb=4 -fake_log_write -node_memory_gb=19" &> ./Results-20170811-No-HA/SI.tpcc_org.sf$sf.t$t.r$r.txt
  done
done
