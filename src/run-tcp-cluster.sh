#!/bin/bash

# Note: adjust segment size to 16GB - TCP backup currently doesn't
# support creating new segments like the RDMA variant does
primary="192.168.1.106" # apt030
declare -a backups=("apt008")

function cleanup {
  killall -9 ermia_SI 2> /dev/null
  for b in "${backups[@]}"; do
    echo "Kill $b"
    ssh $b "killall -9 ermia_SI 2> /dev/null"
  done
}

trap cleanup EXIT

for num_backups in 1; do
  for r in 1 2 3; do
    for t in 1 2 4 8 16; do
    #for t in 1 2 4 8 16; do
      full=0
      redoers=0
      delay="none"
      policy="none"
      echo "----------"
      echo $r thread:$t
      echo "----------"
      ./run-cluster.sh SI $t 10 $t tpcc_org tpccr \
        "-chkpt_interval=1000000 -node_memory_gb=19 -log_ship_by_rdma=0 -fake_log_write -wait_for_backups -num_backups=$num_backups" \
        "-primary_host=$primary -node_memory_gb=17 -log_ship_by_rdma=0 -nvram_log_buffer -quick_bench_start=0 -wait_for_primary -replay_policy=$policy -full_replay=$full -replay_threads=$t -nvram_delay_type=$delay" \
        "${backups[@]:0:$num_backups}"
    done
  done
done
