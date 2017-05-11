#!/bin/bash

primary=apt035
declare -a backups=("apt054" "apt037" "apt053" "apt034" "apt042" "apt041")

function cleanup {
  killall -9 ermia_SI 2> /dev/null
  for b in "${backups[@]}"; do
    echo "Kill $b"
    ssh $b "killall -9 ermia_SI 2> /dev/null"
  done
}

trap cleanup EXIT

function scalability {
  for num_backups in 4 3 2 1; do
    for t in 16 8 4 2; do
      for redo_policy in none pipelined sync; do
        echo "----------"
        echo thread:$t $redo_policy
        echo "----------"
        ./run-cluster.sh SI $t 10 $t tpcc_org tpccr \
          "-chkpt_interval=1000000 -node_memory_gb=19 -log_ship_by_rdma -fake_log_write -wait_for_backups -num_backups=$num_backups -log_ship_buffer_partitions=0" \
          "-primary_host=$primary -node_memory_gb=17 -log_ship_by_rdma -nvram_log_buffer -quick_bench_start -wait_for_primary -replay_policy=$redo_policy" \
          "${backups[@]:0:$num_backups}"
      done
    done
  done
}

function replay_speed {
  for num_backups in 1 2 3 4; do
    t=16
    for redo_policy in pipelined sync; do
      echo "----------"
      echo "thread:$t $redo_policy (single replay thread)"
      echo "----------"
      ./run-cluster.sh SI $t 10 $t tpcc_org tpccr \
        "-chkpt_interval=1000000 -node_memory_gb=19 -log_ship_by_rdma -fake_log_write -wait_for_backups -num_backups=$num_backups -log_ship_buffer_partitions=0" \
        "-primary_host=$primary -node_memory_gb=17 -log_ship_by_rdma -nvram_log_buffer -quick_bench_start -wait_for_primary -replay_policy=$redo_policy -single_redoer" \
        "${backups[@]:0:$num_backups}"

      for parts in 4 8 16; do  # parts/2 will be the number of replay threads
        echo "----------"
        echo "thread:$t $redo_policy partitions:$parts"
        echo "----------"
        ./run-cluster.sh SI $t 10 $t tpcc_org tpccr \
          "-chkpt_interval=1000000 -node_memory_gb=19 -log_ship_by_rdma -fake_log_write -wait_for_backups -num_backups=$num_backups -log_ship_buffer_partitions=$parts" \
          "-primary_host=$primary -node_memory_gb=17 -log_ship_by_rdma -nvram_log_buffer -quick_bench_start -wait_for_primary -replay_policy=$redo_policy" \
          "${backups[@]:0:$num_backups}"
      done
    done
  done
}

scalability
replay_speed
