#!/bin/bash

primary=apt006
declare -a backups=("apt002" "apt035" "apt003" "apt031" "apt005" "apt030" "apt029")

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

function scalability_8threads_2redoers {
  for num_backups in 5 6 7; do #4 3 2 1; do
    t=8
    for redo_policy in pipelined sync; do
      echo "----------"
      echo "thread:$t $redo_policy (2 redo threads)"
      echo "----------"
      parts=4
      ./run-cluster.sh SI $t 10 $t tpcc_org tpccr \
        "-chkpt_interval=1000000 -node_memory_gb=19 -log_ship_by_rdma -fake_log_write -wait_for_backups -num_backups=$num_backups -log_ship_buffer_partitions=$parts" \
        "-primary_host=$primary -node_memory_gb=17 -log_ship_by_rdma -nvram_log_buffer -quick_bench_start -wait_for_primary -replay_policy=$redo_policy" \
        "${backups[@]:0:$num_backups}"
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

function nvram {
  for num_backups in 4 3 2 1; do
    for t in 16 8 4 2; do
      for redo_policy in none pipelined sync; do
        for delay in clflush clwb-emu; do
          echo "----------"
          echo thread:$t $redo_policy
          echo "----------"
          ./run-cluster.sh SI $t 10 $t tpcc_org tpccr \
            "-chkpt_interval=1000000 -node_memory_gb=19 -log_ship_by_rdma -fake_log_write -wait_for_backups -num_backups=$num_backups -log_ship_buffer_partitions=0" \
            "-primary_host=$primary -node_memory_gb=17 -log_ship_by_rdma -nvram_log_buffer -quick_bench_start -wait_for_primary -replay_policy=$redo_policy -nvram_delay_type=$delay" \
            "${backups[@]:0:$num_backups}"
        done
      done
    done
  done
}

echo "Experiment: scalability_8threads_2redoers"
scalability_8threads_2redoers

echo "Experiment: replay_speed"
replay_speed

echo "Experiment: scalability"
scalability

echo "Experiment: nvram"
nvram
