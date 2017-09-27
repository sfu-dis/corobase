#!/bin/bash 
# Note: adjust segment size to 16GB - TCP backup currently doesn't
# support creating new segments like the RDMA variant does

primary="192.168.1.106"
declare -a backups=(192.168.1.101 192.168.1.104 192.168.1.107 192.168.1.102 192.168.1.105 192.168.1.100 192.168.1.103)

function cleanup {
  killall -9 ermia_SI 2> /dev/null
  for b in "${backups[@]}"; do
    echo "Kill $b"
    ssh $b "killall -9 ermia_SI 2> /dev/null"
  done
}

trap cleanup EXIT

run() {
  num_backups=$1
  t=$2
  policy=$3
  full=$4
  redoers=$5
  delay=$6
  nvram=$7
  persist_nvram_on_replay=$8
  persist_policy=$9
  read_view_ms=${10}
  read_view_stat=${11}

  logbuf_mb=16
  group_commit_size_mb=4

  duration=10

  unset GLOG_logtostderr

  echo "----------"
  echo backups:$num_backups thread:$t $policy full_redo=$full redoers=$redoers delay=$delay nvram_log_buffer=$nvram group_commit_size_mb=$group_commit_size_mb
  echo "----------"
  ./run-cluster.sh SI $t $duration $t $logbuf_mb $read_view_stat tpcc_org tpccr \
    "-group_commit -group_commit_size_mb=$group_commit_size_mb -chkpt_interval=1000000 -node_memory_gb=19 -log_ship_by_rdma=0 -fake_log_write -wait_for_backups -num_backups=$num_backups -persist_policy=$persist_policy -read_view_stat_interval_ms=$read_view_ms -read_view_stat_file=$read_view_stat" \
    "-primary_host=$primary -node_memory_gb=20 -log_ship_by_rdma=0 -nvram_log_buffer=$nvram -quick_bench_start -wait_for_primary -replay_policy=$policy -full_replay=$full -replay_threads=$redoers -nvram_delay_type=$delay -read_view_stat_interval_ms=$read_view_ms -read_view_stat_file=$read_view_stat" \
    "${backups[@]:0:$num_backups}"
  echo
}

read_view_stat="/dev/shm/ermia_read_view.txt"
# Synchronous shipping
sync_ship() {
  # Sync time
  for num_backups in 1 2 3 4 5 6 7; do
    sudo ntpd -gq &> /dev/null
    for redoers in 1 2 4 8 16; do
      run $num_backups 16 sync 1 $redoers none 0 0 sync 50 $read_view_stat
      run $num_backups 16 "bg" 1 $redoers none 0 0 sync 50 $read_view_stat
    done
    run $num_backups 16 none 1 0 none 0 0 sync 50 $read_view_stat
  done
}

async_ship() {
  # Sync time
  for num_backups in 1 2 3 4 5 6 7; do
    sudo ntpd -gq &> /dev/null
    for redoers in 1 2 4 8 16; do
      run $num_backups 16 "bg" 1 $redoers none 0 0 async 50 $read_view_stat
    done
    run $num_backups 16 none 1 0 none 0 0 async 50 $read_view_stat
  done
}

for r in 1 2 3; do
  echo "Running async_ship r$r"
  async_ship

  echo "Running sync_ship r$r"
  sync_ship
done

