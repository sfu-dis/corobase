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
  command_log=${10}
  read_view_ms=${11}

  logbuf_mb=16

  group_commit_size_kb=4096 # sync/async - latency sensitive to this, not queue length
  group_commit_queue_length=1000  # cmdlog - latency sensitive to this, not group_commit_size_kb

  read_view_stat="/dev/shm/ermia_read_view.txt"
  offset_replay=0

  if [ "$command_log" == 1 ]; then
    null_log_device=1
  else
    null_log_device=0
  fi

  duration=10

  unset GLOG_logtostderr

  echo "----------"
  echo backups:$num_backups thread:$t $policy full_redo=$full redoers=$redoers delay=$delay nvram_log_buffer=$nvram group_commit_size_kb=$group_commit_size_kb command_log=$command_log
  echo "----------"
  ./run-cluster.sh SI $t $duration $t $logbuf_mb $read_view_stat tpcc_org tpccr \
    "-log_ship_offset_replay=$offset_replay -group_commit -group_commit_size_kb=$group_commit_size_kb -chkpt_interval=1000000 -node_memory_gb=16 -log_ship_by_rdma=0 -null_log_device=$null_log_device -truncate_at_bench_start -wait_for_backups -num_backups=$num_backups -persist_policy=$persist_policy -read_view_stat_interval_ms=$read_view_ms -read_view_stat_file=$read_view_stat -command_log=$command_log -command_log_buffer_mb=16 -group_commit_queue_length=$group_commit_queue_length" \
    "-primary_host=$primary -node_memory_gb=20 -log_ship_by_rdma=0 -nvram_log_buffer=$nvram -quick_bench_start -wait_for_primary -replay_policy=$policy -full_replay=$full -replay_threads=$redoers -nvram_delay_type=$delay -read_view_stat_interval_ms=$read_view_ms -read_view_stat_file=$read_view_stat -command_log=$command_log -command_log_buffer_mb=16" \
    "${backups[@]:0:$num_backups}"
  echo
}

sync_clock() {
  n=$1
  echo "sync clock for primary"
  sudo ntpd -gq &> /dev/null
  for b in ${backups[@]:0:$n}; do
    echo "sync clock for $b"
    ssh -o StrictHostKeyChecking=no $b "sudo ntpd -gq &> /dev/null"
  done
}

# Synchronous shipping
sync_ship() {
  for num_backups in 1 2 3 4 5 6 7; do
    for full_redo in 1; do
      #for redoers in 1 4 8 16; do
      for redoers in 4; do
        run $num_backups 16 "bg" $full_redo $redoers none 0 0 sync 0 0
      done
    done
    run $num_backups 16 none 1 0 none 0 0 sync 0 0
  done
}

async_ship() {
  for num_backups in 1 2 3 4 5 6 7; do
    for full_redo in 1; do
      #for redoers in 1 4 8 16; do
      for redoers in 4; do
        run $num_backups 16 "bg" $full_redo $redoers none 0 0 async 0 0
      done
    done
    run $num_backups 16 none 1 0 none 0 0 async 0 0
  done
}

cmdlog() {
  for num_backups in 1 2 3 4 5 6 7; do
    #for redoers in 1 4 8 16; do
    for redoers in 4; do
      run $num_backups 16 "bg" 1 $redoers none 0 0 sync 1 0
    done
    run $num_backups 16 none 1 0 none 0 0 sync 1 0
  done
}

read_view() {
  for num_backups in 1; do
    for persist_policy in sync async; do
      sync_clock $num_backups
      for redoers in 4; do
        for full_redo in 1; do
          run $num_backups 16 "bg" $full_redo $redoers none 0 0 $persist_policy 0 20
        done
      done
    done

    # cmdlog
    sync_clock $num_backups
    for redoers in 1 4 16; do
      run $num_backups 16 "bg" 1 $redoers none 0 0 sync 1 20
    done
  done
}

for r in 1 2 3; do
  echo "Running async_ship r$r"
  async_ship

  echo "Running sync_ship r$r"
  sync_ship

  echo "Running cmdlog r$r"
  cmdlog
done

echo "Running read_view"
read_view

