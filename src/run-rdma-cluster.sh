#!/bin/bash 
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

  logbuf_mb=16 #$9
  group_commit_size_mb=4 #${10}

  unset GLOG_logtostderr

  echo "----------"
  echo backups:$num_backups thread:$t $policy full_redo=$full redoers=$redoers delay=$delay nvram_log_buffer=$nvram group_commit_size_mb=$group_commit_size_mb
  echo "----------"
  ./run-cluster.sh SI $t 10 $t $logbuf_mb tpcc_org tpccr \
    "-group_commit -group_commit_size_mb=$group_commit_size_mb -chkpt_interval=1000000 -node_memory_gb=19 -log_ship_by_rdma -fake_log_write -wait_for_backups -num_backups=$num_backups -persist_policy=sync" \
    "-primary_host=$primary -node_memory_gb=20 -log_ship_by_rdma -nvram_log_buffer=$nvram -quick_bench_start -wait_for_primary -replay_policy=$policy -full_replay=$full -replay_threads=$redoers -nvram_delay_type=$delay -persist_nvram_on_replay=$persist_nvram_on_replay" \
    "${backups[@]:0:$num_backups}"
  echo
}

multi_backup_replay() {
  for policy in pipelined sync; do
    for redoers in 4 8 1 2; do
      for num_backups in 1 2 3 4 5 6 7; do
        echo "backups=$num_backups $policy redoers=$redoers"
        run $num_backups 16 $policy 0 $redoers none 1 0
      done
    done
  done
}

single_backup_pipelined_replay() {
  policy="pipelined"

  redoers=1
  for t in 1 2 4; do
    echo "backups=1 thread=$t $policy redoers=$redoers"
    run 1 $t $policy 0 $redoers none 1 0
  done

  t=8
  redoers=2
  echo "backups=1 thread=$t $policy redoers=$redoers"
  run 1 $t $policy 0 $redoers none 1 0
}

single_backup_sync_replay() {
  policy="sync"

  redoers=1
  for t in 1 2; do
    echo "backups=1 thread=$t $policy redoers=$redoers"
    run 1 $t $policy 0 $redoers none 1 0
  done

  t=4
  redoers=2
  echo "backups=1 thread=$t $policy redoers=$redoers"
  run 1 $t $policy 0 $redoers none 1 0

  t=8
  redoers=4
  echo "backups=1 thread=$t $policy redoers=$redoers"
  run 1 $t $policy 0 $redoers none 1 0
}

no_replay() {
  for t in 1 2 4 8; do
    for delay in none clwb-emu clflush; do
      echo "backups=1 no_replay threads=$t delay=$delay"
      run 1 $t none 0 0 $delay 1 0
    done
  done

  for num_backups in 1 2 3 4 5 6 7; do
    #for delay in none clwb-emu clflush; do
    for delay in none; do
    echo "backups=$num_backups no_replay delay=$delay"
      run $num_backups 16 none 0 0 $delay 1 0
    done
  done
}

#nvram() {
#  for delay in clwb-emu clflush; do
#    for num_backups in 1 2 3 4 5 6 7; do
#      for redoers in 4 8; do
#        echo "backups:$num_backups pipelined redoers=$redoers delay=$delay"
#        run $num_backups 16 pipelined 0 $redoers $delay 1 0
#      done
#    done
#  done
#}

no_nvram() {
  for num_backups in 1 2 3 4 5 6 7; do
    echo "backups:$num_backups thread:16 pipelined full_redo=0 redoers=4 delay=none nvram_log_buffer=0"
    run $num_backups 16 pipelined 0 4 none 0 0

    echo "backups:$num_backups thread:16 none full_redo=0 redoers=0 delay=none nvram_log_buffer=0"
    run $num_backups 16 none 0 0 none 0 0
  done
}

full_replay() {
  for num_backups in 1 2 3 4 5 6 7; do
    echo "backups:$num_backups pipelined full_redo redoers=4 delay=none"
    run $num_backups 16 pipelined 1 4 none 1 0
  done
}

nvram_persist_on_replay() {
  for delay in clwb-emu clflush; do
    for num_backups in 1 2 3 4 5 6 7; do
      run $num_backups 16 pipelined 0 4 $delay 1 1
      run $num_backups 16 pipelined 0 8 $delay 1 1
    done
  done
}

for r in 1 2 3; do
  echo "Running no_nvram r$r"
  no_nvram
done

for r in 1 2 3; do
  echo "Running multi_backup_replay r$r"
  multi_backup_replay
done

for r in 1 2 3; do
  echo "Running single_backup_pipelined_replay r$r"
  single_backup_pipelined_replay
done

for r in 1 2 3; do
  echo "Running single_backup_sync_replay r$r"
  single_backup_sync_replay
done

for r in 1 2 3; do
  echo "Running no_replay r$r"
  no_replay
done

#for r in 1 2 3; do
#  echo "Running nvram r$r"
#  nvram
#done

for r in 1 2 3; do
  echo "Running full_replay r$r"
  full_replay
done

for r in 1 2 3; do
  echo "Running nvram_persist_on_replay r$r"
  nvram_persist_on_replay
done

