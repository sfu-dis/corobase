#!/bin/bash 
primary=apt030
declare -a backups=("apt061" "apt053" "apt047" "apt043" "apt039" "apt040" "apt055")

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

  echo "----------"
  echo backups:$num_backups thread:$t $policy full_redo=$full redoers=$redoers delay=$delay
  echo "----------"
  ./run-cluster.sh SI $t 10 $t tpcc_org tpccr \
    "-chkpt_interval=1000000 -node_memory_gb=19 -log_ship_by_rdma -fake_log_write -wait_for_backups -num_backups=$num_backups" \
    "-primary_host=$primary -node_memory_gb=20 -log_ship_by_rdma -nvram_log_buffer -quick_bench_start -wait_for_primary -replay_policy=$policy -full_replay=$full -replay_threads=$redoers -nvram_delay_type=$delay" \
    "${backups[@]:0:$num_backups}"
  echo
}

single_backup_replay() {
  delay="none"
  for full_redo in 1 0; do
    for t in 16 8 4 2; do
      for policy in pipelined sync; do
        for redoers in 8 4 2 1; do
          if [ "$redoers" -ge "$t" ]; then
            continue
          fi
          num_backups=1
          echo "backups:$num_backups thread:$t $policy full_redo=$full_redo redoers=$redoers delay=$delay"
          run $num_backups $t $policy $full_redo $redoers $delay
        done
      done
    done
  done
}

multi_backup_replay() {
  delay="none"
  full_redo=0
  for num_backups in 5 6 7; do
    for policy in pipelined sync; do
      for redoers in 8 4 2 1; do
        t=16
        echo "backups:$num_backups thread:$t $policy full_redo=$full_redo redoers=$redoers delay=$delay"
        run $num_backups $t $policy $full_redo $redoers $delay
      done
    done
  done
}

nvram() {
  for delay in clwb-emu clflush; do
    full_redo=0
    for num_backups in 5 6 7; do
      t=16

      policy="none"
      redoers=0
      echo "backups:$num_backups thread:$t $policy full_redo=$full_redo redoers=$redoers delay=$delay"
      run $num_backups $t $policy $full_redo $redoers $delay

      for policy in pipelined ; do
        for redoers in 8; do
          echo "backups:$num_backups thread:$t $policy full_redo=$full_redo redoers=$redoers delay=$delay"
          run $num_backups $t $policy $full_redo $redoers $delay
        done
      done
    done
  done
}

no_replay() {
  delay="none"
  for t in 2 4 8 16; do
    run 1 $t none 0 0 $delay
  done
  for num_backups in 5 6 7; do
    t=16
    run $num_backups $t none 0 0 $delay
  done
}

full_replay() {
  delay="none"
  full_redo=1
  for num_backups in 1 2 3 4 5 6 7; do
    policy="pipelined"
    redoers=4
    t=16
    echo "backups:$num_backups thread:$t $policy full_redo=$full_redo redoers=$redoers delay=$delay"
    run $num_backups $t $policy $full_redo $redoers $delay
  done
}

for r in 1 2 3; do
  echo "Running full_replay r$r"
  full_replay
done

#for r in 1 2 3; do
#  echo "Running nvram r$r"
#  nvram
#done

#for r in 1 2 3; do
#  echo "Running no_replay r$r"
#  no_replay
#done

#for r in 1 2 3; do
#  echo "Running single_backup_replay r$r"
#  single_backup_replay
#done

#for r in 1 2 3; do
#  echo "Running multi_backup_replay r$r"
#  multi_backup_replay
#done


