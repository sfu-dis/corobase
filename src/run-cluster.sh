#!/bin/bash

# Launch a primary and one or multiple backup nodes
# Note: make sure information such as pkeys are in place so ssh doesn't block

# $1 - CC
# $2 - Scale factor
# $3 - Duration (for primary)
# $4 - Number of threads
# $5 - Primary benchmark
# $6 - Backup benchmark
# $7 - Additional parameters (primary)
# $8 - Additional parameters (secondaries)
# $9 and beyond - a list of secondary server hosts

CC=$1; shift
scale_factor=$1; shift
duration=$1; shift
threads=$1; shift
primary_bench=$1; shift
backup_bench=$1; shift
primary_args="$1"; shift
backup_args="$1"; shift

exec_dir=`pwd`
output_dir=$exec_dir/results-`date +%Y%m%d%H%M%S`/
mkdir -p $output_dir

echo "Output dir: $output_dir"
echo "$CC, SF=$scale_factor, duration=$duration, threads=$threads"
echo "Primary args: $primary_args"
echo "Backup args: $backup_args"

#[run] will be 0 if this script is used directly
primary_output_file=$output_dir/primary.$CC.$primary_bench.sf$scale_factor.t$threads.txt
./run.sh ./ermia_$CC $primary_bench $scale_factor $threads $duration "$primary_args" &> $primary_output_file & export primary_pid=$!

cnt=0
for backup in "$@"; do
  # Wait until the primary is ready to receive connections from backups.
  # If there's multiple backups, must wait until the primary becomes
  # available again.
  for (( ; ; )); do
    # Make sure it finished handling the last client and has output new "ready" information
    l=`tail -1 $primary_output_file 2> /dev/null`
    if [[ $l == *"Expecting node"* ]]; then
      n=`echo $l | cut -d ' ' -f3`
      if [[ "$n" == "$cnt" ]]; then
        echo "Primary is ready, starting backup $backup..."
        sleep 3
        cnt=`expr $cnt + 1`
        break
      fi
    fi
  done

  backup_output_file=$output_dir/backup.$backup.$CC.$backup_bench.sf$scale_factor.t$threads.txt
  cmd="cd $exec_dir; \
    mkdir -p $output_dir; \
    ./run2.sh ./ermia_$CC $backup_bench $threads \"$backup_args\" &> $backup_output_file &"
  ssh -o StrictHostKeyChecking=no $backup $cmd
done

echo "Started all backups"

# Wait for the primary to finish
wait
echo "Primary exited"

# See if the backups are done as well
for backup in "$@"; do
  for (( ; ; )); do
    result=`ssh -o StrictHostKeyChecking=no $backup "ps aux | grep ermia_SI | grep -v grep"`
    if [[ $result == *"ermia_SI"* ]]; then
      sleep 2
    else
      echo "Backup $backup exited"
      break
    fi
  done
done

tail $primary_output_file
