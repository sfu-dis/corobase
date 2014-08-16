#!/bin/bash
# $1 - "-static" or "-random"
# $2 - number of read rows
r=$2
type=$1

echo -ne "Writes\tWrite%\tThreads\tCommits/s\tAborts/s\tAborts/commits\n"

for f in `ls -1v microbench$type-rd-$r-*.txt`
do
  wr=`echo "$f" | cut -d '-' -f6`
  wp=`echo "scale=5; $wr / $r * 100" | bc -l`
  th=`echo "$f" | cut -d '-' -f8 | cut -d '.' -f1`
  co=`tail -2 $f | head -1 | cut -d ' ' -f1`
  ab=`tail -2 $f | head -1 | cut -d ' ' -f5`
  ac=`echo "$ab / $co" | bc -l`
  echo -ne "$wr\t$wp%\t$th\t$co\t$ab\t$ac\n"
done
