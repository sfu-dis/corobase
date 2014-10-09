#!/bin/bash
# $1 - "-static" or "-random"
# $2 - number of read rows

if [ "$#" -lt 1 ]; then
    echo "Wrong number of arguments"
    echo "Usage $0 <-static,-random> <number of read rows>"
    exit 0;
fi
type=$1

r='*'
if [ "$#" -gt 1 ]; then
    r=$2
fi
#echo "Using $r"

echo -ne "Reads Writes Write% Threads Commits/s Aborts/s\n"

for f in `ls -1v microbench$type-rd-$r-*.txt`
do
  wr=`echo "$f" | cut -d '-' -f6`
  wp=`echo "scale=5; $wr / $r * 100" | bc -l`
  th=`echo "$f" | cut -d '-' -f8 | cut -d '.' -f1`
  co=`tail -2 $f | head -1 | cut -d ' ' -f1`
  ab=`tail -2 $f | head -1 | cut -d ' ' -f5`
  ## ac=`echo "$ab / $co" | bc -l`
  ## echo -ne "$wr $wp $th $co $ab $ac\n"
  echo -ne "$r $wr $wp $th $co $ab\n"
done
