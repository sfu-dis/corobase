# $1 : raw output files' directory path
echo "system,bench,threads,runtime,total_commits,total_query_commits,total_aborts,total_sys_aborts,user_aborts,read_aborts,write_aborts,absent_aborts,longquery_tried"

for file in `ls $1`
do
	system=`echo $file | awk -F"," '{print $1}' | awk -F"-" '{print $1}'`
	bench=`echo $file | awk -F"," '{print $1}' | awk -F"-" '{print $2}'`
	threads=`echo $file | awk -F"," '{print $1}' | awk -F"-" '{print $3}'`
	runtime=`echo $file | awk -F"," '{print $1}' | awk -F"-" '{print $4}'`
	stat=`tail -n 1 $1/$file -q | awk -F" " '{ printf "%s,%s,%s,%s,%s", $1, $3, $5, $7, $9 }'`
	read_aborts=`grep "ABORT_REASON_READ_NODE" $1/$file | awk -F"=" '{ printf "%s", $2 } '`
	write_aborts=`grep "ABORT_REASON_WRITE_NODE" $1/$file | awk -F"=" '{printf "%s", $2}'`
	absent_aborts=`grep "ABORT_REASON_NODE_SCAN_READ" $1/$file | awk -F"=" '{printf "%s", $2}'`
	long_query_tried=`grep LongQuery $1/$file | awk -F"," '{print $6}' | awk -F"]" '{print $1}'`
	echo $system,$bench,$threads,$runtime,$stat,$read_aborts,$write_aborts,$absent_aborts,$long_query_tried
done
