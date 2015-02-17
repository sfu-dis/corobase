# $1 : raw output files' directory path
echo "system,bench,threads,runtime,total_commits, total_sys_aborts, user_aborts, total_aborts, long_query_commits, si_aborts, serial_aborts, rw_aborts, longquery_tried"
for file in `ls $1`
do
	system=`echo $file | awk -F"," '{print $1}' | awk -F"-" '{print $1}'`
	bench=`echo $file | awk -F"," '{print $1}' | awk -F"-" '{print $2}'`
	threads=`echo $file | awk -F"," '{print $1}' | awk -F"-" '{print $3}'`
	runtime=`echo $file | awk -F"," '{print $1}' | awk -F"-" '{print $4}'`
	stat=`tail -n 1 $1/$file -q | awk -F" " '{ printf "%s,%s,%s,%s,%s,%s,%s,%s", $1, $3, $5, $7, $9, $11, $13, $15 }'`
	long_query_tried=`grep LongQuery $1/$file | awk -F"," '{print $6}' | awk -F"]" '{print $1}'`
	echo $system,$bench,$threads,$runtime,$stat,$long_query_tried
done
