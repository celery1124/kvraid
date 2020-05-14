#!/bin/bash

numofexp=$1

HOME=`pwd`
result_dir=$HOME/$2

mkdir -p $result_dir

threads="64"
tests="workloada workloadb workloadc"
db_type=" kvredund" 

target_dir="/mnt/md0/ycsb-data"
recordcnt='50000000'
sed -i 's/recordcount=.*/recordcount='$recordcnt'/' workloads/workload*
opcnt='50000000'
sed -i 's/operationcount=.*/operationcount='$opcnt'/' workloads/workload*

for exp_id in $( seq 1 $numofexp )
do
	for db in $db_type
	do
		
		for testfile in $tests
		do
			result_txt=$result_dir/${db}_${testfile}_${exp_id}
			result_save_dir=$result_dir/${testfile}
			mkdir -p $result_save_dir
			# clean file if existed
			echo "" > $result_txt
			for numofthreads in $threads
			do
				echo ===== $numofthreads threads ====== >> $result_txt
				echo "" >> $result_txt

				# format kvssd
				nvme format /dev/nvme0n1
				nvme format /dev/nvme1n1
				nvme format /dev/nvme2n1
				nvme format /dev/nvme3n1
				
				sleep 10
				
				# ycsb load
				/usr/bin/time -v ./bin/ycsb load $db -s -P workloads/$testfile -threads $numofthreads 1>out.log 2> err.log 
				cp out.log $result_save_dir/load_out_${exp_id}.log
				cp err.log $result_save_dir/load_err_${exp_id}.log
				echo load >> $result_txt
				printf "load_tp: " >> $result_txt
				cat out.log |grep "Throughput" |awk '{print $3}' >> $result_txt
				printf "\n" >> $result_txt

				sleep 10
				
				# report cpu
				cpu_user=`cat err.log | grep "User time" | awk '{print $4}'`
				cpu_sys=`cat err.log | grep "System time" | awk '{print $4}'`
				printf "cpu_time: " >> $result_txt
				echo "($cpu_user+$cpu_sys)"|bc >> $result_txt
				printf "cpu_util: " >> $result_txt
				cat err.log | grep "CPU" | awk '{print $7}' >> $result_txt

				# report dev utilization
				printf "dev_usage: " >> $result_txt
				nvme list|grep nvme0n1 |awk '{print $6}' >> $result_txt
				printf "\n" >> $result_txt
				

				# ycsb run 

				/usr/bin/time -v ./bin/ycsb run $db -s -P workloads/$testfile -threads $numofthreads 1>out.log 2> err.log  
				cp out.log $result_save_dir/run_out_${exp_id}.log
				cp err.log $result_save_dir/run_err_${exp_id}.log
				echo "run" >> $result_txt
				printf "run_tp: " >> $result_txt
				cat out.log |grep "Throughput" |awk '{print $3}' >> $result_txt
				printf "\n" >> $result_txt

				sleep 10
				
				# report cpu
				cpu_user=`cat err.log | grep "User time" | awk '{print $4}'`
				cpu_sys=`cat err.log | grep "System time" | awk '{print $4}'`
				printf "cpu_time: " >> $result_txt
				echo "($cpu_user+$cpu_sys)"|bc >> $result_txt
				printf "cpu_util: " >> $result_txt
				cat err.log | grep "CPU" | awk '{print $7}' >> $result_txt

				# report dev utilization
				printf "dev_usage: " >> $result_txt
				nvme list|grep nvme0n1 |awk '{print $6}' >> $result_txt
				printf "\n" >> $result_txt

				echo "" >> $result_txt
				rm *.log
				
			done
		done
	done
done

echo testing completed
