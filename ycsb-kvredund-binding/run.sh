#!/bin/bash

numofexp=$1

result_dir=$HOME/$2

mkdir -p $result_dir
rm *.log # remove uncessary files

threads="8"
tests="evalf"
#tests="evala evalb evalc evald evale evalf"
kvredund_type="0 1 2" # 0-KVRaid 1-KVEC 2-KVMirror
meta_type="0 1" # 0-Mem 1-Storage (leveldb)

# clean remaining log file in any
rm -rf *.log

for exp_id in $( seq 1 $numofexp )
do
	for testfile in $tests
	do
		for kv_type in $kvredund_type
		do
			sed -i 's/\"kvr_type\":.*/\"kvr_type\":'${kv_type}',/' kvredund_config.json
			for m_type in $meta_type
			do
				sed -i 's/\"meta_type\":.*/\"meta_type\":'${m_type}',/' kvredund_config.json

				result_txt=$result_dir/${testfile}_${kv_type}_${m_type}_${exp_id}
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
					 nvme format /dev/nvme4n1
					 nvme format /dev/nvme5n1
						
					# ycsb load
					./bin/ycsb load kvredund -s -P workloads/$testfile -threads $numofthreads > tmp.log 
						
					echo $testfile results >> $result_txt
					echo load >> $result_txt
					printf "load_tp: " >> $result_txt
					cat tmp.log|grep OVERALL|grep Throughput|awk '{print $3}' >> $result_txt
					printf "load_lat: " >> $result_txt
					cat tmp.log|grep AverageLatency|grep INSERT|awk '{print $3}' >> $result_txt
					# report io
					printf "store_ios: " >> $result_txt
					cat kv_device.log|grep ", get"| awk '{ SUM += $2} END { print SUM }' >> $result_txt
					printf "get_ios: " >> $result_txt
					cat kv_device.log|grep ", get"| awk '{ SUM += $4} END { print SUM }' >> $result_txt
					printf "delete_ios: " >> $result_txt
					cat kv_device.log|grep ", get"| awk '{ SUM += $6} END { print SUM }' >> $result_txt

					rm -rf kv_device.log
					
					sleep 3
					# ycsb run 

					./bin/ycsb run kvredund -s -P workloads/$testfile -threads $numofthreads > tmp.log  
					echo "run" >> $result_txt
					printf "run_tp: " >> $result_txt
					cat tmp.log|grep OVERALL|grep Throughput|awk '{print $3}' >> $result_txt
					printf "insert_lat: " >> $result_txt
					cat tmp.log|grep AverageLatency|grep INSERT|awk '{print $3}' >> $result_txt
					printf "update_lat: " >> $result_txt
					cat tmp.log|grep AverageLatency|grep UPDATE|awk '{print $3}' >> $result_txt
					printf "get_lat: " >> $result_txt
					cat tmp.log|grep AverageLatency|grep READ|awk '{print $3}' >> $result_txt
					printf "scan_lat: " >> $result_txt
					cat tmp.log|grep AverageLatency|grep SCAN|awk '{print $3}' >> $result_txt
					printf "rmw_lat: " >> $result_txt
					cat tmp.log|grep AverageLatency|grep READMODIFYWRITE|awk '{print $3}' >> $result_txt
					# report io
					printf "store_ios: " >> $result_txt
					cat kv_device.log|grep ", get"| awk '{ SUM += $2} END { print SUM }' >> $result_txt
					printf "get_ios: " >> $result_txt
					cat kv_device.log|grep ", get"| awk '{ SUM += $4} END { print SUM }' >> $result_txt
					printf "delete_ios: " >> $result_txt
					cat kv_device.log|grep ", get"| awk '{ SUM += $6} END { print SUM }' >> $result_txt

					rm -rf kv_device.log

					sleep 3
					# # ycsb run 

					# ./bin/ycsb run kvredund -s -P workloads/$testfile -threads $numofthreads > tmp.log  
					# echo "run scan 1" >> $result_txt
					# printf "run_tp: " >> $result_txt
					# cat tmp.log|grep OVERALL|grep Throughput|awk '{print $3}' >> $result_txt
					# printf "scan_lat: " >> $result_txt
					# cat tmp.log|grep AverageLatency|grep SCAN|awk '{print $3}' >> $result_txt
						
					echo "" >> $result_txt
				done
				# no meta_type for KVMirror
				if [[ "$kv_type" == "2" ]]; then
					break
				fi
			done
		done
	done
done

rm *.log
echo testing completed
