#!/bin/bash

numofexp=$1

result_dir=$HOME/$2

mkdir -p $result_dir
rm *.log # remove uncessary files

threads="32"
#tests="evalf"
val_dis="constant uniform"
tests="evala evalb evalc evald evale evalf"
kvredund_type="0 1 2" # 0-KVRaid 1-KVEC 2-KVMirror
meta_type="0 1" # 0-Mem 1-Storage (leveldb)


recordcnt='50000000'
sed -i 's/recordcount=.*/recordcount='$recordcnt'/' workloads/eval*_*
opcnt='10000000'
sed -i 's/operationcount=.*/operationcount='$opcnt'/' workloads/eval*_*

for exp_id in $( seq 1 $numofexp )
do
	for kv_type in $kvredund_type
	do
		sed -i 's/\"kvr_type\":.*/\"kvr_type\":'${kv_type}',/' kvredund_config.json
		for m_type in $meta_type
		do
			sed -i 's/\"meta_type\":.*/\"meta_type\":'${m_type}',/' kvredund_config.json

			for v_dis in $val_dis
			do
				result_txt=$result_dir/${v_dis}_${kv_type}_${m_type}_${exp_id}
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
					./bin/ycsb load kvredund -s -P workloads/eval_${v_dis}_load -threads $numofthreads 2> err.log 
						
					echo load >> $result_txt
					printf "load_tp: " >> $result_txt
					sed '/CLEANUP/d' err.log |grep "operations" |awk '{print $7}'|awk '{if(NR>3)SUM+=$1} END{print SUM/(NR-3)}' >> $result_txt
					printf "\n" >> $result_txt
					printf "load_lat: " >> $result_txt
					sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/INSERT/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>3)SUM+=$9} END{print SUM/(NR-3)}' >> $result_txt
					printf "\n" >> $result_txt
					printf "load_lat_99th: " >> $result_txt
					sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/INSERT/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>3)SUM+=$13} END{print SUM/(NR-3)}' >> $result_txt
					printf "\n" >> $result_txt
					# report io
					printf "store_ios: " >> $result_txt
					cat kv_device.log|grep ", get"| awk '{ SUM += $2} END { print SUM }' >> $result_txt
					printf "get_ios: " >> $result_txt
					cat kv_device.log|grep ", get"| awk '{ SUM += $4} END { print SUM }' >> $result_txt
					printf "delete_ios: " >> $result_txt
					cat kv_device.log|grep ", get"| awk '{ SUM += $6} END { print SUM }' >> $result_txt

					rm -rf kv_device.log
					sleep 3

					for testfile in $tests
					do
						wl=$testfile_$v_dis
						echo ===== workload $wl ====== >> $result_txt
						echo "" >> $result_txt
						# ycsb run 

						./bin/ycsb run kvredund -s -P workloads/$wl -threads $numofthreads 2> err.log  
						echo "run" >> $result_txt
						printf "run_tp: " >> $result_txt
						sed '/CLEANUP/d' err.log |grep "operations" |awk '{print $7}'|awk '{if(NR>3)SUM+=$1} END{print SUM/(NR-3)}' >> $result_txt
						printf "\n" >> $result_txt
						printf "insert_lat: " >> $result_txt
						sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/INSERT/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>3)SUM+=$9} END{print SUM/(NR-3)}' >> $result_txt
						printf "\n" >> $result_txt
						printf "load_lat_99th: " >> $result_txt
						sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/INSERT/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>3)SUM+=$13} END{print SUM/(NR-3)}' >> $result_txt
						printf "\n" >> $result_txt
						printf "update_lat: " >> $result_txt
						sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/UPDATE/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>3)SUM+=$9} END{print SUM/(NR-3)}' >> $result_txt
						printf "\n" >> $result_txt
						printf "update_lat_99th: " >> $result_txt
						sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/UPDATE/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>3)SUM+=$13} END{print SUM/(NR-3)}' >> $result_txt
						printf "\n" >> $result_txt
						printf "read_lat: " >> $result_txt
						sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/READ/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>3)SUM+=$9} END{print SUM/(NR-3)}' >> $result_txt
						printf "\n" >> $result_txt
						printf "read_lat_99th: " >> $result_txt
						sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/READ/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>3)SUM+=$13} END{print SUM/(NR-3)}' >> $result_txt
						printf "\n" >> $result_txt
						printf "scan_lat: " >> $result_txt
						sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/SCAN/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>3)SUM+=$9} END{print SUM/(NR-3)}' >> $result_txt
						printf "\n" >> $result_txt
						printf "scan_lat_99th: " >> $result_txt
						sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/SCAN/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>3)SUM+=$13} END{print SUM/(NR-3)}' >> $result_txt
						printf "\n" >> $result_txt
						printf "rmw_lat: " >> $result_txt
						sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/READMODIFYWRITE/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>3)SUM+=$9} END{print SUM/(NR-3)}' >> $result_txt
						printf "\n" >> $result_txt
						printf "rmw_lat_99th: " >> $result_txt
						sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/READMODIFYWRITE/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>3)SUM+=$13} END{print SUM/(NR-3)}' >> $result_txt
						printf "\n" >> $result_txt

						# report io
						printf "store_ios: " >> $result_txt
						cat kv_device.log|grep ", get"| awk '{ SUM += $2} END { print SUM }' >> $result_txt
						printf "get_ios: " >> $result_txt
						cat kv_device.log|grep ", get"| awk '{ SUM += $4} END { print SUM }' >> $result_txt
						printf "delete_ios: " >> $result_txt
						cat kv_device.log|grep ", get"| awk '{ SUM += $6} END { print SUM }' >> $result_txt

						echo "" >> $result_txt
						rm -rf *.log
						sleep 3
					done
					
				done
			done
			# no meta_type for KVMirror
			if [[ "$kv_type" == "2" ]]; then
				break
			fi
		done
	done
	
done

rm *.log
echo testing completed
