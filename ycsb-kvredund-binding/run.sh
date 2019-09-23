#!/bin/bash

numofexp=$1

HOME=`pwd`
result_dir=$HOME/$2

mkdir -p $result_dir
rm *.log # remove uncessary files

threads="64"
#tests="evalf"
#tests="evala_constant evalb_constant evalc_constant evald_constant evale_constant evalf_constant evala_uniform evalb_uniform evalc_uniform evald_uniform evale_uniform evalf_uniform"
#tests="evala_uniform evalb_uniform evalc_uniform evald_uniform evalf_uniform wr91_uniform wr82_uniform wr73_uniform wr64_uniform"
tests="wr91_uniform wr73_uniform wr55_uniform wr37_uniform wr19_uniform"
kvredund_type="1 2 0 3" # 0-KVRaid 1-KVEC 2-KVMirror 3-KVRaidPack
meta_type="1 2" # 0-Mem 1-Storage (leveldb) 2-CockoHashMap
gc_ena="1"

dev_cap='85899345920'
sed -i 's/\"dev_cap\":.*/\"dev_cap\":'${dev_cap}',/' kvredund_config.json
recordcnt='50000000'
sed -i 's/recordcount=.*/recordcount='$recordcnt'/' workloads/*_uniform
opcnt='50000000'
sed -i 's/operationcount=.*/operationcount='$opcnt'/' workloads/*_uniform
fieldlen='4000'
sed -i 's/fieldlength=.*/fieldlength='$fieldlen'/' workloads/*_uniform
minfieldlen='100'
sed -i 's/minfieldlength=.*/minfieldlength='$minfieldlen'/' workloads/*_uniform

for exp_id in $( seq 1 $numofexp )
do
	for kv_type in $kvredund_type
	do
		sed -i 's/\"kvr_type\":.*/\"kvr_type\":'${kv_type}',/' kvredund_config.json
		for m_type in $meta_type
		do
			sed -i 's/\"meta_type\":.*/\"meta_type\":'${m_type}',/' kvredund_config.json
			for gc_en in $gc_ena
			do
				sed -i 's/\"gc_ena\":.*/\"gc_ena\":'${gc_en}',/' kvredund_config.json

				for testfile in $tests
				do
					result_txt=$result_dir/${testfile}_${kv_type}_${m_type}_${gc_en}_${exp_id}
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
						
						sleep 60
						# ycsb load
						./bin/ycsb load kvredund -s -P workloads/$testfile -threads $numofthreads -p maxexecutiontime=1800 -jvm-args="-Xms16g -Xmx16g" 2> err.log 
							
						echo load >> $result_txt
						printf "load_tp: " >> $result_txt
						sed '/CLEANUP/d' err.log |grep "operations" |awk '{print $7}'|awk '{if(NR>5)SUM+=$1} END{print SUM/(NR-3)}' >> $result_txt
						printf "\n" >> $result_txt
						printf "load_lat: " >> $result_txt
						sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/INSERT/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>5)SUM+=$9} END{print SUM/(NR-3)}' >> $result_txt
						printf "\n" >> $result_txt
						printf "load_lat_99th: " >> $result_txt
						sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/INSERT/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| sed 's/,/ /g' | awk 'BEGIN {max=0} {if($13>max) max=$13} END {print max}' >> $result_txt
						printf "\n" >> $result_txt
						# report io
						printf "store_ios: " >> $result_txt
						cat kv_device.log|grep ", get"| awk '{ SUM += $2} END { print SUM }' >> $result_txt
						printf "get_ios: " >> $result_txt
						cat kv_device.log|grep ", get"| awk '{ SUM += $4} END { print SUM }' >> $result_txt
						printf "delete_ios: " >> $result_txt
						cat kv_device.log|grep ", get"| awk '{ SUM += $6} END { print SUM }' >> $result_txt
						# report device usage
						printf "dev_usage: " >> $result_txt
						cat kv_device.log|grep "usage"| awk '{ SUM += $2} END { print SUM }' >> $result_txt

						rm -rf kv_device.log err.log
						sleep 10

						# ycsb run 
						retry_cnt=0
						while :
						do
							./bin/ycsb run kvredund -s -P workloads/$testfile -threads $numofthreads -p maxexecutiontime=1800 -jvm-args="-Xms16g -Xmx16g"  2> err.log  
							
							if test -n "$(find ./ -maxdepth 1 -name 'hs_err*' -print -quit)"
							then
								rm hs_err*
								let "retry_cnt=retry_cnt+1"
								if [ $retry_cnt -ge 3 ]; then
									break
								fi
								echo retry number $retry_cnt
								sleep 10
								continue
							fi
							if [[ -f kv_device.log ]]; then
								echo ycsb run success
							else
								echo "un-normal terminate for ycsb run"
								cat err.log
								break
							fi

							echo "run" >> $result_txt
							printf "run_tp: " >> $result_txt
							sed '/CLEANUP/d' err.log |grep "operations" |awk '{print $7}'|awk '{if(NR>5)SUM+=$1} END{print SUM/(NR-3)}' >> $result_txt
							printf "\n" >> $result_txt
							printf "insert_lat: " >> $result_txt
							sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/INSERT/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>5)SUM+=$9} END{print SUM/(NR-3)}' >> $result_txt
							printf "\n" >> $result_txt
							printf "load_lat_99th: " >> $result_txt
							sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/INSERT/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| sed 's/,/ /g' | awk 'BEGIN {max=0} {if($13>max) max=$13} END {print max}' >> $result_txt
							printf "\n" >> $result_txt
							printf "update_lat: " >> $result_txt
							sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/UPDATE/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>5)SUM+=$9} END{print SUM/(NR-3)}' >> $result_txt
							printf "\n" >> $result_txt
							printf "update_lat_99th: " >> $result_txt
							sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/UPDATE/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| sed 's/,/ /g' | awk 'BEGIN {max=0} {if($13>max) max=$13} END {print max}' >> $result_txt
							printf "\n" >> $result_txt
							printf "read_lat: " >> $result_txt
							sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/READ/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>5)SUM+=$9} END{print SUM/(NR-3)}' >> $result_txt
							printf "\n" >> $result_txt
							printf "read_lat_99th: " >> $result_txt
							sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/READ/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| sed 's/,/ /g' | awk 'BEGIN {max=0} {if($13>max) max=$13} END {print max}' >> $result_txt
							printf "\n" >> $result_txt
							printf "scan_lat: " >> $result_txt
							sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/SCAN/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>5)SUM+=$9} END{print SUM/(NR-3)}' >> $result_txt
							printf "\n" >> $result_txt
							printf "scan_lat_99th: " >> $result_txt
							sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/SCAN/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| sed 's/,/ /g' | awk 'BEGIN {max=0} {if($13>max) max=$13} END {print max}' >> $result_txt
							printf "\n" >> $result_txt
							printf "rmw_lat: " >> $result_txt
							sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/READMODIFYWRITE/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>5)SUM+=$9} END{print SUM/(NR-3)}' >> $result_txt
							printf "\n" >> $result_txt
							printf "rmw_lat_99th: " >> $result_txt
							sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/READMODIFYWRITE/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| sed 's/,/ /g' | awk 'BEGIN {max=0} {if($13>max) max=$13} END {print max}' >> $result_txt
							printf "\n" >> $result_txt

							# report io
							printf "store_ios: " >> $result_txt
							cat kv_device.log|grep ", get"| awk '{ SUM += $2} END { print SUM }' >> $result_txt
							printf "get_ios: " >> $result_txt
							cat kv_device.log|grep ", get"| awk '{ SUM += $4} END { print SUM }' >> $result_txt
							printf "delete_ios: " >> $result_txt
							cat kv_device.log|grep ", get"| awk '{ SUM += $6} END { print SUM }' >> $result_txt
							# report device usage
							printf "dev_usage: " >> $result_txt
							cat kv_device.log|grep "usage"| awk '{ SUM += $2} END { print SUM }' >> $result_txt

							printf "invalid-alive: " >> $result_txt
							cat kv_device.log|grep "invalid-alive" | awk '{ SUM += $2} END { print SUM }' >> $result_txt
							printf "\n" >> $result_txt
							printf "invalid-alive-group0: " >> $result_txt
							cat kv_device.log|grep "invalid-alive" | awk '{ SUM += $4} END { print SUM }' >> $result_txt
							printf "\n" >> $result_txt
							printf "invalid-alive-group1: " >> $result_txt
							cat kv_device.log|grep "invalid-alive" | awk '{ SUM += $6} END { print SUM }' >> $result_txt
							printf "\n" >> $result_txt
							printf "invalid-alive-group2: " >> $result_txt
							cat kv_device.log|grep "invalid-alive" | awk '{ SUM += $8} END { print SUM }' >> $result_txt
							printf "\n" >> $result_txt
							printf "invalid-alive-group3: " >> $result_txt
							cat kv_device.log|grep "invalid-alive" | awk '{ SUM += $10} END { print SUM }' >> $result_txt
							printf "\n" >> $result_txt

							echo "" >> $result_txt
							rm -rf *.log
							break
						done

						
						sleep 30
						
					done
				done
				# no gc for KVEC KVMirror
				if [ "$kv_type" == "2" ] || [ "$kv_type" == "1" ]; then
					break
				fi
			done
			# no meta_type for KVMirror
			if [ "$kv_type" == "2" ] || [ "$kv_type" == "1" ] || [ "$kv_type" == "3" ]; then
				break
			fi
		done
	done
	sleep 120
done

rm *.log
echo testing completed
