#!/bin/bash

numofexp=$1

HOME=`pwd`
result_dir=$HOME/$2

mkdir -p $result_dir
rm *.log # remove uncessary files

MAX_EXE_TIME="7200"
threads="64"
#tests="evalf"
#tests="evala_constant evalb_constant evalc_constant evald_constant evale_constant evalf_constant evala_uniform evalb_uniform evalc_uniform evald_uniform evale_uniform evalf_uniform"
#tests="evala_uniform evalb_uniform evalc_uniform evald_uniform evalf_uniform wr91_uniform wr82_uniform wr73_uniform wr64_uniform"
#tests="allw_uniform "
#tests="wr55_uniform"
tests=" wr55_uniform wr37_uniform wr19_uniform"
#tests="recovery_uniform"
kvredund_type="3 " # 0-KVRaid 1-KVEC 2-KVMirror 3-KVRaidPack
meta_type="2" # 0-Mem 1-Storage (leveldb) 2-CockoHashMap
gc_ena="1"

dev_cap='320'
sed -i 's/\"dev_cap\":.*/\"dev_cap\":'${dev_cap}',/' kvredund_config.json
recordcnt='200000000'
sed -i 's/recordcount=.*/recordcount='$recordcnt'/' workloads/*_uniform
opcnt='200000000'
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
			# only meta_type 2 for KVMirror KVRaid_Pack
                        if [ "$kv_type" == "2" ] || [ "$kv_type" == "3" ]; then
                                if [ "$m_type" != "2" ]; then
					continue
				fi
                       fi
			# only meta_type 1 for KVEC
			if [ "$kv_type" == "1" ]; then
                                if [ "$m_type" != "1" ]; then
                                        continue
                                fi
                        fi

			sed -i 's/\"meta_type\":.*/\"meta_type\":'${m_type}',/' kvredund_config.json
			for gc_en in $gc_ena
			do
				sed -i 's/\"gc_ena\":.*/\"gc_ena\":'${gc_en}',/' kvredund_config.json

				for testfile in $tests
				do
					result_txt=$result_dir/${testfile}_${kv_type}_${m_type}_${gc_en}_${exp_id}
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
						nvme format /dev/nvme4n1
						nvme format /dev/nvme5n1
						
						sleep 60
						# ycsb load
						/usr/bin/time -v ./bin/ycsb load kvredund -s -P workloads/$testfile -threads $numofthreads -p maxexecutiontime=$MAX_EXE_TIME -jvm-args="-Xms16g -Xmx16g" 1>out.log 2> err.log 
						cp out.log $result_save_dir/load_out_${kv_type}_${m_type}_${gc_en}_${exp_id}.log
						cp err.log $result_save_dir/load_err_${kv_type}_${m_type}_${gc_en}_${exp_id}.log
						cp kv_device.log $result_save_dir/load_dev_${kv_type}_${m_type}_${gc_en}_${exp_id}.log

						echo load >> $result_txt
						printf "load_tp: " >> $result_txt
						sed '/CLEANUP/d' err.log | sed '/0 operations/d' |grep "operations" |awk '{print $7}'|awk '{l[NR]=$1} END{for(i=7; i<=NR-3;i++) {SUM+=l[i];CNT++;} {print SUM/CNT}}' >> $result_txt
						printf "\n" >> $result_txt
						printf "load_lat: " >> $result_txt
						grep "INSERT" out.log | grep "AverageLatency" | awk '{print $3}' >> $result_txt
						#sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/INSERT/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>5)SUM+=$9} END{print SUM/(NR-3)}' >> $result_txt
						printf "\n" >> $result_txt
                                                printf "load_lat_95th: " >> $result_txt
						grep "INSERT" out.log | grep "95thPercentileLatency" | awk '{print $3}' >> $result_txt 
						printf "\n" >> $result_txt
						printf "load_lat_99th: " >> $result_txt
						grep "INSERT" out.log | grep "99thPercentileLatency" | awk '{print $3}' >> $result_txt
						#sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/INSERT/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| sed 's/,/ /g' | awk 'BEGIN {max=0} {if($13>max) max=$13} END {print max}' >> $result_txt
						printf "\n" >> $result_txt
						# report io
						printf "store_ios: " >> $result_txt
						cat kv_device.log|grep ", get"| awk '{ SUM += $2} END { print SUM }' >> $result_txt
						printf "get_ios: " >> $result_txt
						cat kv_device.log|grep ", get"| awk '{ SUM += $4} END { print SUM }' >> $result_txt
						printf "delete_ios: " >> $result_txt
						cat kv_device.log|grep ", get"| awk '{ SUM += $6} END { print SUM }' >> $result_txt
						printf "write_bytes: " >> $result_txt
						cat kv_device.log|grep ", get"| awk '{ SUM += $8} END { print SUM }' >> $result_txt

						# report device usage
						sleep 60 # wait for mapping_table file
						if [ "$m_type" == "2" ]; then
							meta_usage=`ls -lh mapping_table.log | awk '{print substr($5,1,length($5)-1)}'`
						else
							meta_usage=0
						fi
						printf "dev_usage: " >> $result_txt
						data_usage=`cat kv_device.log|grep "usage"| awk '{ SUM += $2} END { printf "%lu", SUM }'`
						echo "($data_usage + $meta_usage)" | bc >> $result_txt
						printf "\n" >> $result_txt

						# report cpu
						cpu_user=`cat err.log | grep "User time" | awk '{print $4}'`
						cpu_sys=`cat err.log | grep "System time" | awk '{print $4}'`
						printf "cpu_time: " >> $result_txt
						echo "($cpu_user+$cpu_sys)"|bc >> $result_txt
						printf "cpu_util: " >> $result_txt
						cat err.log | grep "CPU" | awk '{print $7}' >> $result_txt

						rm -rf kv_device.log err.log out.log
						sleep 180

						# ycsb run 
						retry_cnt=0
						while :
						do
							/usr/bin/time -v ./bin/ycsb run kvredund -s -P workloads/$testfile -threads $numofthreads -p maxexecutiontime=$MAX_EXE_TIME -jvm-args="-Xms16g -Xmx16g"  1>out.log 2> err.log  
							cp out.log $result_save_dir/run_out_${kv_type}_${m_type}_${gc_en}_${exp_id}.log
							cp err.log $result_save_dir/run_err_${kv_type}_${m_type}_${gc_en}_${exp_id}.log
							cp kv_device.log $result_save_dir/run_dev_${kv_type}_${m_type}_${gc_en}_${exp_id}.log

							if test -n "$(find ./ -maxdepth 1 -name 'hs_err*' -print -quit)"
							then
								mv hs_err* $result_save_dir/ 
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
								cp out.log $result_save_dir/
								cp err.log $result_save_dir/
								cat err.log
								mv hs_err* $result_save_dir/
								break
							fi

							echo "run" >> $result_txt
							printf "run_tp: " >> $result_txt
							tp_lines=`sed '/CLEANUP/d' err.log |grep "operations"| wc -l`
							sed '/CLEANUP/d' err.log | sed '/0 operations/d' |grep "operations" |awk '{print $7}'|awk '{l[NR]=$1} END{for(i=7; i<=NR-3;i++) {SUM+=l[i];CNT++;} {print SUM/CNT}}' >> $result_txt
							printf "\n" >> $result_txt
							printf "insert_lat: " >> $result_txt
							grep "INSERT" out.log | grep "AverageLatency" | awk '{print $3}' >> $result_txt
							#sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/INSERT/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>5)SUM+=$9} END{print SUM/(NR-3)}' >> $result_txt
							printf "\n" >> $result_txt
							printf "insert_lat_95th: " >> $result_txt
							grep "INSERT" out.log | grep "95thPercentileLatency" | awk '{print $3}' >> $result_txt
							printf "\n" >> $result_txt
                                                        printf "insert_lat_99th: " >> $result_txt
                                                        grep "INSERT" out.log | grep "99thPercentileLatency" | awk '{print $3}' >> $result_txt
							#sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/INSERT/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| sed 's/,/ /g' | awk 'BEGIN {max=0} {if($13>max) max=$13} END {print max}' >> $result_txt
							printf "\n" >> $result_txt
							printf "update_lat: " >> $result_txt
							grep "UPDATE" out.log | grep "AverageLatency" | awk '{print $3}' >> $result_txt
							#sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/UPDATE/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>5)SUM+=$9} END{print SUM/(NR-3)}' >> $result_txt
							printf "\n" >> $result_txt
							printf "update_lat_95th: " >> $result_txt
							grep "UPDATE" out.log | grep "95thPercentileLatency" | awk '{print $3}' >> $result_txt
                                                        printf "\n" >> $result_txt
                                                        printf "update_lat_99th: " >> $result_txt
                                                        grep "UPDATE" out.log | grep "99thPercentileLatency" | awk '{print $3}' >> $result_txt
							#sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/UPDATE/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| sed 's/,/ /g' | awk 'BEGIN {max=0} {if($13>max) max=$13} END {print max}' >> $result_txt
							printf "\n" >> $result_txt
							printf "read_lat: " >> $result_txt
							grep "\[READ\]" out.log | grep "AverageLatency" | awk '{print $3}' >> $result_txt
							#sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/READ/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>5)SUM+=$9} END{print SUM/(NR-3)}' >> $result_txt
							printf "\n" >> $result_txt
							printf "read_lat_95th: " >> $result_txt
							grep "\[READ\]" out.log | grep "95thPercentileLatency" | awk '{print $3}' >> $result_txt
                                                        printf "\n" >> $result_txt
                                                        printf "read_lat_99th: " >> $result_txt
                                                        grep "\[READ\]" out.log | grep "99thPercentileLatency" | awk '{print $3}' >> $result_txt
							#sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/READ/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| sed 's/,/ /g' | awk 'BEGIN {max=0} {if($13>max) max=$13} END {print max}' >> $result_txt
							printf "\n" >> $result_txt
							printf "scan_lat: " >> $result_txt
							grep "SCAN" out.log | grep "AverageLatency" | awk '{print $3}' >> $result_txt
							#sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/SCAN/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>5)SUM+=$9} END{print SUM/(NR-3)}' >> $result_txt
							printf "\n" >> $result_txt
							printf "scan_lat_95th: " >> $result_txt
							grep "SCAN" out.log | grep "95thPercentileLatency" | awk '{print $3}' >> $result_txt
                                                        printf "\n" >> $result_txt
                                                        printf "scan_lat_99th: " >> $result_txt
                                                        grep "SCAN" out.log | grep "99thPercentileLatency" | awk '{print $3}' >> $result_txt
							#sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/SCAN/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| sed 's/,/ /g' | awk 'BEGIN {max=0} {if($13>max) max=$13} END {print max}' >> $result_txt
							printf "\n" >> $result_txt
							printf "rmw_lat: " >> $result_txt
							grep "READMODIFYWRITE" out.log | grep "AverageLatency" | awk '{print $3}' >> $result_txt
							#sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/READMODIFYWRITE/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| awk '{if(NR>5)SUM+=$9} END{print SUM/(NR-3)}' >> $result_txt
							printf "\n" >> $result_txt
							printf "rmw_lat_95th: " >> $result_txt
							grep "READMODIFYWRITE" out.log | grep "95thPercentileLatency" | awk '{print $3}' >> $result_txt
                                                        printf "\n" >> $result_txt
                                                        printf "rmw_lat_99th: " >> $result_txt
                                                        grep "READMODIFYWRITE" out.log | grep "99thPercentileLatency" | awk '{print $3}' >> $result_txt
							#sed '/CLEANUP/d' err.log |grep "operations" |awk '{flag=0; j=0;for(i=1;i<NF;i++){if ($i~/READMODIFYWRITE/) {flag=1}; if(flag==1) {printf("%s ",$i); if(j++==8) break;}} printf("\n")}'|sed 's/=/ /g'| sed 's/,/ /g' | awk 'BEGIN {max=0} {if($13>max) max=$13} END {print max}' >> $result_txt
							printf "\n" >> $result_txt

							# report io
							printf "store_ios: " >> $result_txt
							cat kv_device.log|grep ", get"| awk '{ SUM += $2} END { print SUM }' >> $result_txt
							printf "get_ios: " >> $result_txt
							cat kv_device.log|grep ", get"| awk '{ SUM += $4} END { print SUM }' >> $result_txt
							printf "delete_ios: " >> $result_txt
							cat kv_device.log|grep ", get"| awk '{ SUM += $6} END { print SUM }' >> $result_txt
							printf "write_bytes: " >> $result_txt
							cat kv_device.log|grep ", get"| awk '{ SUM += $8} END { print SUM }' >> $result_txt
							# report device usage
							sleep 60 # wait for mapping_table file
							#printf "dev_usage: " >> $result_txt
							#cat kv_device.log|grep "usage"| awk '{ SUM += $2} END { print SUM }' >> $result_txt
							if [ "$m_type" == "2" ]; then
								meta_usage=`ls -lh mapping_table.log | awk '{print substr($5,1,length($5)-1)}'`
							else
								meta_usage=0
							fi
							printf "dev_usage: " >> $result_txt
							data_usage=`cat kv_device.log|grep "usage"| awk '{ SUM += $2} END { printf "%lu", SUM }'`
							echo "($data_usage + $meta_usage)" | bc >> $result_txt
							printf "\n" >> $result_txt

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

							# report cpu
							cpu_user=`cat err.log | grep "User time" | awk '{print $4}'`
							cpu_sys=`cat err.log | grep "System time" | awk '{print $4}'`
							printf "cpu_time: " >> $result_txt
							echo "($cpu_user+$cpu_sys)"|bc >> $result_txt
							printf "cpu_util: " >> $result_txt
							cat err.log | grep "CPU" | awk '{print $7}' >> $result_txt

							echo "" >> $result_txt
							rm -rf *.log
							break
						done

						
						sleep 180
						
					done
					sleep 10
				done
				sleep 10
				# no gc for KVEC KVMirror
				if [ "$kv_type" == "2" ] || [ "$kv_type" == "1" ]; then
					break
				fi
			done
			sleep 10
		done
		sleep 10
	done
	sleep 60
done

rm *.log
echo testing completed
