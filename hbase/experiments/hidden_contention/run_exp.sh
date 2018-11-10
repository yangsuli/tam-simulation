for client_2_reply_size in 0.01 0.02 0.03 0.04 0.05 0.06 0.07 0.08 0.09 0.1 0.11 0.12 0.13 0.14 0.15 0.16 0.17 0.18 0.19 0.2
do
	for arg in original original_sched resource_stage
	do
	   bash generate_client_ini.sh $client_2_reply_size > client.ini
	   python run_sim.py $arg > detail.log
	   mkdir log_${arg}_${client_2_reply_size}
	   mv *.log log_${arg}_${client_2_reply_size}
	   mv client.ini log_${arg}_${client_2_reply_size}
	done
done
