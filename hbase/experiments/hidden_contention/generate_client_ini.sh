client_2_reply_size=$1

echo [hbase_client_1]
echo client_id = 1
echo client_name = "client_1"
echo num_instances = 1000
echo rpc_size = 1e-3
echo reply_size = 0.02
echo rpc_time = 0
echo mem_update_size = 0
echo log_append_size = 0
echo namenode_lookup_time = 0
echo datanode_read_size = 0
echo short_circuit_ratio = 0.5
echo think_time = 0
echo monitor_interval = 0.01
echo
echo [hbase_client_2]
echo client_id = 2
echo client_name = "client_2"
echo num_instances = 1000
echo rpc_size = 1e-3
echo reply_size = $client_2_reply_size
echo rpc_time = 0
echo mem_update_size = 0
echo log_append_size = 0
echo namenode_lookup_time = 0
echo datanode_read_size = 0
echo short_circuit_ratio = 0.5
echo think_time = 0
echo monitor_interval = 0.01

