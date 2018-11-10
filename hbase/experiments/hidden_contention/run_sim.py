import sys
import os
import simpy
import ConfigParser


from physical.cluster import PhysicalCluster, PhyClusterConf
from hbase.hdfs import HDFS
from hbase.hbase import HBase, OneStagePerResHBase, HBaseClient
from hbase.hbase_config import HDFSConf, HBaseConf

from stage_req import StageReq

if __name__ == '__main__':
    exp_arg = sys.argv[1]

    config_file = exp_arg + ".ini"
    client_config_file = "client.ini"
    stage_log_file = open("stage.log", "w+")
    resource_log_file = open("resource.log", "w+")
    client_log_file = open("client.log", "w+")

    env = simpy.Environment()
    config = ConfigParser.ConfigParser()
    config.read(config_file)
    client_config = ConfigParser.ConfigParser()
    client_config.read(client_config_file)


    cluster_conf = PhyClusterConf()
    cluster_conf.load_config(config, "cluster")
    phy_cluster = PhysicalCluster.get_cluster(env, cluster_conf, resource_log_file)
    if exp_arg != "resource_stage":
    	hdfs_conf = HDFSConf()
    	hdfs_conf.load_config(config, "hdfs")
    	hdfs = HDFS.get_cluster(env, phy_cluster, hdfs_conf, resource_log=resource_log_file, stage_log=stage_log_file)

    hbase_conf = HBaseConf()
    hbase_conf.load_config(config, "hbase")
    if exp_arg == "resource_stage":
    	hbase = OneStagePerResHBase.get_cluster(env, phy_cluster, hbase_conf, resource_log=resource_log_file, stage_log=stage_log_file)
    else:
    	hbase = HBase.get_cluster(env, phy_cluster, hdfs, hbase_conf, resource_log=resource_log_file, stage_log=stage_log_file)


    client_config = ConfigParser.ConfigParser()
    client_config.read(client_config_file)
    clients = HBaseClient.get_clients(env, hbase, client_config, client_log_file)

    env.run(until=1)
