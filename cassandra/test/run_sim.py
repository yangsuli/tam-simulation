
import os
import sys

cwd = os.path.abspath(os.path.curdir)
sim_root = cwd + '/../../'
sys.path.append(sim_root)
print sim_root

import simpy
from cassandra.cassandra_client import CassandraClient
from cassandra.cassandra_client import CassandraAsyncClient
from cassandra.cassandra import Cassandra
import ConfigParser

from physical.cluster import PhyClusterConf, PhysicalCluster
from cassandra.cassandra_config import CassandraConf


client_objs = {'sync': CassandraClient, 'async': CassandraAsyncClient}


def main(client_type='sync'):
    print 'client_type is:' + str(client_type)
    if client_type not in client_objs.keys():
        raise RuntimeError('client type not supported ' + client_type)
    client_config_name = 'test_client.ini'
    cassandra_config_name = 'test_sys.ini'
    stg_log = open('stage.log', 'w+')
    client_log = open('client.log', 'w+')
    resource_log = open('resource.log', 'w+')
    env = simpy.Environment()
    sys_config = ConfigParser.ConfigParser()
    sys_config.read(cassandra_config_name)
    client_config = ConfigParser.ConfigParser()
    client_config.read(client_config_name)

    cluster_config = PhyClusterConf()
    cluster_config.load_config(sys_config, 'cluster')
    physical_cluster = PhysicalCluster.get_cluster(env, cluster_config, resource_log)

    cassandra_config = CassandraConf()
    cassandra_config.load_config(sys_config, 'cassandra')

    print 'configs ----------------------- '
    print cluster_config
    print cassandra_config
    cassandra_sys = Cassandra.get_cluster(env, physical_cluster, cassandra_config, stg_log)
    for section in client_config.sections():
        client = client_objs[client_type].get_client(env, cassandra_sys, client_config, section, client_log)
    env.run(until=5)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        main(client_type=sys.argv[1])
    else:
        main()
