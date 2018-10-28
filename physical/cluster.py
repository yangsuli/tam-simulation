import sys

from scheduler import FIFOScheduler, get_scheduler
from seda_config import ConfigError
from seda_resource import Resource


class PhysicalNode(object):
    def __init__(self, env, name, cpu_res, io_res, network_res):
        self.env = env
        self.name = name
        self.cpu_res = cpu_res
        self.io_res = io_res
        self.net_res = network_res

    def get_cpu_res(self):
        return self.cpu_res

    def get_io_res(self):
        return self.io_res

    def get_net_res(self):
        return self.net_res

    @staticmethod
    def get_phy_node(env, name, node_conf, resource_log=sys.stdout):
        cpu_scheduler = node_conf.cpu_scheduler_generator(env)
        cpu_res = Resource(env, name + "_cpu", node_conf.cpu_freq, node_conf.num_cpus, cpu_scheduler,
                           log_file=resource_log)
        cpu_res.monitor_interval = node_conf.resource_monitor_interval
        io_scheduler = node_conf.io_scheduler_generator(env)
        io_res = Resource(env, name + "_io", node_conf.disk_bandwidth, node_conf.num_disks, io_scheduler,
                          log_file=resource_log)
        io_res.monitor_interval = node_conf.resource_monitor_interval
        net_scheduler = node_conf.network_scheduler_generator(env)
        net_res = Resource(env, name + "_network", node_conf.network_bandwidth, node_conf.num_links, net_scheduler,
                           log_file=resource_log)
        net_res.monitor_interval = node_conf.resource_monitor_interval

        return PhysicalNode(env, name, cpu_res, io_res, net_res)

    @staticmethod
    # a physical node that's has only a fraction of its original resources (for isolation experiments)
    def get_part_phy_node(env, fraction, name, node_conf, resource_log=sys.stdout):
        cpu_scheduler = node_conf.cpu_scheduler_generator(env)
        cpu_res = Resource(env, name + "_cpu", node_conf.cpu_freq * fraction, node_conf.num_cpus, cpu_scheduler,
                           log_file=resource_log)
        cpu_res.monitor_interval = node_conf.resource_monitor_interval
        io_scheduler = node_conf.io_scheduler_generator(env)
        io_res = Resource(env, name + "_io", node_conf.disk_bandwidth * fraction, node_conf.num_disks, io_scheduler,
                          log_file=resource_log)
        io_res.monitor_interval = node_conf.resource_monitor_interval
        net_scheduler = node_conf.network_scheduler_generator(env)
        net_res = Resource(env, name + "_network", node_conf.network_bandwidth * fraction, node_conf.num_links,
                           net_scheduler,
                           log_file=resource_log)
        net_res.monitor_interval = node_conf.resource_monitor_interval

        return PhysicalNode(env, name, cpu_res, io_res, net_res)


class PhysicalCluster(object):
    def __init__(self, env, node_list):
        self.env = env
        self.node_list = node_list  # FIXME: --> _node_list, and just call by get_physical_node_list ???

    @staticmethod
    def get_cluster(env, cluster_conf, resource_log=sys.stdout):
        node_list = []
        for i in range(cluster_conf.num_nodes):
            node = PhysicalNode.get_phy_node(env, "node_" + str(i), cluster_conf.node_conf, resource_log)
            node_list.append(node)

        return PhysicalCluster(env, node_list)

    @staticmethod
    def get_part_cluster(env, fraction, cluster_conf, resource_log=sys.stdout):
        node_list = []
        for i in range(cluster_conf.num_nodes):
            node = PhysicalNode.get_part_phy_node(env, fraction, "node_" + str(i), cluster_conf.node_conf, resource_log)
            node_list.append(node)

        return PhysicalCluster(env, node_list)

    def get_physical_node_list(self):
        return self.node_list


class PhyNodeConf(object):
    def __init__(self):
        self.cpu_freq = 1  # defaults to 1: equal to a virtual cpu
        self.num_cpus = 1  # defaults to 1 cpu per node
        self.disk_bandwidth = 100  # defaults to 100 MB/s
        self.num_disks = 1  # defaults to 1 disk per node
        self.network_bandwidth = 100  # defaults to 100 MB/s
        self.num_links = 1  # defaults to 1 network link per node
        self.cpu_scheduler_generator = lambda env: FIFOScheduler(env, float('inf'))
        self.io_scheduler_generator = lambda env: FIFOScheduler(env, float('inf'))
        self.network_scheduler_generator = lambda env: FIFOScheduler(env, float('inf'))
        self.resource_monitor_interval = 10

    def load_config(self, config, node_section):
        """

        :param config: ConfigParser object
        :param node_section: section describing the node configuration
        """
        assert node_section in config.sections()
        options = config.options(node_section)
        for option in options:
            if option == "cpu_freq":
                self.cpu_freq = config.getfloat(node_section, option)
            elif option == "num_cpus":
                self.num_cpus = config.getint(node_section, option)
            elif option == "disk_bandwidth":
                self.disk_bandwidth = config.getfloat(node_section, option)
            elif option == "num_disks":
                self.num_disks = config.getint(node_section, option)
            elif option == "network_bandwidth":
                self.network_bandwidth = config.getfloat(node_section, option)
            elif option == "num_links":
                self.num_links = config.getint(node_section, option)
            elif option == "cpu_scheduler":
                cpu_sched_section = config.get(node_section, option)
                self.cpu_scheduler_generator = lambda env: get_scheduler(env, config, cpu_sched_section)
            elif option == "io_scheduler":
                io_sched_section = config.get(node_section, option)
                self.io_scheduler_generator = lambda env: get_scheduler(env, config, io_sched_section)
            elif option == "network_scheduler":
                net_sched_section = config.get(node_section, option)
                self.network_scheduler_generator = lambda env: get_scheduler(env, config, net_sched_section)
            elif option == "resource_monitor_interval":
                self.resource_monitor_interval = config.getfloat(node_section, option)
            else:
                raise ConfigError("Unknown phy_node option: " + option)


class PhyClusterConf(object):
    def __init__(self):
        self.num_nodes = 1  # default to only 1 node in the cluster
        self.node_conf = PhyNodeConf()

    def load_config(self, config, cluster_section):
        assert cluster_section in config.sections()
        options = config.options(cluster_section)
        for option in options:
            if option == "num_nodes":
                self.num_nodes = config.getint(cluster_section, option)
            elif option == "node_config":
                node_section = config.get(cluster_section, option)
                self.node_conf.load_config(config, node_section)
            else:
                raise ConfigError("Unknown phy_cluster option: " + option)
