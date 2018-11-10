from seda_config import ConfigError
from scheduler import FIFOScheduler, get_scheduler

class DataNodeConf(object):
    def __init__(self):
        self.datanode_type = 'simple'  # defaults to one-stage datanode
        self.datanode_xceive_stage_handlers = 10
        self.datanode_cpu_stage_handlers = 10
        self.datanode_net_stage_handlers = 10
        self.datanode_io_stage_handlers = 10
        self.datanode_xceive_stage_scheduler_generator = None  # defaults to no-scheduling, i.e., on-demand
        # defaults to schedule I/O resources, only works for single resource scheduler
        self.datanode_xceive_stage_schedule_resource = "io"
        self.datanode_cpu_stage_scheduler_generator = lambda env: FIFOScheduler(env, float('inf'))
        self.datanode_net_stage_scheduler_generator = lambda env: FIFOScheduler(env, float('inf'))
        self.datanode_packet_ack_stage_scheduler_generator = None
        self.datanode_packet_ack_stage_schedule_resource = "cpu"
        self.datanode_packet_responders = 10
        self.datanode_io_stage_scheduler_generator = lambda env: FIFOScheduler(env, float('inf'))
        self.stage_monitor_interval = 10  # defaults to 10s monitor interval

    def __str__(self):
        return "DataNodeConf:\n" + \
               "datanode type:" + str(self.datanode_type) + "\n" + \
               "datanode_xceive_stage_handlers:" + str(self.datanode_xceive_stage_handlers) + "\n" + \
               "datanode_cpu_stage_handlers:" + str(self.datanode_cpu_stage_handlers) + "\n" + \
               "datanode_io_stage_handlers:" + str(self.datanode_io_stage_handlers) + "\n" + \
               "datanode_net_stage_handlers:" + str(self.datanode_net_stage_handlers) + "\n" + \
               "datanode_xceive_stage_scheduler_generator:" + str(
            self.datanode_xceive_stage_scheduler_generator) + "\n" + \
               "datanode_cpu_stage_scheduler_generator:" + str(self.datanode_cpu_stage_scheduler_generator) + "\n" + \
               "datanode_io_stage_scheduler_generator:" + str(self.datanode_io_stage_scheduler_generator) + "\n" + \
               "datanode_net_stage_scheduler_generator:" + str(self.datanode_net_stage_scheduler_generator)

    def load_config(self, config, dn_section):
        """

        :param config: ConfigParser object
        :param dn_section: section describing the datanode configuration
        """

        assert dn_section in config.sections()

        options = config.options(dn_section)
        for option in options:
            if option == "datanode_type":
                self.datanode_type = config.get(dn_section, option)
            elif option == "datanode_xceive_stage_handlers":
                self.datanode_xceive_stage_handlers = config.getint(dn_section, option)
            elif option == "datanode_cpu_stage_handlers":
                self.datanode_cpu_stage_handlers = config.getint(dn_section, option)
            elif option == "datanode_net_stage_handlers":
                self.datanode_net_stage_handlers = config.getint(dn_section, option)
            elif option == "datanode_packet_responders":
                self.datanode_packet_responders = config.getint(dn_section, option)
            elif option == "datanode_io_stage_handlers":
                self.datanode_io_stage_handlers = config.getint(dn_section, option)
            elif option == "datanode_xceive_stage_scheduler":
                xceive_sched_section = config.get(dn_section, option)
                if xceive_sched_section == "None":
                    self.datanode_xceive_stage_scheduler_generator = None
                else:
                    self.datanode_xceive_stage_scheduler_generator = lambda env: get_scheduler(env, config,
                                                                                               xceive_sched_section)
            elif option == "datanode_packet_ack_stage_scheduler":
                packet_ack_sched_section = config.get(dn_section, option)
                if packet_ack_sched_section == "None":
                    self.datanode_packet_ack_stage_scheduler_generator = None
                else:
                    self.datanode_packet_ack_stage_scheduler_generator = lambda env: get_scheduler(env, config,
                                                                                                   packet_ack_sched_section)
            elif option == "datanode_xceive_stage_schedule_resource":
                self.datanode_xceive_stage_schedule_resource = config.get(dn_section, option)
            elif option == "datanode_packet_ack_stage_schedule_resource":
                self.datanode_packet_ack_stage_schedule_resource = config.get(dn_section, option)
            elif option == "datanode_cpu_stage_scheduler":
                cpu_stage_sched_section = config.get(dn_section, option)
                self.datanode_cpu_stage_scheduler_generator = lambda env: get_scheduler(env, config,
                                                                                        cpu_stage_sched_section)
            elif option == "datanode_net_stage_scheduler":
                net_stage_sched_section = config.get(dn_section, option)
                self.datanode_net_stage_scheduler_generator = lambda env: get_scheduler(env, config,
                                                                                        net_stage_sched_section)
            elif option == "datanode_io_stage_scheduler":
                io_stage_sched_section = config.get(dn_section, option)
                self.datanode_io_stage_scheduler_generator = lambda env: get_scheduler(env, config,
                                                                                       io_stage_sched_section)
            elif option == "stage_monitor_interval":
                self.stage_monitor_interval = config.getfloat(dn_section, option)
            else:
                print "Warning: unknown option for hdfs:", option


class HDFSConf(object):
    def __init__(self):
        self.namenode_handlers = 10  # defaults to 10 namenode rpc handlers
        self.replica = 3
        self.namenode_scheduler_generator = lambda env: FIFOScheduler(env, float('inf'))  # defaults to FIFO scheduling
        self.datanode_conf = DataNodeConf()  # use default deatanode config

    def __str__(self):
        return "HDFSConf:\n" + \
               "namenode_handlers:" + str(self.namenode_handlers) + "\n" + \
               "namenode_scheduler_generator:" + str(self.namenode_scheduler_generator) + "\n" + \
               "datanode_conf:" + str(self.datanode_conf)

    def load_config(self, config, hdfs_section):
        """

        :param config: ConfigParser object
        :param hdfs_section: section describing the hdfs configuration
        """

        assert hdfs_section in config.sections()

        options = config.options(hdfs_section)
        for option in options:
            if option == "namenode_handlers":
                self.namenode_handlers = config.getint(hdfs_section, option)
            elif option == "replica":
                self.replica = config.getint(hdfs_section, option)
            elif option == "namenode_scheduler":
                namenode_sched_section = config.get(hdfs_section, option)
                if namenode_sched_section == "None":
                    self.namenode_scheduler_generator = None
                else:
                    self.namenode_scheduler_generator = lambda env: get_scheduler(env, config, namenode_sched_section)
            elif option == "datanode":
                datanode_config_section = config.get(hdfs_section, option)
                self.datanode_conf.load_config(config, datanode_config_section)
            else:
                print "Warning: unknown option for hdfs:", option


class RegionServerConf(object):
    def __init__(self):
        self.total_mem_size = 1000  # default: 1G mem
        self.mem_quota = 100  # default 100M mem quota
        self.flush_threshold = 0.5
        self.num_mem_flushers = 2
        self.num_rpc_readers = 10
        self.num_rpc_handlers = 10
        self.num_rpc_responders = 1
        self.rpc_read_scheduler_generator = lambda env: FIFOScheduler(env, float('inf'))
        self.rpc_read_stage_schedule_resource = "net"
        self.rpc_handle_scheduler_generator = lambda env: FIFOScheduler(env, float('inf'))
        self.rpc_handle_stage_schedule_resource = "cpu"
        self.rpc_handle_blocking_latency_lower_bound = None
        self.rpc_handle_blocking_latency_higher_bound = None
        self.rpc_respond_scheduler_generator = lambda env: FIFOScheduler(env, float('inf'))
        self.rpc_respond_stage_schedule_resource = "net"
        self.rpc_unified_scheduler_generator = lambda env: FIFOScheduler(env, float('inf'))
        self.rpc_unified_schedule_resource = "all"
        self.data_stream_scheduler_generator = None
        self.data_stream_stage_schedule_resource = "net"
        self.log_append_scheduler_generator = lambda env: FIFOScheduler(env, float('inf'))
        self.log_append_stage_schedule_resource = "cpu"
        self.num_log_syncers = 5
        self.num_data_streamers = 10
        self.stage_monitor_interval = 10  # defaults to 10 seconds monitor interval

        self.num_cpu_stage_workers = 10
        self.cpu_stage_scheduler_generator = lambda env: FIFOScheduler(env, float('inf'))
        self.num_net_stage_workers = 10
        self.net_stage_scheduler_generator = lambda env: FIFOScheduler(env, float('inf'))
        self.num_io_stage_workers = 10
        self.io_stage_scheduler_generator = lambda env: FIFOScheduler(env, float('inf'))

    def load_config(self, config, rs_section):
        """

        :param config: ConfigParser object
        :param rs_section: section descrbing the region server configuration
        """

        assert rs_section in config.sections()

        options = config.options(rs_section)
        for option in options:
            if option == "num_rpc_readers":
                self.num_rpc_readers = config.getint(rs_section, option)
            elif option == "num_mem_flushers":
                self.num_mem_flushers = config.getint(rs_section, option)
            elif option == "total_mem_size":
                self.total_mem_size = config.getfloat(rs_section, option)
            elif option == "mem_quota":
                self.mem_quota = config.getfloat(rs_section, option)
            elif option == "flush_threshold":
                self.flush_threshold = config.getfloat(rs_section, option)
            elif option == "num_rpc_handlers":
                self.num_rpc_handlers = config.getint(rs_section, option)
            elif option == "num_rpc_responders":
                self.num_rpc_responders = config.getint(rs_section, option)
            elif option == "rpc_read_scheduler":
                rpc_read_sched_section = config.get(rs_section, option)
                self.rpc_read_scheduler_generator = lambda env: get_scheduler(env, config, rpc_read_sched_section)
            elif option == "rpc_read_stage_schedule_resource":
                self.rpc_read_stage_schedule_resource = config.get(rs_section, option)
            elif option == "rpc_handle_scheduler":
                rpc_handle_sched_section = config.get(rs_section, option)
                self.rpc_handle_scheduler_generator = lambda env: get_scheduler(env, config, rpc_handle_sched_section)
            elif option == "rpc_handle_stage_schedule_resource":
                self.rpc_handle_stage_schedule_resource = config.get(rs_section, option)
            elif option == "rpc_handle_blocking_latency_lower_bound":
                self.rpc_handle_blocking_latency_lower_bound = config.getfloat(rs_section, option)
            elif option == "rpc_handle_blocking_latency_higher_bound":
                self.rpc_handle_blocking_latency_higher_bound = config.getfloat(rs_section, option)
            elif option == "rpc_respond_scheduler":
                rpc_respond_sched_section = config.get(rs_section, option)
                self.rpc_respond_scheduler_generator = lambda env: get_scheduler(env, config, rpc_respond_sched_section)
            elif option == "rpc_respond_stage_schedule_resource":
                self.rpc_respond_stage_schedule_resource = config.get(rs_section, option)
            elif option == "rpc_unified_scheduler":
                rpc_unified_sched_section = config.get(rs_section, option)
                self.rpc_unified_scheduler_generator = lambda env: get_scheduler(env, config, rpc_unified_sched_section)
            elif option == "rpc_unified_schedule_resource":
                self.rpc_unified_schedule_resource = config.get(rs_section, option)
            elif option == "stage_monitor_interval":
                self.stage_monitor_interval = config.getfloat(rs_section, option)
            elif option == "data_stream_scheduler":
                data_stream_sched_section = config.get(rs_section, option)
                if data_stream_sched_section == "None":
                    self.data_stream_scheduler_generator = None
                else:
                    self.data_stream_scheduler_generator = lambda env: get_scheduler(env, config,
                                                                                     data_stream_sched_section)
            elif option == "data_stream_stage_schedule_resource":
                self.data_stream_stage_schedule_resource = config.get(rs_section, option)
            elif option == "log_append_scheduler":
                log_append_sched_section = config.get(rs_section, option)
                self.log_append_scheduler_generator = lambda env: get_scheduler(env, config, log_append_sched_section)
            elif option == "log_append_stage_schedule_resource":
                self.log_append_stage_schedule_resource = config.get(rs_section, option)
            elif option == "num_data_streamers":
                self.num_data_streamers = config.getint(rs_section, option)
            elif option == "num_log_syncers":
                self.num_log_syncers = config.getint(rs_section, option)
            elif option == "num_cpu_stage_workers":
                self.num_cpu_stage_workers = config.getint(rs_section, option)
            elif option == "num_net_stage_workers":
                self.num_net_stage_workers = config.getint(rs_section, option)
            elif option == "num_io_stage_workers":
                self.num_io_stage_workers = config.getint(rs_section, option)
            elif option == "cpu_stage_scheduler":
                cpu_stage_sched_section = config.get(rs_section, option)
                self.cpu_stage_scheduler_generator = lambda env: get_scheduler(env, config, cpu_stage_sched_section)
            elif option == "net_stage_scheduler":
                net_stage_sched_section = config.get(rs_section, option)
                self.net_stage_scheduler_generator = lambda env: get_scheduler(env, config, net_stage_sched_section)
            elif option == "io_stage_scheduler":
                io_stage_sched_section = config.get(rs_section, option)
                self.io_stage_scheduler_generator = lambda env: get_scheduler(env, config, io_stage_sched_section)
            else:
                raise ConfigError("unknown region_server option: " + option)


class HBaseConf(object):
    def __init__(self):
        self.rs_type = "original"
        self.rs_conf = RegionServerConf()
        self.replica = None # only used when HBase manages data I/O by itself, without using HDFS

    def load_config(self, config, hbase_section):
        """

        :param config: ConfigParser object
        :param hbase_section: section describing the hbase configuration
        """

        assert hbase_section in config.sections()

        options = config.options(hbase_section)
        for option in options:
            if option == "region_server_config":
                rs_config_section = config.get(hbase_section, option)
                self.rs_conf.load_config(config, rs_config_section)
            elif option == "region_server_type":
                self.rs_type = config.get(hbase_section, option)
            elif option == "replica":
                self.replica = config.getint(hbase_section, option)
            else:
                raise ConfigError("unknown hbase option: " + option)
