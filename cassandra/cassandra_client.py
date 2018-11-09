#!/usr/bin/env python

"""
cassandra_client
simulate the client-api and behavior of clients
"""
import ConfigParser
import abc
import ast
import math
import random
import sys

from cassandra import Cassandra
from cassandra import CassandraReq
from client import BasicClient
from seda_config import ConfigError
from util.key_generate_util import KeyGenerator


class LoadBalancePolicy(object):
    """ The policy that decides which Cassandra hosts to contact for each new query
    All known Impl: DCAwareRoundRobinPolicy, LatencyAwarePolicy, RoundRobinPolicy, TokenAwarePolicy, WhiteListPolicy
    """
    __metaclass__ = abc.ABCMeta

    @staticmethod
    def create_load_balance_policy(typ, access_pn_list, start_pn_list_index=0):
        return load_balance_policy_objs[typ](access_pn_list, start_pn_list_index=start_pn_list_index)

    @abc.abstractmethod
    def new_query_plan(self):
        """Returns the hosts to use for a new query
        Each new query will call this method. The first host in the result will then be used to perform the query.
        In the event of a connection problem (the queried host is down or appear to be so), the next host will be used.
        If all hosts of the returned Iterator are down, the query will fail.
        """
        return

    @abc.abstractmethod
    def host_distance(self, host):
        """ Returns the distance assigned by this policy to the provided host.
        """
        return


class RoundRobinPolicy(LoadBalancePolicy):
    """A Round-robin load balancing policy.
    This policy queries nodes in a round-robin fashion. We do not consider failure of host now.
    """

    def __init__(self, access_pn_list, start_pn_list_index=0):
        """
        :param access_pn_list:
        :param start_host_index: index of list access_pn_list to start access (not pn-id)
        """
        assert len(access_pn_list) > 0
        self.access_pn_list = access_pn_list
        self.start_pn_list_index = start_pn_list_index
        self.cur_list_index = self.start_pn_list_index

    def new_query_plan(self):
        rt_pn_id = self.access_pn_list[self.cur_list_index]
        self.cur_list_index = (self.cur_list_index + 1) % len(self.access_pn_list)
        return rt_pn_id

    def host_distance(self, host):
        return 1


load_balance_policy_objs = {'round_robin': RoundRobinPolicy}


###################################################################################################################
# Cassandra Client
###################################################################################################################


class CassandraClient(BasicClient):
    def __init__(self, env, cassandra_sys, client_name, client_id, num_instances, think_time, monitor_interval,
                 # basic attr
                 req_sz, reply_sz, msg_pass_req_sz, msg_pass_reply_sz,  # network size
                 read_size, write_size,  # io size
                 replication_factor, write_consistency, read_consitency, work_consistency,  # consistency level
                 access_pn_list, access_vn_list, common_stg_cpu_time, proc_stg_name_list, key_distribution, key_args,
                 load_balance_policy='round_robin',
                 log_file=sys.stdout):
        super(CassandraClient, self).__init__(env, client_name, client_id, num_instances, think_time, monitor_interval,
                                              log_file)
        self.cassandra_sys = cassandra_sys
        self.req_sz = req_sz
        self.reply_sz = reply_sz
        self.msg_pass_req_sz = msg_pass_req_sz
        self.msg_pass_reply_sz = msg_pass_reply_sz
        self.read_sz = read_size
        self.write_sz = write_size
        self.replication_factor = replication_factor
        self.write_consistency = write_consistency
        self.read_consistency = read_consitency
        self.work_consistency = work_consistency
        self.consistency_access_sum = self.get_consistency_access_sum()
        self.access_pn_list = access_pn_list
        self.access_vn_list = access_vn_list
        self.proc_stg_name_list = proc_stg_name_list
        self.per_stg_cpu_time_dict = {key: common_stg_cpu_time for key in Cassandra.get_all_stage_name_list()}
        self._cassandra_req_attr_list = \
            ['env', 'client_id', 'req_sz', 'reply_sz', 'msg_pass_req_sz', 'msg_pass_reply_sz', 'read_sz', 'write_sz',
             'replication_factor', 'write_consistency', 'read_consistency', 'consistency_access_sum', 'access_vn_list',
             'access_pn_list', 'per_stg_cpu_time_dict', 'proc_stg_name_list']

        self.load_balance_policy = load_balance_policy
        self.load_balancer = LoadBalancePolicy.create_load_balance_policy(self.load_balance_policy, access_pn_list)

        self.key_distribution = key_distribution
        self.key_generator = KeyGenerator.create_key_generator(self.key_distribution, key_args)

        # self.think_time = 0.01

    def update_cpu_time(self, stg_name, cpu_time):
        self.per_stg_cpu_time_dict[stg_name] = cpu_time

    def _get_cassandra_req_kargs(self):
        cur_kargs = {key: self.__dict__[key] for key in self._cassandra_req_attr_list}
        cur_kargs['client'] = self
        return cur_kargs

    def get_consistency_access_sum(self):
        # TODO: now use quorum, but needs to follow the configuration
        # Note: need to force to return int
        return int(math.ceil(float(self.replication_factor) / 2))

    def generate_cassandra_req(self):
        req = CassandraReq(**self._get_cassandra_req_kargs())
        cur_access_key = self.key_generator.next()
        req.set_access_key(cur_access_key)
        return req

    def run(self):
        while True:
            # generate request to submit
            # cassandra_req = CassandraReq(**self._get_cassandra_req_kargs())
            # cur_access_key = self.key_generator.next()
            # cassandra_req.set_access_key(cur_access_key)
            cassandra_req = self.generate_cassandra_req()

            # submit request to cassandra sys
            start = self.env.now
            submit_evt, done_evt = self.cassandra_sys.submit_req(cassandra_req)
            yield submit_evt
            yield done_evt

            # For monitor statistics
            latency = self.env.now - start
            self.num_reqs += 1
            self.total_latency += latency
            if latency > self.max_latency:
                self.max_latency = latency

            # client end behavior
            think_time = self.get_think_time()
            if think_time != 0:
                yield self.env.timeout(think_time)

    @staticmethod
    def get_client(env, cassandra_sys, config, section, client_log=sys.stdout):
        assert section in config.sections()
        try:
            # basic attr
            client_id = config.getint(section, 'client_id')
            client_name = config.get(section, 'client_name')
            num_instances = config.getint(section, 'num_instances')
            think_time = config.getfloat(section, 'think_time')
            monitor_interval = config.getfloat(section, 'monitor_interval')
            # network usage
            req_sz = config.getfloat(section, 'req_sz')
            reply_sz = config.getfloat(section, 'reply_sz')
            msg_pass_req_sz = config.getfloat(section, 'msg_pass_req_sz')
            msg_pass_reply_sz = config.getfloat(section, 'msg_pass_reply_sz')
            # io usage
            read_sz = config.getfloat(section, 'read_sz')
            write_sz = config.getfloat(section, 'write_sz')
            # consistency level
            r_factor = config.getint(section, 'replication_factor')
            w_consistency = config.get(section, 'write_consistency')
            r_consistency = config.get(section, 'read_consistency')
            work_consistency = config.get(section, config.get(section, 'work_consistency'))
            # access list
            access_pn_list = ast.literal_eval(config.get(section, 'access_pn_list'))
            access_vn_list = ast.literal_eval(config.get(section, 'access_vn_list'))
            # access pattern
            load_balance_policy = config.get(section, 'load_balance_policy')
            # key distribution
            key_distribution = config.get(section, 'key_distribution')
            if 'key_distribution_param_list' in config.options(section):
                key_args = ast.literal_eval(config.get(section, 'key_distribution_param_list'))
                print 'key_distribution_param_list:' + str(key_args)
            else:
                key_args = [sys.maxint]

            # key_args = [sys.maxint]  # TODO: read args from config file, now use default
            # cpu time
            common_stg_cpu_time = config.getfloat(section, 'common_stg_cpu_time')
            # proc stages
            proc_stg_name_list = ast.literal_eval(config.get(section, 'proc_stg_name_list'))

        except ConfigParser.NoOptionError as error:
            raise ConfigError("client " + error.section + " configuration not complete, no option: " + error.option)
        client = CassandraClient(env, cassandra_sys, client_name, client_id, num_instances, think_time,
                                 monitor_interval,
                                 req_sz, reply_sz, msg_pass_req_sz, msg_pass_reply_sz,
                                 read_sz, write_sz,
                                 r_factor, w_consistency, r_consistency, work_consistency,
                                 access_pn_list, access_vn_list, common_stg_cpu_time,
                                 proc_stg_name_list, key_distribution, key_args,
                                 load_balance_policy=load_balance_policy, log_file=client_log)

        # handle optional configuration
        options = config.options(section)
        for option in options:
            if 'cpu_time' in option and 'common' not in option:
                cur_stg_name = option.replace('_cpu_time', '')
                assert Cassandra.is_valid_stage_name(cur_stg_name)
                client.update_cpu_time(cur_stg_name, config.getfloat(section, option))

        return client


class CassandraAsyncClient(BasicClient):
    def __init__(self, env, client):
        super(CassandraAsyncClient, self).__init__(
            env, client.client_name, client.client_id, client.num_instances, client.think_time, client.monitor_interval,
            log_file=client.log_file)
        self.shadow_sync_client = client
        self.start_time_dict = {}
        self.shadow_sync_client.monitor_interval = sys.maxint

    @staticmethod
    def get_client(env, cassandra_sys, config, section, client_log=sys.stdout):
        close_client = CassandraClient.get_client(env, cassandra_sys, config, section, client_log=client_log)
        return CassandraAsyncClient(env, close_client)

    def done_callback(self, evt):
        # print 'callback called'
        # throughput
        self.num_reqs += 1
        # latency
        start_time = self.start_time_dict[evt]
        latency = self.env.now - start_time
        self.total_latency += latency
        if latency > self.max_latency:
            self.max_latency = latency

    def run(self):
        while True:
            # generate request to submit
            cassandra_req = self.shadow_sync_client.generate_cassandra_req()

            # submit request to cassandra sys
            start = self.env.now
            submit_evt, done_evt = self.shadow_sync_client.cassandra_sys.submit_req(cassandra_req)
            yield submit_evt
            self.start_time_dict[done_evt] = start
            done_evt.callbacks.append(self.done_callback)

            # client end behavior
            think_time = self.get_think_time()
            if think_time == 0:
                think_time = 0.05
                # print 'think_time should not be 0 for async client , set to ' + str(think_time)
            yield self.env.timeout(random.random() * think_time)
