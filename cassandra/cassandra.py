#!/usr/bin/env python

"""
cassandra
"""
import abc
import copy
import random

from cassandra_config import CassandraConf
from cassandra_stages import get_all_stage_name_list
from cassandra_stages import guess_stage_name
from stage import Stage
from stage_req import BlkStageReq
from stage_req import ConditionalStageReq
from stage_req import ResBlockingProfile
from stage_req import StageReq
from util.consistent_hash_util import ConsistentHash
from util.schedule_util import ScheduleUtil


###################################################################################################################
# classes to help build the ring, assign token, that is, implementation of policies for clusters building
###################################################################################################################


class TokenAllocationStrategy(object):
    """Interface to control How token is mapped to physical node
    Default is random to all the nodes, but can be manully assigned for some strong machine
    And has better strategy (?) since 3.0 (https://www.datastax.com/dev/blog/token-allocation-algorithm)
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, vn_list, pn_list):
        self.vn_list = vn_list
        self.pn_list = pn_list
        self.vn_pn_idx_map = {}
        self.pn_vn_idx_map = {}
        self.pn_sum = len(self.pn_list)
        self.vn_sum = len(self.vn_list)

    @staticmethod
    def create_token_allocation_strategy(typ, vn_list, pn_list):
        return token_allocation_strategy_objs[typ](vn_list, pn_list)

    def _get_pre_vn_id(self, vn_id):
        return (vn_id - 1 + self.vn_sum) % self.vn_sum

    def _get_suf_vn_id(self, vn_id):
        return (vn_id + 1 + self.vn_sum) % self.vn_sum

    def _update_assign_map(self, cur_vn_id, cur_pn_id):
        assert cur_vn_id not in self.vn_pn_idx_map.keys()  # one token should be just assigned once
        self.vn_pn_idx_map[cur_vn_id] = cur_pn_id
        if cur_pn_id not in self.pn_vn_idx_map.keys():
            self.pn_vn_idx_map[cur_pn_id] = []
        self.pn_vn_idx_map[cur_pn_id].append(cur_vn_id)

    def _assign_vn_to_pn(self, cur_vn, cur_pn):
        cur_pn.add_vnode(cur_vn)
        cur_vn.assign_to_pn(cur_pn)
        self._update_assign_map(cur_vn.get_vid(), cur_pn.get_pid())

    @abc.abstractmethod
    def assign_vn_to_pn(self):
        """
        return:[self.vn_pn_idx_map, self.pn_vn_idx_map]
        """
        return


class RandomTokenAllocationStrategy(TokenAllocationStrategy):
    def assign_vn_to_pn(self):
        """Randomly allocate all the tokens to physical nodes
        """
        vn_list_idx_list = range(self.vn_sum)
        random.shuffle(vn_list_idx_list)
        pn_list_idx = 0
        for vn_list_idx in vn_list_idx_list:
            cur_vn = self.vn_list[vn_list_idx]
            cur_pn = self.pn_list[pn_list_idx]
            self._assign_vn_to_pn(cur_vn, cur_pn)
            pn_list_idx = (pn_list_idx + 1) % self.pn_sum
        return [self.vn_pn_idx_map, self.pn_vn_idx_map]


class InOrderTokenAllocationStrategy(TokenAllocationStrategy):
    def assign_vn_to_pn(self):
        """Assign the token in order
        That is, 0 to pn-0, 1 to pn-1, 2 to pn-2, as the circle goes
        """
        for i in range(self.vn_sum):
            cur_vn = self.vn_list[i]
            cur_pn = self.pn_list[(i % self.pn_sum)]
            self._assign_vn_to_pn(cur_vn, cur_pn)
        return [self.vn_pn_idx_map, self.pn_vn_idx_map]


class RandomFaultToleranceTokenAllocationStrategy(TokenAllocationStrategy):
    def assign_vn_to_pn(self):
        """Randomly allocation all the tokens, but force the successive to be in different nodes for fault tolerance
        """
        for i in range(len(self.vn_list)):
            avoid_pn_id_list = []
            if self._get_pre_vn_id(i) in self.vn_pn_idx_map.keys():
                avoid_pn_id_list.append(self.vn_pn_idx_map[self._get_pre_vn_id(i)])
            if self._get_suf_vn_id(i) in self.vn_pn_idx_map.keys():
                avoid_pn_id_list.append(self.vn_pn_idx_map[self._get_suf_vn_id(i)])
            remain_pn_id_list = list(set(range(self.pn_sum)) - set(avoid_pn_id_list))
            cur_pn_id = random.choice(remain_pn_id_list)
            cur_vn = self.vn_list[i]
            cur_pn = self.pn_list[cur_pn_id]
            self._assign_vn_to_pn(cur_vn, cur_pn)
        return [self.vn_pn_idx_map, self.pn_vn_idx_map]


class ReplicationStrategy(object):
    """An abstract parent class to provide the vnode and pnode of given key
    """
    __metaclass__ = abc.ABCMeta

    @staticmethod
    def create_replication_strategy(typ, ring_sz, **kwargs):
        return replication_strategy_objs[typ](ring_sz, **kwargs)

    def __init__(self, ring_sz):
        self.ring_sz = ring_sz

    @abc.abstractmethod
    def get_vn_id(self, key, access_vn_id_list=None):
        """"""
        return


class RandomReplicationStrategy(ReplicationStrategy):
    """Just random choose a physical node to access
    For this strategy, the access_key of CassandraReq does not make sense, no matter the access_key is generated
    from which distribution
    """

    def __init__(self, ring_sz):
        super(RandomReplicationStrategy, self).__init__(ring_sz)

    def get_vn_id(self, key, access_vn_id_list=None):
        if access_vn_id_list is None:
            access_vn_id_list = range(self.ring_sz)
        return access_vn_id_list[random.randint(0, len(access_vn_id_list) - 1)]


class ConsistentHashReplicationStrategy(ReplicationStrategy):
    """Perform consistent hashing to access_key to find which virtual node to access
    Depends on the access_key of each CassandraReq object
    """

    def __init__(self, ring_sz):
        super(ConsistentHashReplicationStrategy, self).__init__(ring_sz)
        self.ch = ConsistentHash(ring_sz, 1)

    def get_vn_id(self, key, access_vn_id_list=None):
        vn_id = self.ch.get_machine(key)
        return vn_id


class FixReplicationStrategy(ReplicationStrategy):
    """Only access single vnode, for hot-key workload
    """

    def __init__(self, ring_sz, vn_id):
        super(FixReplicationStrategy, self).__init__(ring_sz)
        self.fix_vn_id = vn_id

    def get_vn_id(self, key, access_vn_id_list=None):
        return self.fix_vn_id


###################################################################################################################
# Several types of Cassandra Physical Node
###################################################################################################################

class BaseCassandraPhysicalNode(object):
    """Original Physical Node
    Mainly deal with stages
    Attr: bare_metal: instance of physical.cluster.PhysicalNode, deal with resource
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, env, cassandra_sys, pn_id, pn_name, physical_node, token_sum=256):
        self.env = env
        self.cassandra_sys = cassandra_sys
        self.pn_id = pn_id
        self.pn_name = pn_name
        self.token_sum = token_sum
        self.vnode_dict = {}
        self.bare_metal = physical_node
        # init all the stages to None
        self.__dict__.update({key: None for key in Cassandra.get_all_stage_name_list()})

    def init_stages(self, cassandra_conf, stage_log):
        assert isinstance(cassandra_conf, CassandraConf)
        stg_name_list = Cassandra.get_all_stage_name_list()
        for stg_name in stg_name_list:
            cur_num_workers = cassandra_conf.get_stg_num_workers(stg_name)
            cur_scheduler_generator = cassandra_conf.get_stg_scheduler_generator(stg_name)
            cur_scheduler = cur_scheduler_generator(self.env)
            cur_schedule_resource = cassandra_conf.get_stg_schedule_resource(stg_name)
            cur_req_cost_func = self.get_req_cost_func(cur_schedule_resource)
            [is_cur_stage_special, cur_stage_type_name, cur_stage_type_data_dict] = \
                cassandra_conf.get_special_stage_constructor_additional_param_dict(stg_name)
            cur_stage_name = self.pn_name + '_' + stg_name
            if not is_cur_stage_special:
                cur_stage = Stage(self.env, cur_stage_name, cur_num_workers, cur_scheduler,
                                  cur_req_cost_func, log_file=stage_log)
            else:
                print 'stage:' + stg_name + ' is a ' + str(cur_stage_type_name) + 'scheduler:' + str(cur_scheduler)
                cur_stage_kwargs_dict = self._get_basic_stage_init_dict(cur_stage_name, cur_num_workers, cur_scheduler,
                                                                        cur_req_cost_func, stage_log)
                cur_stage_kwargs_dict.update(cur_stage_type_data_dict)
                self._transform_resource_str_to_real_obj(cur_stage_kwargs_dict)
                cur_stage = Stage.create_special_stage(cur_stage_type_name, cur_stage_kwargs_dict)

            cur_stage.set_monitor_interval(cassandra_conf.get_stg_monitor_interval())
            self.__dict__.update({stg_name: cur_stage})

    def _transform_resource_str_to_real_obj(self, data_dict):
        need_phy_res_param_list = ['enforce_res']
        for key, val in data_dict.items():
            if key in need_phy_res_param_list:
                data_dict[key] = self.get_res_by_kw(val)

    def _get_basic_stage_init_dict(self, name, num_workers, cur_scheduler, cost_func, log_file):
        return {'env': self.env, 'name': name, 'num_workers': num_workers, 'req_queue': cur_scheduler,
                'req_cost_func': cost_func, 'log_file': log_file}

    def get_stage(self, stg_name):
        """From name of stage get the stage object of this physical node
        """
        if stg_name not in self.__dict__:
            raise AttributeError
        return self.__dict__[stg_name]

    def add_vnode(self, vnode):
        self.vnode_dict[vnode.get_vid()] = vnode

    def get_vnode(self, vn_id):
        assert vn_id in self.vnode_dict.keys()
        return self.vnode_dict[vn_id]

    def is_vnode_local(self, vn_id):
        return vn_id in self.vnode_dict.keys()

    def get_pid(self):
        return self.pn_id

    def get_name(self):
        return self.pn_name

    def get_res_by_kw(self, kw):
        if kw == 'io':
            return self.get_io_res()
        elif kw == 'net':
            return self.get_net_res()
        elif kw == 'cpu':
            return self.get_cpu_res()
        else:
            raise RuntimeError('resource cannot found for keyword:' + kw)

    def get_cpu_res(self):
        return self.bare_metal.get_cpu_res()

    def get_io_res(self):
        return self.bare_metal.get_io_res()

    def get_net_res(self):
        return self.bare_metal.get_net_res()

    def get_req_cost_func(self, schedule_resource):
        return ScheduleUtil.get_cost_func(schedule_resource, self.bare_metal)

    @abc.abstractmethod
    def submit_req(self, cassandra_req, vn_id):
        """Main control point of how stage interact with each other
        need to be implemented by each subclass
        """
        return

    @staticmethod
    def is_valid_physical_node_type(typ):
        return typ in physical_node_objs


class OriginalCassandraPhysicalNode(BaseCassandraPhysicalNode):
    def __init__(self, env, cassandra_sys, pn_id, pn_name, physical_node, token_sum=256):
        super(OriginalCassandraPhysicalNode, self).__init__(env, cassandra_sys, pn_id, pn_name, physical_node,
                                                            token_sum=token_sum)

    def submit_req(self, cassandra_req, vn_id):
        """Simulate the request flow of original cassandra system
        """
        return self.submit_req_impl(cassandra_req, vn_id)

    def submit_req_impl(self, cassandra_req, vn_id, is_async_sys=False):
        submit_time = self.env.now
        c_req_handle_res_cost_list = [
            {self.get_cpu_res(): cassandra_req.get_stg_cpu_time(guess_stage_name('c_req_handle')),
             self.get_net_res(): cassandra_req.req_sz}]
        c_req_handle_res_profile = ResBlockingProfile(c_req_handle_res_cost_list)
        c_req_handle_req = BlkStageReq(self.env, self.get_stage(guess_stage_name('c_req_handle')),
                                       cassandra_req.client_id)
        c_req_handle_req.append_res_blk_profile(c_req_handle_res_profile)

        c_respond_res_dict = {self.get_cpu_res(): cassandra_req.get_stg_cpu_time(guess_stage_name('c_respond')),
                              self.get_net_res(): cassandra_req.reply_sz}
        c_respond_req = StageReq(
            self.env, self.get_stage(guess_stage_name('c_respond')), cassandra_req.client_id, c_respond_res_dict, [],
            [])
        access_vn_sum = cassandra_req.consistency_access_sum
        access_vn_id_list = self.cassandra_sys.compute_vn_id_list(vn_id, access_vn_sum)
        # blk_evt_list = []
        request_response_req = ConditionalStageReq(self.env, self.get_stage(guess_stage_name('request_response')),
                                                   cassandra_req.client_id, {}, [], [], condition_func=None)
        if not is_async_sys:
            # Real Original Version that c-req-handle stage block on req-response stage
            c_req_handle_res_profile.add_blocking_event(request_response_req.done)
            # the 2nd pharse of c-rea-handle stage, downstream stage is c-req-respond
            c_req_handle_callback_res_profile = ResBlockingProfile([])
            c_req_handle_req.append_res_blk_profile(c_req_handle_callback_res_profile)
            c_req_handle_callback_res_profile.add_downstream_req(c_respond_req)
        else:
            # For async version system, request_response_req directly send req to respond
            request_response_req.add_downstream_req(c_respond_req)
        ############################################################################################
        # For Conditional Stage, set the succeed condition
        request_response_req.total_vn_sum = access_vn_sum
        request_response_req.current_vn_sum = 0
        request_response_req.unsatisfy_res_profile = {
            self.get_cpu_res(): cassandra_req.get_stg_cpu_time(guess_stage_name('request_response'))}
        request_response_req.satisfy_res_profile = {
            self.get_cpu_res(): cassandra_req.get_stg_cpu_time(guess_stage_name('request_response'))}
        request_response_req.can_be_done = False
        request_response_req.cur_client = cassandra_req.client
        request_response_req.client_submit_time = submit_time

        def get_conditional_res_profile(tmp_request_response_req):
            # callback to get the resource consumption of each submission of this request
            tmp_request_response_req.current_vn_sum += 1
            assert tmp_request_response_req.current_vn_sum <= tmp_request_response_req.total_vn_sum
            if tmp_request_response_req.current_vn_sum == tmp_request_response_req.total_vn_sum:
                tmp_request_response_req.can_be_done = True
                return tmp_request_response_req, tmp_request_response_req.satisfy_res_profile
            else:
                no_downstream_tmp_req = copy.copy(tmp_request_response_req)
                no_downstream_tmp_req.reset_downstream_req()
                no_downstream_tmp_req.reset_blocking_event()
                return no_downstream_tmp_req, tmp_request_response_req.unsatisfy_res_profile

        request_response_req.condition_func = get_conditional_res_profile
        #############################################################################################
        for i in range(access_vn_sum):
            # access all the replication followed consistency level
            cur_vn_id = access_vn_id_list[i]
            proc_cpu_usage = 0

            for stg_name in cassandra_req.proc_stg_name_list:
                proc_cpu_usage += cassandra_req.get_stg_cpu_time(guess_stage_name(stg_name))
            if self.is_vnode_local(cur_vn_id):
                # local vnode, no network usage
                proc_res_dict = {self.get_cpu_res(): proc_cpu_usage,
                                 self.get_io_res(): (cassandra_req.read_sz + cassandra_req.write_sz)}
                # now just use the first proc stage to charge all resource
                proc_req = StageReq(self.env, self.get_stage(guess_stage_name(cassandra_req.proc_stg_name_list[0])),
                                    cassandra_req.client_id, proc_res_dict, [], [])
                c_req_handle_res_profile.add_downstream_req(proc_req)
                proc_req.add_downstream_req(request_response_req)
            else:
                cur_pn = self.cassandra_sys.get_host_by_vn(cur_vn_id)
                # msg_out, msg_in is from the view of the first access node
                msg_out_send_res_cost_dict = {
                    self.get_cpu_res(): cassandra_req.get_stg_cpu_time(guess_stage_name('msg_out')),
                    self.get_net_res(): cassandra_req.msg_pass_req_sz}
                msg_out_send_req = StageReq(self.env, self.get_stage(guess_stage_name('msg_out')),
                                            cassandra_req.client_id,
                                            msg_out_send_res_cost_dict, [], [])
                msg_out_recv_res_cost_dict = {
                    cur_pn.get_cpu_res(): cassandra_req.get_stg_cpu_time(guess_stage_name('msg_out')),
                    cur_pn.get_net_res(): cassandra_req.msg_pass_req_sz}
                msg_out_recv_req = StageReq(cur_pn.env, cur_pn.get_stage(guess_stage_name('msg_in')),
                                            cassandra_req.client_id,
                                            msg_out_recv_res_cost_dict, [], [])
                msg_out_send_req.add_downstream_req(msg_out_recv_req)
                c_req_handle_res_profile.add_downstream_req(msg_out_send_req)
                proc_res_dict = {cur_pn.get_cpu_res(): proc_cpu_usage,
                                 cur_pn.get_io_res(): (cassandra_req.read_sz + cassandra_req.write_sz)}
                proc_req = StageReq(cur_pn.env, cur_pn.get_stage(guess_stage_name(cassandra_req.proc_stg_name_list[0])),
                                    cassandra_req.client_id, proc_res_dict, [], [])
                msg_out_recv_req.add_downstream_req(proc_req)
                msg_in_send_res_cost_dict = {
                    cur_pn.get_cpu_res(): cassandra_req.get_stg_cpu_time(guess_stage_name('msg_in')),
                    cur_pn.get_net_res(): cassandra_req.msg_pass_reply_sz}
                msg_in_send_req = StageReq(cur_pn.env, cur_pn.get_stage(guess_stage_name('msg_out')),
                                           cassandra_req.client_id,
                                           msg_in_send_res_cost_dict, [], [])
                proc_req.add_downstream_req(msg_in_send_req)
                msg_in_recv_res_cost_dict = {
                    self.get_cpu_res(): cassandra_req.get_stg_cpu_time(guess_stage_name('msg_in')),
                    self.get_net_res(): cassandra_req.msg_pass_req_sz}
                msg_in_recv_req = StageReq(self.env, self.get_stage(guess_stage_name('msg_in')),
                                           cassandra_req.client_id,
                                           msg_in_recv_res_cost_dict, [], [])
                msg_in_send_req.add_downstream_req(msg_in_recv_req)
                msg_in_recv_req.add_downstream_req(request_response_req)

        return c_req_handle_req.submit(), c_respond_req.done


class NoBlockCassandraPhysicalNode(OriginalCassandraPhysicalNode):
    """C-Req-handle stage will not block until response notify its completion
    """

    def __init__(self, env, cassandra_sys, pn_id, pn_name, physical_node, token_sum=256):
        super(NoBlockCassandraPhysicalNode, self).__init__(env, cassandra_sys, pn_id, pn_name, physical_node,
                                                           token_sum=token_sum)

    def submit_req(self, cassandra_req, vn_id):
        return self.submit_req_impl(cassandra_req, vn_id, is_async_sys=True)


class UnifiedCassandraPhysicalNode(OriginalCassandraPhysicalNode):
    """Unified schedule all types of resource, each in one queue and c-req blocked (blocking not fixed)
    """

    def submit_req(self, cassandra_req, vn_id):
        raise NotImplementedError


class UnifiedNoblockCassandraPhysicalNode(NoBlockCassandraPhysicalNode):
    """Unified schedule all types of resource, each in one queue and c-req not block for request-response stage
    """

    def __init__(self, env, cassandra_sys, pn_id, pn_name, physical_node, token_sum=256):
        super(UnifiedNoblockCassandraPhysicalNode, self).__init__(env, cassandra_sys, pn_id, pn_name, physical_node,
                                                                  token_sum=token_sum)

    def init_stages(self, cassandra_conf, stage_log):
        super(UnifiedNoblockCassandraPhysicalNode, self).init_stages(cassandra_conf, stage_log)
        # update all stages use one stage scheduler
        stg_name_list = Cassandra.get_all_stage_name_list()
        unified_scheduler_generator = cassandra_conf.get_unified_scheduler_generator()
        unified_scheduler = unified_scheduler_generator(self.env)
        assert unified_scheduler_generator is not None
        for stg_name in stg_name_list:
            cur_stage = self.__dict__[stg_name]
            cur_stage.update_scheduler(unified_scheduler)


class CassandraPhysicalNodeFactory(object):
    @staticmethod
    def create_physical_node(env, cassandra_sys, typ, pn_id, pn_name, physical_node, token_sum=256):
        return physical_node_objs[typ](env, cassandra_sys, pn_id, pn_name, physical_node, token_sum)


class CassandraVirtualNode(object):
    """Virtual Node
    In Cassandra, each virtual node corresponds to a token, and by default a physical node has 256 tokens
    """

    def __init__(self, vn_id, vn_name):
        self.vn_id = vn_id
        self.vn_name = vn_name
        self.pn = None

    def assign_to_pn(self, pn):
        self.pn = pn

    def get_pid(self):
        return self.pn.get_pid()

    def get_vid(self):
        return self.vn_id

    def get_name(self):
        return self.vn_name


###################################################################################################################
# helper variables for factories to find the right class object
###################################################################################################################
physical_node_objs = {
    'original': OriginalCassandraPhysicalNode,
    'async': NoBlockCassandraPhysicalNode,
    'unified': UnifiedCassandraPhysicalNode,
    'unified_async': UnifiedNoblockCassandraPhysicalNode}
replication_strategy_objs = {
    'random': RandomReplicationStrategy, 'consistent_hash': ConsistentHashReplicationStrategy}
token_allocation_strategy_objs = {
    'random': RandomTokenAllocationStrategy,
    'in_order': InOrderTokenAllocationStrategy,
    'random_ft': RandomFaultToleranceTokenAllocationStrategy}


###################################################################################################################


class CassandraReq(object):
    """Request client issues to cassandra system
    the interface between CassandraClient and Cassandra, initialized by client
    """

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.access_key = 1

    def set_access_key(self, access_key):
        self.access_key = access_key

    def get_access_key(self):
        return self.access_key

    def get_stg_cpu_time(self, stg_name):
        assert hasattr(self, 'per_stg_cpu_time_dict')
        assert stg_name in self.per_stg_cpu_time_dict.keys()
        return self.per_stg_cpu_time_dict[stg_name]


###################################################################################################################
# Cassandra System
###################################################################################################################

class Cassandra(object):
    """Cassandra System
    """

    def __init__(self, env, cassandra_conf, stage_log):
        self.version = '3.0'
        self.env = env
        self.cassandra_conf = cassandra_conf
        self.stage_log = stage_log
        self.pn_list = []
        self.pn_dict = {}
        self.vn_list = []
        self.vn_dict = {}
        self.vn_pn_id_map = {}
        self.pn_vn_id_map = {}
        self.replication_strategy = None
        self.token_allocation_strategy = None
        self.total_token_sum = 0
        self.per_client_instance_access_vn_list_dict = {}

    @staticmethod
    def get_cluster(env, phy_cluster, cassandra_conf, stage_log):
        cassandra_sys = Cassandra(env, cassandra_conf, stage_log)
        bare_metal_list = phy_cluster.get_physical_node_list()
        # initialize cassandra physical node list
        cassandra_sys._build_cassandra_physical_node_list(
            env, cassandra_conf.pnode_type, bare_metal_list, cassandra_conf.node_token_sum)
        # now all the physical nodes are assumed to be the same, and thus has the same # of tokens
        total_token_sum = len(bare_metal_list) * cassandra_conf.node_token_sum
        arg_dict = {}  # if has some special need that the constructor need more params
        replication_strategy = ReplicationStrategy.create_replication_strategy(
            cassandra_conf.replication_strategy, total_token_sum, **arg_dict)
        cassandra_sys.replication_strategy = replication_strategy
        cassandra_sys.total_token_sum = total_token_sum
        # initialize vnode list
        cassandra_sys._build_vnode_list()
        token_allocation_strategy = TokenAllocationStrategy.create_token_allocation_strategy(
            cassandra_conf.token_allocation_strategy, cassandra_sys.vn_list, cassandra_sys.pn_list)
        # assign vnode to physical node
        [cassandra_sys.vn_pn_id_map, cassandra_sys.pn_vn_id_map] = token_allocation_strategy.assign_vn_to_pn()
        # cassandra_sys._print_per_pn_token_sum()
        return cassandra_sys

    def compute_vn_id_list(self, start_vn_id, access_sum):
        vn_id_list = []
        for i in range(access_sum):
            vn_id_list.append((start_vn_id + i) % self.total_token_sum)
        return vn_id_list

    def submit_req(self, cassandra_req):
        kwargs = {'key': str(cassandra_req.get_access_key()), 'access_vn_id_list': cassandra_req.access_vn_list}
        cur_vn_id = self.replication_strategy.get_vn_id(**kwargs)
        cur_pn_id = self.vn_pn_id_map[cur_vn_id]
        return self.pn_dict[cur_pn_id].submit_req(cassandra_req, cur_vn_id)

    def get_host_by_pn(self, pn_id):
        return self.pn_dict[pn_id]

    def get_host_by_vn(self, vn_id):
        return self.pn_dict[self.vn_pn_id_map[vn_id]]

    def _build_cassandra_physical_node_list(self, env, typ, bare_metal_list, token_sum=256):
        for i in range(len(bare_metal_list)):
            pn = CassandraPhysicalNodeFactory.create_physical_node(env, self, typ, i, 'node_' + str(i),
                                                                   bare_metal_list[i],
                                                                   token_sum=token_sum)
            self.pn_list.append(pn)
            self.pn_dict[i] = pn
            pn.init_stages(self.cassandra_conf, self.stage_log)

    def _build_vnode_list(self):
        assert self.total_token_sum != 0
        for i in range(self.total_token_sum):
            cur_vn = CassandraVirtualNode(i, 'vnode_' + str(i))
            self.vn_list.append(cur_vn)
            self.vn_dict[i] = cur_vn

    def _print_per_pn_token_sum(self):
        for pn_id in self.pn_vn_id_map.keys():
            print 'pn_id:' + str(pn_id) + ' has ' + str(len(self.pn_vn_id_map[pn_id]))

    @staticmethod
    def get_all_stage_name_list():
        return get_all_stage_name_list()

    @staticmethod
    def is_valid_stage_name(name):
        # return any(name in stg_name_str for stg_name_str in Cassandra.get_all_stage_name_list())
        # need to be exactly the name of stages
        return name in Cassandra.get_all_stage_name_list()


class CassandraStub(object):
    def __init__(self, env):
        self.env = env

    @staticmethod
    def get_cluster(env, phy_cluster, cassandra_conf, stage_log, resource_log):
        print 'get_cluster_stub for test'
