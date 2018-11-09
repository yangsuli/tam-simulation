#!/usr/bin/env python

"""
cassandra_config
"""
import ConfigParser
import ast
import copy

from cassandra_stages import get_all_stage_name_list
from scheduler import get_scheduler
from seda_config import ConfigError


class CassandraConf(object):
    normal_type_str = 'basic'

    def __init__(self):
        self.stg_name_list = get_all_stage_name_list()
        # Note: stg_name_list need to be exactly match the name used in .ini config file, but can add postfix
        self.stg_num_workers_dict = {}
        self.stg_scheduler_generator_dict = {}
        self.stg_schedule_resource_dict = {}
        self.type_name_kw = 'type_name'
        self.normal_type_str = CassandraConf.normal_type_str
        self.stg_type_data_dict = {stg_nm: {self.type_name_kw: self.normal_type_str} for stg_nm in self.stg_name_list}
        self.node_token_sum = 256
        self.pnode_type = 'original'
        self.replication_strategy = 'random'
        self.token_allocation_strategy = 'random'
        self.common_stg_name = 'common'
        self.stg_num_workers_dict[self.common_stg_name] = 0
        self.stg_scheduler_generator_dict[self.common_stg_name] = None
        self.stg_schedule_resource_dict[self.common_stg_name] = None
        self._init_stg_scheduler_dict()
        self.stage_monitor_interval = 1
        self.unified_scheduler_generator = None

    def _init_stg_scheduler_dict(self):
        for stg_name in self.stg_name_list:
            if stg_name not in self.stg_schedule_resource_dict \
                    or self.stg_schedule_resource_dict[stg_name] is None:
                self.stg_schedule_resource_dict[stg_name] = self.stg_schedule_resource_dict[self.common_stg_name]
            if stg_name not in self.stg_scheduler_generator_dict \
                    or self.stg_scheduler_generator_dict[stg_name] is None:
                self.stg_scheduler_generator_dict[stg_name] = self.stg_scheduler_generator_dict[self.common_stg_name]

    def _if_has_stg_name(self, option_str):
        possible_str_list = []
        for stg_name in self.stg_name_list:
            if stg_name in option_str:
                possible_str_list.append(stg_name)
        if len(possible_str_list) > 0:
            return True, max(possible_str_list, key=len)
        return False, ''

    def get_special_stage_constructor_additional_param_dict(self, stg_name):
        type_name = self.stg_type_data_dict[stg_name][self.type_name_kw]
        if type_name == self.normal_type_str:
            return False, self.normal_type_str, {}
        else:
            rt_dict = copy.copy(self.stg_type_data_dict[stg_name])
            del rt_dict[self.type_name_kw]
            return True, type_name, rt_dict

    def get_stg_num_workers(self, stg_name):
        return self.stg_num_workers_dict[stg_name] if stg_name in self.stg_name_list else self.stg_num_workers_dict[
            self.common_stg_name]

    def get_stg_scheduler_generator(self, stg_name):
        return self.stg_scheduler_generator_dict[stg_name] if stg_name in self.stg_name_list else \
            self.stg_scheduler_generator_dict[self.common_stg_name]

    def get_stg_schedule_resource(self, stg_name):
        return self.stg_schedule_resource_dict[stg_name] if stg_name in self.stg_name_list else \
            self.stg_schedule_resource_dict[self.common_stg_name]

    def get_stg_monitor_interval(self):
        return self.stage_monitor_interval

    def get_unified_scheduler_generator(self):
        return self.unified_scheduler_generator

    def is_stage_custom(self, stg_name):
        """Judge a stage of stg_name is customerized or not through configuration instead of using common stage
        :param stg_name:
        :return: if config num_worker, if config new type of scheduler, if config resource to schedule
        """
        return [stg_name in self.stg_num_workers_dict, stg_name in self.stg_scheduler_generator_dict,
                stg_name in self.stg_schedule_resource_dict]

    @staticmethod
    def _get_type_attr_config_map(type_name, stg_name):
        """ Given a type name and the stage name, return the dict that map stage attribute to config keyword
        :return: the dict which key is the name to construct the special stage while value is the keyword to
        retrieve it in the config file
        """
        param_config_dict = {
            'SpeculativeExec':
                {'enforce_res': stg_name + '_type_enforce_res',
                 'speculative_client_id': stg_name + '_type_spec_client_id'},
            CassandraConf.normal_type_str: {},
        }
        if type_name not in param_config_dict.keys():
            raise RuntimeError('stage type not supported:' + str(type_name) + ' support:' + param_config_dict.keys())
        return param_config_dict[type_name]

    def _build_special_stage_param(self, config, cassandra_section):
        """Build the data that need to construct a special type of stage or help it function
        """
        options = config.options(cassandra_section)
        for stg_name, type_data_dict in self.stg_type_data_dict.items():
            cur_attr_config_map = CassandraConf._get_type_attr_config_map(type_data_dict[self.type_name_kw], stg_name)
            for attr, config_kw in cur_attr_config_map.items():
                if config_kw not in options:
                    raise RuntimeError('Not enough info to build special stage:' + stg_name + ' lack ' + str(config_kw))
                cur_val = config.get(cassandra_section, config_kw)
                try:
                    type_data_dict[attr] = ast.literal_eval(cur_val)
                except:
                    type_data_dict[attr] = cur_val

    def load_config(self, config, cassandra_section):
        """Load the configuration of the section describe cassandra system
        Typically, named after [cassandra]
        """
        assert isinstance(config, ConfigParser.ConfigParser)
        assert cassandra_section in config.sections()

        options = config.options(cassandra_section)
        for option in options:
            [has_stg_name, stg_name] = self._if_has_stg_name(option)
            if has_stg_name:
                if 'num_workers' in option:
                    self.stg_num_workers_dict[stg_name] = config.getint(cassandra_section, option)
                elif 'scheduler' in option:
                    print 'customerized scheduler:' + option
                    stg_scheduler_section = config.get(cassandra_section, option)
                    self.stg_scheduler_generator_dict[stg_name] = \
                        lambda env: get_scheduler(env, config, stg_scheduler_section)
                elif 'schedule_resource' in option:
                    self.stg_schedule_resource_dict[stg_name] = config.get(cassandra_section, option)
                elif 'type_name' in option:
                    self.stg_type_data_dict[stg_name] = {self.type_name_kw: config.get(cassandra_section, option)}
                else:
                    print 'Warning: not handle stage config:' + option
            elif option == 'common_stg_scheduler':
                common_stg_scheduler_section = config.get(cassandra_section, option)
                self.stg_scheduler_generator_dict[self.common_stg_name] = \
                    lambda env: get_scheduler(env, config, common_stg_scheduler_section)
                self._init_stg_scheduler_dict()
            elif option == 'common_stg_schedule_resource':
                self.stg_schedule_resource_dict[self.common_stg_name] = config.get(cassandra_section, option)
                self._init_stg_scheduler_dict()
            elif option == 'common_stg_num_workers':
                self.stg_num_workers_dict[self.common_stg_name] = config.getint(cassandra_section, option)
            elif option == 'node_token_sum':
                self.node_token_sum = config.getint(cassandra_section, option)
            elif option == 'pnode_type':
                self.pnode_type = config.get(cassandra_section, option)
            elif option == 'replication_strategy':
                self.replication_strategy = config.get(cassandra_section, option)
            elif option == 'token_allocation_strategy':
                self.token_allocation_strategy = config.get(cassandra_section, option)
            elif option == 'stage_monitor_interval':
                self.stage_monitor_interval = config.getfloat(cassandra_section, option)
            elif option == 'unified_scheduler':
                unified_scheduler_section = config.get(cassandra_section, option)
                self.unified_scheduler_generator = lambda env: get_scheduler(env, config, unified_scheduler_section)
            else:
                raise ConfigError('Unknown option for cassandra:' + option)

            # build config for special stages, at this point, all the name has been read
            self._build_special_stage_param(config, cassandra_section)
