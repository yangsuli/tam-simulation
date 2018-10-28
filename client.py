import abc
import random
import sys

import simpy


class BasicClient(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, env, client_name, client_id, num_instances, think_time, monitor_interval, log_file):
        self.env = env
        self.client_name = client_name
        self.client_id = client_id
        self.num_instances = num_instances
        self.instances = [env.process(self.run()) for i in range(num_instances)]
        self.think_time = think_time
        self.log_file = log_file

        # init the stats data
        self.stats = {}
        self.monitor_interval = monitor_interval
        self.monitor = env.process(self._monitor_run())
        self.num_reqs = 0
        self.total_latency = 0
        self.max_latency = 0
        self.last_report = env.now
        self.last_avg_ltc = 0

    def get_think_time(self):
        return self.think_time

    @abc.abstractmethod
    def run(self):
        """run method of client, may think time, may async"""

    def _calc_stats(self):
        self.stats = {}
        interval = self.env.now - self.last_report
        self.stats["throughput"] = float(self.num_reqs) / interval

        if self.num_reqs == 0:
            self.stats["avg_latency"] = float('NaN')
        else:
            self.stats["avg_latency"] = self.total_latency / self.num_reqs

        self.stats["max_latency"] = self.max_latency
        self.last_avg_ltc = self.stats['avg_latency']

    def _clear_stats(self):
        self.num_reqs = 0
        self.total_latency = 0
        self.max_latency = 0
        self.last_report = self.env.now

    def _log_stats(self):
        self.log_file.write(
            "client_name: %s client_id: %d time: %f throughput: %f avg_latency: %f max_latency: %f\n" % (
                self.client_name, self.client_id, self.env.now, self.stats["throughput"], self.stats["avg_latency"],
                self.stats["max_latency"]))

    def _monitor_run(self):
        while True:
            yield self.env.timeout(self.monitor_interval)
            self._calc_stats()
            self._log_stats()
            self._clear_stats()


class Client(object):
    def __init__(self, env, client_name, client_id, num_instances, new_req_func, think_time, log_file=sys.stdout):
        self.env = env
        self.client_name = client_name
        self.client_id = client_id
        self.new_req_func = new_req_func
        self.think_time = think_time
        self.log_file = log_file
        self.instances = []
        for i in range(num_instances):
            self.instances.append(env.process(self.run()))
        self.monitor_interval = 10
        self.monitor = env.process(self.monitor_run())
        self.stats = {}
        self.num_reqs = 0
        self.total_latency = 0
        self.max_latency = 0
        self.last_report = env.now
        self.last_avg_ltc = 0

    def get_think_time(self):
        return self.think_time

    def run(self):
        while True:
            req = self.new_req_func()
            assert req.client_id == self.client_id

            start = self.env.now
            yield req.submit()
            all_done = simpy.AllOf(self.env, req.all_done_list())
            yield all_done

            latency = self.env.now - start
            self.num_reqs += 1
            self.total_latency += latency
            if latency > self.max_latency:
                self.max_latency = latency

            think_time = self.get_think_time()
            if think_time != 0:
                yield self.env.timeout(think_time)

    def calc_stats(self):
        self.stats = {}
        interval = self.env.now - self.last_report
        self.stats["throughput"] = float(self.num_reqs) / interval

        if self.num_reqs == 0:
            self.stats["avg_latency"] = float('NaN')
        else:
            self.stats["avg_latency"] = self.total_latency / self.num_reqs

        self.stats["max_latency"] = self.max_latency
        self.last_avg_ltc = self.stats['avg_latency']

    def clear_stats(self):
        self.num_reqs = 0
        self.total_latency = 0
        self.max_latency = 0
        self.last_report = self.env.now

    def log_stats(self):
        self.log_file.write(
            "client_name: %s client_id: %d time: %f throughput: %f avg_latency: %f max_latency: %f\n" % (
                self.client_name, self.client_id, self.env.now, self.stats["throughput"], self.stats["avg_latency"],
                self.stats["max_latency"]))

    def monitor_run(self):
        while True:
            yield self.env.timeout(self.monitor_interval)
            self.calc_stats()
            self.log_stats()
            self.clear_stats()


class RandomThinkClient(Client):
    def __init__(self, env, client_name, client_id, num_instances, new_req_func, think_time, log_file=sys.stdout):
        return Client.__init__(self, env, client_name, client_id, num_instances, new_req_func, think_time, log_file)

    def get_think_time(self):
        return random.uniform(self.think_time[0], self.think_time[1])

