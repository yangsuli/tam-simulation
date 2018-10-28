import sys

from scheduler import FIFOScheduler


class ResReq(object):
    def __init__(self, env, client_id, amt, tag=None, priority=0):
        self.client_id = client_id
        self.amt = amt
        self.env = env
        self.tag = tag
        self.priority = priority
        self.done = env.event()
        self.stage = None
        self.start_time = self.env.now

    def cost(self):
        return self.amt


class Resource(object):
    def __init__(self, env, name, rate, parallel, scheduler=None, log_file=sys.stdout):
        """
        :param env: simpy simulation environment
        :param name: name of corresponding resource
        :param rate: performance of resource (in #req/s)
        :param parallel: the sum of resource existing in the env
        :param scheduler: expected scheduler of the resource, can be no-scheduling
        :param log_file: file handle of log file (should have been opened)
        """
        self.env = env
        self.name = name
        self.rate = rate
        self.scheduler = scheduler
        if self.scheduler is None:
            self.scheduler = FIFOScheduler(env, float('inf'))
        self.workers = {}
        for i in range(parallel):
            self.workers[i] = env.process(self.worker_run(i))
        self.log_file = log_file
        self.stats = {}
        self.usage = 0
        self.credit_usage = 0
        self.start_use_time = {}
        self.per_client_usage = {}
        self.waiting = {}
        self.monitor_interval = 1
        self.monitor = env.process(self.monitor_run())
        self.last_report = self.env.now
        self.current_service = {}
        self.per_stage_usage = {}
        self.per_stage_waiting = {}
        self.calc_stage_usage = False
        self.saved_res_stats = None

    def __str__(self):
        return "Resource(name:%s rate:%f parallel:%d)" % (self.name, self.rate, len(self.workers))

    def __repr__(self):
        return self.name

    def get_total_usage(self):
        now_total_usage = self.usage
        now_credit_usage = self.credit_usage
        cur_usage = 0
        for w_id in self.start_use_time:
            if self.start_use_time[w_id] is not None:
                cur_usage += (self.env.now - self.start_use_time[w_id]) * self.rate
        # usage is reset in reset_stats
        self.credit_usage = cur_usage
        return now_total_usage + cur_usage - now_credit_usage

    def request(self, client_id, amt, tag, priority=0):
        resReq = ResReq(self.env, client_id, amt, tag, priority=priority)
        resReq.start_time = self.env.now
        # here we are not waiting until put is done
        # but that is ok because someone will be waiting for req.done
        # at which point most definitely req has been submitted...
        self.scheduler.put(resReq)
        if client_id not in self.waiting:
            self.waiting[client_id] = 0
        if self.calc_stage_usage:
            # add data for per stage queue
            if tag not in self.per_stage_waiting:
                self.per_stage_waiting[tag] = {}
            if client_id not in self.per_stage_waiting[tag]:
                self.per_stage_waiting[tag][client_id] = 0
            self.per_stage_waiting[tag][client_id] += 1
        self.waiting[client_id] += 1
        return resReq.done

    def worker_run(self, w_id):
        while True:
            req = yield self.scheduler.get()
            if req.client_id not in self.current_service:
                self.current_service[req.client_id] = 0
            self.current_service[req.client_id] += 1
            self.start_use_time[w_id] = self.env.now
            yield self.env.timeout(float(req.amt) / self.rate)
            self.start_use_time[w_id] = None
            self.usage += req.amt
            if req.client_id not in self.per_client_usage:
                self.per_client_usage[req.client_id] = 0
            if self.calc_stage_usage:
                if req.tag not in self.per_stage_usage:
                    self.per_stage_usage[req.tag] = {}
                if req.client_id not in self.per_stage_usage[req.tag]:
                    self.per_stage_usage[req.tag][req.client_id] = 0
                self.per_stage_usage[req.tag][req.client_id] += req.amt
                self.per_stage_waiting[req.tag][req.client_id] -= 1
            self.current_service[req.client_id] -= 1
            self.per_client_usage[req.client_id] += req.amt
            self.waiting[req.client_id] -= 1
            req.done.succeed()

    def get_res_req_time(self, amt):
        return float(amt) / self.rate

    def calc_stats(self):
        self.stats = {}
        interval = self.env.now - self.last_report
        self.stats["utilization"] = float(self.get_total_usage()) / (self.rate * len(self.workers)) / interval
        self.stats["occupancy"] = self.scheduler.get_occupancy()
        per_client_utilization = {}
        for client_id in self.per_client_usage:
            per_client_utilization[client_id] = float(self.per_client_usage[client_id]) / (
                    self.rate * len(self.workers)) / interval
        self.stats["per_client_utilization"] = per_client_utilization
        self.saved_res_stats = self.stats

    def clear_stats(self):
        self.usage = 0
        self.per_client_usage = {}
        self.last_report = self.env.now
        if self.calc_stage_usage:
            self.per_stage_usage = {}

    def monitor_run(self):
        while True:
            yield self.env.timeout(self.monitor_interval)
            self.calc_stats()
            self.log_file.write(
                "resource: %s time: %f occupancy: %f utilization: %f waiting: %s per_client_utilization %s\n" % (
                    self.name, self.env.now, self.stats["occupancy"], self.stats["utilization"], str(self.waiting),
                    str(self.stats["per_client_utilization"])))

            if self.calc_stage_usage:
                print 'resource:' + str(self.name) + ' per_stage_usage:' + str(
                    self.per_stage_usage) + ' per_stage_waiting:' + str(self.per_stage_waiting)
            self.clear_stats()
