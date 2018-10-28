import simpy

from client import Client
from scheduler import WFQScheduler, DRFScheduler
from seda_resource import Resource
from stage import Stage, OnDemandStage
from stage_req import StageReq


class TwoStageServer(object):
    """
    A two stage server, where stage2 suffers the no scheduling problem
    """

    def __init__(self, env, num_workers, resource_log, stage_log):
        res1 = Resource(env, "r1", 1, 1, log_file=resource_log)
        res1.monitor_interval = 0.1
        scheduler = DRFScheduler(env, float('inf'), 2)
        stage1 = Stage(env, "s1", num_workers, scheduler, req_cost_func=lambda req: self.get_req_cost(req),
                       log_file=stage_log)
        stage1.monitor_interval = 0.1

        res2 = Resource(env, "r2", 100, 1, log_file=resource_log)
        res2.monitor_interval = 0.1
        stage2 = OnDemandStage(env, "s2", log_file=stage_log)
        stage2.monitor_interval = 0.1

        self.env = env
        self.stage1 = stage1
        self.stage2 = stage2
        self.res1 = res1
        self.res2 = res2

    def get_two_stage_req(self, client_id, r1_amt, r2_amt):
        if r2_amt == 0:
            s1_req = StageReq(self.env, self.stage1, client_id, {self.res1: r1_amt, self.res2: 0}, [], [])
            return s1_req, s1_req.done

        s2_req = StageReq(self.env, self.stage2, client_id, {self.res2: r2_amt, self.res1: 0}, [], [])
        s1_req = StageReq(self.env, self.stage1, client_id, {self.res1: r1_amt, self.res2: 0}, [], [])
        s1_req.downstream_reqs.append(s2_req)

        return s1_req, s2_req.done

    def get_req_cost(self, req):
        if len(req.downstream_reqs) == 0:
            return [req.resource_profile[self.res1] / (self.res1.rate * len(self.res1.workers)),
                    req.resource_profile[self.res2] / (self.res2.rate * len(self.res2.workers))]

        assert len(req.downstream_reqs) == 1
        resource_profile = req.resource_profile.copy()
        d_req = req.downstream_reqs[0]
        resource_profile[self.res1] += d_req.resource_profile[self.res1]
        resource_profile[self.res2] += d_req.resource_profile[self.res2]
        return [resource_profile[self.res1] / (self.res1.rate * len(self.res1.workers)),
                resource_profile[self.res2] / (self.res2.rate * len(self.res2.workers))]


class SchedTwoStageSever(object):
    """
    A two stage server, where both stages are regulated by schedulers
    """

    def __init__(self, env, num_workers, resource_log, stage_log):
        res1 = Resource(env, "r1", 1, 1, log_file=resource_log)
        res1.monitor_interval = 0.1
        scheduler = WFQScheduler(env, float('inf'))
        stage1 = Stage(env, "s1", num_workers, scheduler, req_cost_func=lambda req: req.resource_profile[res1],
                       log_file=stage_log)
        stage1.monitor_interval = 0.1

        res2 = Resource(env, "r2", 100, 1, log_file=resource_log)
        res2.monitor_interval = 0.1
        scheduler = WFQScheduler(env, float('inf'))
        stage2 = Stage(env, "s2", 1, scheduler, req_cost_func=lambda req: req.resource_profile[res2],
                       log_file=stage_log)
        stage2.monitor_interval = 0.1

        self.env = env
        self.stage1 = stage1
        self.stage2 = stage2
        self.res1 = res1
        self.res2 = res2

    def get_two_stage_req(self, client_id, r1_amt, r2_amt):
        if r2_amt == 0:
            s1_req = StageReq(self.env, self.stage1, client_id, {self.res1: r1_amt, self.res2: 0}, [], [])
            return s1_req, s1_req.done

        s1_req = StageReq(self.env, self.stage1, client_id, {self.res1: r1_amt, self.res2: 0}, [], [])
        s2_req = StageReq(self.env, self.stage2, client_id, {self.res2: r2_amt, self.res1: 0}, [], [])
        s1_req.downstream_reqs.append(s2_req)

        return s1_req, s2_req.done


class TwoStageClient(Client):
    def __init__(self, env, server, name, client_id, num_instances, r1_amt, r2_amt, think_time, log_file):
        self.env = env
        self.server = server
        self.client_name = name
        self.client_id = client_id
        self.r1_amt = r1_amt
        self.r2_amt = r2_amt
        self.think_time = think_time
        self.instances = []
        for i in range(num_instances):
            self.instances.append(env.process(self.run()))
        self.monitor_interval = 1
        self.monitor = env.process(self.monitor_run())
        self.num_reqs = 0
        self.total_latency = 0
        self.max_latency = 0
        self.last_report = self.env.now
        self.log_file = log_file

    def run(self):
        while True:
            start = self.env.now

            req, done_evt = self.server.get_two_stage_req(self.client_id, self.r1_amt, self.r2_amt)
            yield req.submit()
            yield done_evt

            latency = self.env.now - start
            self.num_reqs += 1
            self.total_latency += latency

            if self.think_time > 0:
                yield self.env.timeout(self.think_time)


def run_sim(arg, c1_threads, c2_threads):
    env = simpy.Environment()

    resource_log = open("resource_" + arg + "_" + str(c1_threads) + "_" + str(c2_threads) + ".log", "w+")
    stage_log = open("stage_" + arg + "_" + str(c1_threads) + "_" + str(c2_threads) + ".log", "w+")
    client_log = open("client_" + arg + "_" + str(c1_threads) + "_" + str(c2_threads) + ".log", "w+")

    if arg == "ondemand":
        server = TwoStageServer(env, 10, resource_log, stage_log)
    elif arg == "sched":
        server = SchedTwoStageSever(env, 10, resource_log, stage_log)
    else:
        raise Exception("unknown arg " + arg)

    client_1 = TwoStageClient(env, server, "c1", 1, c1_threads, 0, 0.1, 0.01, client_log)
    client_2 = TwoStageClient(env, server, "c2", 2, c2_threads, 0, 0.1, 0.01, client_log)

    env.run(until=20)


c1_threads = 10
c2_threads_list = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
for arg in ["ondemand", "sched"]:
    for c2_threads in c2_threads_list:
        run_sim(arg, c1_threads, c2_threads)
