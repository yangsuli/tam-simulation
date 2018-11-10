import copy
import sys

import simpy

from scheduler import LimitInFlight,  DRFScheduler
from scheduler import is_unified_scheduler


class Stage(object):
    """
    Stage processes StageReq (and its variants).
    It contains a group of workers that continuously pick up requests from its req_queue and call process() on
    these requests
    """

    def __init__(self, env, name, num_workers, req_queue, req_cost_func=None, log_file=sys.stdout):
        self.env = env
        self.name = name
        self.req_queue = req_queue
        self.req_cost_func = req_cost_func
        self.log_file = log_file
        self.workers = {}
        for i in range(num_workers):
            self.workers[i] = env.process(self.worker_run(i))
        self.monitor_interval = 10
        self.monitor = self.env.process(self.monitor_run())
        self.last_report = self.env.now
        self.current_blocking = {}
        self.current_service = {}
        self.start_service_time = {}
        self.start_blocking_time = {}
        self._total_blocking_time = 0
        self._total_service_time = 0
        self._credit_blocking_time = 0
        self._credit_service_time = 0
        self.cur_w_id = num_workers
        self.num_workers = num_workers
        self.debug = False

    def update_scheduler(self, req_queue):
        print 'update scheduler to:' + str(req_queue)
        self.req_queue = req_queue

    def get_total_blocking_time(self):
        _total_blocking_time = self._total_blocking_time
        _credit_blocking_time = self._credit_blocking_time
        in_blocking_time = 0
        for w_id in self.start_blocking_time:
            if self.start_blocking_time[w_id] is not None:
                in_blocking_time += self.env.now - self.start_blocking_time[w_id]

        self._total_blocking_time = 0
        self._credit_blocking_time = in_blocking_time
        return _total_blocking_time + in_blocking_time - _credit_blocking_time

    def get_total_service_time(self):
        _total_service_time = self._total_service_time
        _credit_service_time = self._credit_service_time
        in_service_time = 0
        for w_id in self.start_service_time:
            if self.start_service_time[w_id] is not None:
                in_service_time += self.env.now - self.start_service_time[w_id]

        self._total_service_time = 0
        self._credit_service_time = in_service_time
        return _total_service_time + in_service_time - _credit_service_time

    def put_req(self, req):
        """
        put request to the stage's req_queue, where it is subjected to scheduling

        :param req: StageReq
        :return: an event the indicating the put; only after the event succeeded (can wait on it by yield event)
                 is the put completed
        """
        if self.req_cost_func is not None:
            req._cost = self.req_cost_func(req)
        handler = self.req_queue.put(req)
        return handler

    def start_blocking(self, w_id, req):
        client_id = req.client_id
        if client_id not in self.current_blocking:
            self.current_blocking[client_id] = 0
        self.current_blocking[client_id] += 1
        self.start_blocking_time[w_id] = self.env.now

    def finish_blocking(self, w_id, req):
        client_id = req.client_id
        assert client_id in self.current_blocking
        self.current_blocking[client_id] -= 1
        self._total_blocking_time += self.env.now - self.start_blocking_time[w_id]
        self.start_blocking_time[w_id] = None

    def start_service(self, w_id, req):
        client_id = req.client_id
        if client_id not in self.current_service:
            self.current_service[client_id] = 0
        self.current_service[client_id] += 1
        self.start_service_time[w_id] = self.env.now
        if self.debug:
            print "time", self.env.now, "stage", self.name, "start service req for client_id", req.client_id,\
                "now service threads:", self.current_service  # ,' req:',str(req)

    def finish_service(self, w_id, req):
        req.service_time = self.env.now - self.start_service_time[w_id]
        client_id = req.client_id
        assert client_id in self.current_service
        self.current_service[client_id] -= 1
        self._total_service_time += self.env.now - self.start_service_time[w_id]
        self.start_service_time[w_id] = None
        if self.debug:
            print "time", self.env.now, "stage", self.name, "finish service req for client_id", req.client_id, \
                "now service threads:", self.current_service  # , 'req:',str(req)

    def worker_run(self, w_id):
        while True:
            if w_id not in self.workers:
                self.env.exit()
            if is_unified_scheduler(self.req_queue):
                # pass stage as a parameter if the scheduler may be shared by different stages
                # so that the scheduler can issue a request for our stage
                req = yield self.req_queue.get(self)
            else:
                req = yield self.req_queue.get()
            assert req.stage == self
            self.start_service(w_id, req)
            yield self.env.process(req.process(w_id))
            self.finish_service(w_id, req)
            self.req_queue.complete_one_req(req)

    def add_workers(self, num_workers):
        old_length = len(self.workers)
        for i in range(num_workers):
            w_id = self.cur_w_id
            self.workers[w_id] = self.env.process(self.worker_run(w_id))
            self.cur_w_id += 1
        assert len(self.workers) == old_length + num_workers

    def remove_workers(self, num_workers):
        # we do not allow number of workers to drop below 1
        assert num_workers < len(self.workers)
        old_length = len(self.workers)

        key_list = self.workers.keys()
        for i in range(num_workers):
            w_id = key_list[i]
            del self.workers[w_id]
        assert len(self.workers) == old_length - num_workers

    def calc_stats(self):
        # noinspection PyAttributeOutsideInit
        self.stats = {}
        interval = self.env.now - self.last_report
        avai_time = interval * len(self.workers)
        service_time = self.get_total_service_time()
        blocking_time = self.get_total_blocking_time()
        utilization = service_time / float(avai_time)
        real_utilization = (service_time - blocking_time) / float(avai_time)

        self.stats["utilization"] = utilization
        self.stats["real_utilization"] = real_utilization
        self.stats["occupancy"] = self.req_queue.get_occupancy()

    def clear_stats(self):
        self.last_report = self.env.now

    def log_stats(self):
        self.log_file.write(
            "stage: %s time: %f utilization: %f real_utilization: %f service_threads: %s blocking_threads: %s scheduler: [%s] occupancy:%f\n" % (
                self.name, self.env.now, self.stats["utilization"], self.stats["real_utilization"],
                str(self.current_service), str(self.current_blocking), str(self.req_queue), self.stats["occupancy"]))

    def monitor_run(self):
        while True:
            yield self.env.timeout(self.monitor_interval)
            self.calc_stats()
            self.log_stats()
            self.clear_stats()

    def set_monitor_interval(self, interval):
        self.monitor_interval = interval

    @staticmethod
    def create_special_stage(stage_type, kwarg_dict):
        return special_stage_objs[stage_type](**kwarg_dict)


class AfterCostStage(Stage):
    """ calculate cost after req processing, need to work with scheduler which accounts cost after processing """

    def put_req(self, req):
        handler = self.req_queue.put(req)
        return handler

    def finish_service(self, w_id, req):
        Stage.finish_service(self, w_id, req)
        if self.req_cost_func is not None:
            req._cost = self.req_cost_func(req)


class ActiveWorkerStage(Stage):
    """
    Instead of fixed number of workers
    maintain the number of active workers, if some worker goes blocking, start another one
    an attempt to solve the blocking problem
    """

    def __init__(self, env, name, num_active_workers, req_queue, req_cost_func=None, log_file=sys.stdout):
        Stage.__init__(self, env, name, num_active_workers, req_queue, req_cost_func, log_file)

    def start_blocking(self, w_id, req):
        Stage.start_blocking(self, w_id, req)
        self.add_workers(1)

    def finish_blocking(self, w_id, req):
        Stage.finish_blocking(self, w_id, req)
        self.remove_workers(1)


class OnDemandStage(object):
    """ The Stage which spawn a new process once a request come """

    def __init__(self, env, name, log_file=sys.stdout):
        self.env = env
        self.name = name
        self.workers = []
        self.log_file = log_file
        self.monitor_interval = 10
        self.monitor = self.env.process(self.monitor_run())
        self.last_report = self.env.now
        self.num_completed_reqs = 0
        self.num_accepted_reqs = 0

    def worker_done(self, process):
        self.workers.remove(process)
        self.num_completed_reqs += 1

    def put_req(self, req):
        self.num_accepted_reqs += 1
        worker = self.env.process(req.process())
        self.workers.append(worker)
        worker.callbacks.append(self.worker_done)
        return simpy.Event(self.env).succeed()

    def calc_stats(self):
        # noinspection PyAttributeOutsideInit
        self.stats = {}
        interval = self.env.now - self.last_report
        self.stats["complete_rate"] = float(self.num_completed_reqs) / interval
        self.stats["accept_rate"] = float(self.num_accepted_reqs) / interval

    def clear_stats(self):
        self.last_report = self.env.now
        self.num_accepted_reqs = 0
        self.num_completed_reqs = 0

    def log_stats(self):
        self.log_file.write("stage: %s time: %f accept_rate: %f complete_rate: %f num_workers %d\n" % (
            self.name, self.env.now, self.stats["accept_rate"], self.stats["complete_rate"], len(self.workers)))

    def monitor_run(self):
        while True:
            yield self.env.timeout(self.monitor_interval)
            self.calc_stats()
            self.log_stats()
            self.clear_stats()

    def set_monitor_interval(self, interval):
        self.monitor_interval = interval


class SpeculativeExecStage(Stage):
    """
    In addition to normal workers that process requests normally,
    SpeculativeExecStage has a set of speculative workers that speculatively execute requests from a particular client
    (identified by speculative_client_id).
    If during the speculative execution, the request is found to require resource it should not access (named in
    enforce_res), the execution will be aborted and the request put back to the req_queue.

    It is useful when we want to serve some requests to utilize the currently idle resources but do not allow these
    requests to consume resources currently in contention.

    """

    def __init__(self, env, name, enforce_res, speculative_client_id, num_workers, req_queue, req_cost_func=None,
                 log_file=sys.stdout):
        Stage.__init__(self, env, name, num_workers, req_queue, req_cost_func, log_file)
        self.enforce_res = enforce_res
        self.speculative_client_id = speculative_client_id
        self.speculative_workers = {}
        for i in range(len(self.workers), len(self.workers) + 5):
            print i
            self.speculative_workers[i] = env.process(self.speculative_worker_run(i))

    def speculative_worker_run(self, w_id):
        while True:
            if w_id not in self.speculative_workers:
                self.env.exit()

            assert isinstance(self.req_queue, DRFScheduler)
            req = self.req_queue.pick_req_from_client_id(self.speculative_client_id)
            if req == None:
                print "time", self.env.now, "speculative worker sleep 1ms"
                # no req available, sleep 1ms and retry
                yield self.env.timeout(0.001)
                continue
            assert req.stage == self
            self.start_service(w_id, req)
            status = yield self.env.process(req.process_no_res(w_id, self.enforce_res))
            if status == 0:  # execution completed
                print "time", self.env.now, "speculative complete req", req
                self.req_queue.complete_one_req(req)
            elif status == -1:  # abort because used enforce_res
                print "time", self.env.now, "speculative put req back", req
                req.put_back = True
                yield self.put_req(req)
            else:
                raise Exception("unknown speculative process status")
            self.finish_service(w_id, req)


class ExactResourceExecStage(SpeculativeExecStage):
    """
    similar to SpeculativeExecStage, but instead of speculating, we assume that we just know the resource usage
    beforehand
    """

    def speculative_worker_run(self, w_id):
        while True:
            if w_id not in self.speculative_workers:
                self.env.exit()

            assert isinstance(self.req_queue, DRFScheduler)
            req = self.req_queue.pick_req_from_client_id(self.speculative_client_id)
            if req == None:
                print "time", self.env.now, "speculative worker sleep 1ms"
                # no req available, sleep 1ms and retry
                yield self.env.timeout(0.001)
                continue

            # just put back req if it uses resources we enforce
            if self.enforce_res in req.resource_profile:
                req.put_back = True
                yield self.put_req(req)
                continue

            assert req.stage == self
            self.start_service(w_id, req)
            status = yield self.env.process(req.process_no_res(w_id, self.enforce_res))
            if status == 0:  # execution completed
                print "time", self.env.now, "speculative complete req", req
                self.req_queue.complete_one_req(req)
            elif status == -1:  # abort because used enforce_res
                print "time", self.env.now, "speculative put req back", req
                req.put_back = True
                yield self.put_req(req)
            else:
                raise Exception("unknown speculative process status")
            self.finish_service(w_id, req)


#########################################################################################################
# To help build special stages
# Note: contract here: the keyword should be each stage's class name but cut the 'Stage'
#########################################################################################################
special_stage_objs = {'SpeculativeExec': SpeculativeExecStage}
