import ConfigParser
import Queue
import ast
import math
import random
from threading import Lock

import simpy
from simpy.core import BoundClass
from simpy.resources.base import BaseResource

'''
This file defines a set of schedulers (FIFO, WFQ, DRF, etc.) that different stages can use.

You can also write your only scheduler; before doing so, please make sure you know how simpy works
and have read the simpy.resources implementation carefully, as TAM scheduler implementation are based on
simpy.resources.

Something to note when writing a new scheduler

1. By default, BaseResource in simpy only calls get() when new items are added
   so for token-bucket scheduler and alike, where get() could return additional items
   not only because new item is added, but also because more tokens are available
   the scheduler is responsible to call _trigger_get() on every occasion where get() could return more items
   (e.g., after refilling tokens)

2. if _do_get() returns False, simpy assumes there is no more items left in the queue
   and will not perform any more gets until new items are added
   However, for some schedulers, get() returns nothing for one client does not mean
   other clients cannot get more requests processed (e.g., because they still have tokens left)
   so _do_get() should return True whenever *some* clients can still get requests from the queue,
   not only when this particular invocation of _do_get() returns a request

'''


class StageGet(simpy.resources.store.StoreGet):
    # Get a request for whose stage matches stage
    def __init__(self, store, stage):
        self.stage = stage
        super(StageGet, self).__init__(store)


class OccupancyCalc(object):
    """
    A helper class for calculating the occupancy of the scheduler queue.
    To use this, a scheduler class should inherit this class, properly initialize the following fields:
        self.env: to the simulation environment
        self.currentInStore: to 0
        self.queueStartFill: to None
        self.occupiedTime: to 0
        self.credit_time: to 0
        self.last_report_time: to env.now

    and call occupancy_calc_add_one_to_queue whenever an item is added to the scheduler's request queue
    and call occupancy_calc_take_one_frome queue whenever an item is removed from the scheduler's request queue

    """

    def __init__(self):
        """
        This constructor is never intended to be called
        It is expected that all classes inheriting from OccupancyCalc initialize these fields in their own constructor
        """

        self.env = None
        self.currentInStore = None
        self.queueStartFill = None
        self.occupiedTime = None

    def get_occupancy(self):
        interval = self.env.now - self.last_report_time
        if interval == 0:
            return float('NaN')

        occupiedTime = self.occupiedTime
        credit_time = self.credit_time
        in_process_time = 0
        if self.queueStartFill is not None:
            in_process_time = self.env.now - self.queueStartFill

        self.occupiedTime = 0
        self.credit_time = in_process_time
        self.last_report_time = self.env.now

        occupancy = float(occupiedTime + in_process_time - credit_time) / interval

        return occupancy

    def occupancy_calc_add_one_to_queue(self):
        """
        to track queue occupancy, this should be called whenever an item is added to the queue / scheduler
        """

        self.currentInStore += 1
        if self.currentInStore == 1:
            self.queueStartFill = self.env.now

    def occupancy_calc_take_one_from_queue(self):
        """
        to track queue occupancy, this should be called whenever an item is taken from the queue/scheduler
        """

        self.currentInStore -= 1
        if self.currentInStore == 0:
            assert self.queueStartFill != None
            self.occupiedTime += self.env.now - self.queueStartFill
            self.queueStartFill = None


class LimitInFlight(object):
    """
    A helper class that tracks how many requests are currently in flight, and the in-flight request number limit
    To enforce the in-flight request number limit, a  scheduler class can inherit this class, call satisfy_flight
    to check whether scheduling a new request would violate the in-flight limit, and hold on scheduling the request
    if it will.

    To correctly track the in-flight request, the inheriting scheduler class must call send_one_req when a request is
    scheduled, and complete_one_req when a request has completed processing
    """

    def __init__(self, env, flight_limit, current_flight):
        self.flight_limit = {}
        self.current_flight = {}

    def set_flight_limit(self, client_id, limit):
        self.flight_limit[client_id] = limit

    def get_flight_limit(self, client_id=None):
        if client_id is None:
            return self.flight_limit

        if client_id in self.flight_limit:
            return self.flight_limit[client_id]
        return None

    def send_one_req(self, req):
        client_id = req.client_id
        if client_id not in self.current_flight:
            self.current_flight[client_id] = 0
        if client_id in self.flight_limit:
            assert self.current_flight[client_id] < self.flight_limit[client_id]
        self.current_flight[client_id] += 1

    def complete_one_req(self, req):
        client_id = req.client_id
        assert client_id in self.current_flight
        self.current_flight[client_id] -= 1
        assert self.current_flight[client_id] >= 0

    def satisfy_flight(self, client_id):
        if client_id not in self.flight_limit:
            return True
        if client_id not in self.current_flight:
            self.current_flight[client_id] = 0
        return self.current_flight[client_id] < self.flight_limit[client_id]


class FIFOScheduler(simpy.Store, OccupancyCalc):
    """
    A very basic FIFO scheduler, refer to this for how to implement a scheduler

    """

    def __init__(self, env, capacity, log_prefix=""):
        simpy.Store.__init__(self, env, capacity)
        self.env = env
        self.currentInStore = 0
        self.queueStartFill = None
        self.occupiedTime = 0
        self.credit_time = 0
        self.last_report_time = self.env.now
        self.notify_list = []
        self.total_queue_size = 0
        self.queue_size_dict = {}
        self.log_prefix = log_prefix

    def __str__(self):
        string = "FIFOScheduler queues: {"
        for client_id in self.queue_size_dict:
            string += str(client_id) + ":" + str(self.queue_size_dict[client_id]) + ", "
        string += "}"
        return string

    def _do_put(self, event):
        """
        This is called when clients call scheduler.put(req), where req is stored in event.item

        :param event: the put event, flag success when put is done
        """

        if len(self.items) < self._capacity:
            # self.items is defined in simpy.store: The list of items available in the store
            self.items.append(event.item)
            cur_item = event.item
            cur_id = cur_item.client_id
            if cur_id not in self.queue_size_dict.keys():
                self.queue_size_dict[cur_id] = 0
            self.queue_size_dict[cur_id] += 1
            self.total_queue_size += 1
            self.occupancy_calc_add_one_to_queue()
            event.succeed()

    def _do_get(self, event):
        """
        This is called when clients call scheduler.get()

        :param event: the get event, a request is attached to this event when get succeeds
        """
        if self.items:
            self.occupancy_calc_take_one_from_queue()
            cur_id = self.items[0].client_id
            self.queue_size_dict[cur_id] -= 1
            self.total_queue_size -= 1
            event.succeed(self.items.pop(0))

            # sometimes a scheduler may make decisions based on other scheduler's behaviors
            # in this case, it adds itself to the notify_list of other schedulers;
            # whenever these schedulers schedule a request, this scheduler will be notified
            # (its _trigger_get called), so that it will be waken up to make scheduling decisions
            for scheduler in self.notify_list:
                scheduler._trigger_get(event)

    def complete_one_req(self, req):
        """
        Called whenever the stage finishes processing a request gotten from the scheduler.
        Useful when, e.g., you want to limit the number of in-flight requests; do nothing here

        :param req:
        """
        pass

    def can_take_more_reqs(self, client_id):
        """
        Whether the scheduler can take more request (issued to it via put) from a particular client.
        Some upstream schedulers may its downstream schedulers about this to decide whether to issue more requests
        to downstream.

        :param client_id:
        :return:
        """
        return True


class PriorityScheduler(FIFOScheduler):
    """
    Same as FIFOScheduler, but schedulers priority requests (request.priority > 0) first
    Note that the specific priority value does not matter
    """

    def __init__(self, env, capacity, log_prefix=''):
        super(PriorityScheduler, self).__init__(env, capacity, log_prefix)
        self.high_prio_list = []
        self.low_prio_list = []
        self.low_prio_starve_level = 0
        self.STARVE_MAX = 4

    def _is_starving(self):
        return self.low_prio_starve_level > self.STARVE_MAX

    def _reset_starving(self):
        self.low_prio_starve_level = 0

    def _incre_starving(self):
        self.low_prio_starve_level += 1

    def _do_put(self, event):
        req = event.item
        if req.priority > 0:
            self.high_prio_list.append(req)
        else:
            self.low_prio_list.append(req)
        self.occupancy_calc_add_one_to_queue()
        event.succeed()

    def _do_get(self, event):
        if len(self.high_prio_list) > 0 or len(self.low_prio_list) > 0:
            sched_list = None
            if len(self.high_prio_list) > 0:
                if not self._is_starving():
                    sched_list = self.high_prio_list
                    self._incre_starving()
                else:
                    if len(self.low_prio_list) > 0:
                        sched_list = self.low_prio_list
                        self._reset_starving()
                    else:
                        sched_list = self.high_prio_list
            else:
                sched_list = self.low_prio_list
            assert sched_list is not None
            req = sched_list.pop(0)
            self.occupancy_calc_take_one_from_queue()
            event.succeed(req)
            for scheduler in self.notify_list:
                scheduler._trigger_get(event)


class LimitInFlightFIFOScheduler(FIFOScheduler, LimitInFlight):
    """
    Same as FIFOScheduler, but you can set limit on number of in-flight requests
    """

    def __init__(self, env, capacity, log_prefix=""):
        FIFOScheduler.__init__(self, env, capacity, log_prefix)
        self.flight_limit = {}
        self.current_flight = {}

    def complete_one_req(self, req):
        LimitInFlight.complete_one_req(self, req)
        BaseResource._trigger_get(self, None)

    def _do_get(self, event):
        for req in self.items:
            if not self.satisfy_flight(req.client_id):
                continue
            self.occupancy_calc_add_one_to_queue()
            self.send_one_req(req)
            event.succeed(req)
            self.items.remove(req)
            for scheduler in self.notify_list:
                scheduler._trigger_get(event)
            break

        if self.currentInStore == 0:
            return False
        else:
            return True


class WFQScheduler(OccupancyCalc, simpy.resources.base.BaseResource):
    """
    Weighted fair queuing scheduler; implementation based on virtual time
    (see https://en.wikipedia.org/wiki/Fair_queuing#Pseudo_code)
    """

    def __init__(self, env, capacity, log_prefix=""):
        self.env = env
        self.log_prefix = log_prefix
        self.queues = {}
        self.lastVirFinish = {}
        self.virStart = 0
        self.active_client_list = []
        # records the time the client's queue becomes empty for all clients that don't have requests pending,
        # but are still considered active
        # we periodically check it and deactivate client that haven't had requests for a while
        self.queue_empty_time = {}
        # if a client don't have any requests for deactivate_period, we consider it not active
        self.deactivate_period = 1
        self.totalActiveWeight = 0
        self.weights = {}
        self.currentInStore = 0
        self.queueStartFill = None
        self.occupiedTime = 0
        self.credit_time = 0
        self.last_report_time = self.env.now
        self.debug = False
        self.notify_list = []
        BaseResource.__init__(self, env, capacity)

    put = BoundClass(simpy.resources.store.StorePut)
    get = BoundClass(simpy.resources.store.StoreGet)

    def __str__(self):
        string = "WFQScheduler queues: {"
        for client_id in self.queues:
            string += str(client_id) + ":" + str(self.queues[client_id].qsize()) + ", "
        string += "}"

        return string

    def set_weight(self, client_id, client_weight):
        if client_id in self.weights:
            old_weight = self.weights[client_id]
        self.weights[client_id] = client_weight

        if client_id in self.active_client_list:
            self.totalActiveWeight -= old_weight
            self.totalActiveWeight += client_weight

    def deactivate_clients(self):
        to_remove_clients = []
        for client_id in self.queue_empty_time:
            if self.queue_empty_time[client_id] + self.deactivate_period < self.env.now:
                assert client_id in self.active_client_list
                self.active_client_list.remove(client_id)
                to_remove_clients.append(client_id)
                self.totalActiveWeight -= self.weights[client_id]
                if self.debug:
                    print "time:", self.env.now, "deactivate change totalActiveWeight to", self.totalActiveWeight
        for client_id in to_remove_clients:
            del self.queue_empty_time[client_id]

    def _do_put(self, event):
        self.receive(event.item)
        # print self.log_prefix, "after put time: ", self.env.now, "size: ", self.currentInStore
        self.occupancy_calc_add_one_to_queue()
        event.succeed()
        cur_req = event.item
        cur_id = cur_req.client_id

    def receive(self, req):
        # we do deactivation before each receive, so that the virtual time we set reflects the current active clients
        self.deactivate_clients()

        if req.client_id not in self.weights:
            self.set_weight(req.client_id, 1)  # set to default weight
        if req.client_id not in self.queues:
            self.queues[req.client_id] = Queue.Queue()
            self.lastVirFinish[req.client_id] = 0
        if req.client_id not in self.active_client_list:
            self.active_client_list.append(req.client_id)
            self.totalActiveWeight += self.weights[req.client_id]
            if self.debug:
                print "time:", self.env.now, "change totalActiveWeight to", self.totalActiveWeight, "triggered by client", req.client_id
        if req.client_id in self.queue_empty_time:
            del self.queue_empty_time[req.client_id]
        self.queues[req.client_id].put(req)
        self.updateTime(req)

    def updateTime(self, req):
        virStart = max(self.env.now, self.lastVirFinish[req.client_id])
        # here we assume total rate is 1 (every second we process 1 cost)
        req.virFinish = req.cost() * float(self.totalActiveWeight) / self.weights[req.client_id] + virStart
        self.lastVirFinish[req.client_id] = req.virFinish

    def send(self):
        if self.debug:
            print "send called with queue_sizes:", [(client_id, self.queues[client_id].qsize()) for client_id in
                                                    self.queues], "lastVirFinish:", self.lastVirFinish, "at time:", self.env.now
        minVirFinish = None
        curMinQueue = None
        req = None
        for client_id in self.queues:
            if not self.queues[client_id].empty() and (
                    minVirFinish is None or self.queues[client_id].queue[0].virFinish < minVirFinish):
                req = self.queues[client_id].queue[0]
                minVirFinish = self.queues[client_id].queue[0].virFinish
                curMinQueue = self.queues[client_id]
        if curMinQueue is not None:
            req = curMinQueue.get()
            if curMinQueue.empty():
                assert req.client_id not in self.queue_empty_time
                self.queue_empty_time[req.client_id] = self.env.now
        return req

    def _do_get(self, event):
        req = self.send()
        if req is not None:
            if self.debug:
                print "get return req of stage:", req.stage.name, "cost:", req._cost, "for client:", req.client_id, "at time:", self.env.now
            self.occupancy_calc_take_one_from_queue()
            event.succeed(req)
            for scheduler in self.notify_list:
                scheduler._trigger_get(event)

    def complete_one_req(self, req):
        pass

    def can_take_more_reqs(self, client_id):
        if client_id not in self.queues:
            return True
        return self.queues[client_id].qsize() < 5


class FeedbackWFQScheduler(WFQScheduler):
    """
    Same as WFQScheduler, but only schedules more requests when its downstream scheduler can take more requests
    """

    def __init__(self, env, downstream_stage, capacity, log_prefix=""):
        WFQScheduler.__init__(self, env, capacity, log_prefix)
        self.downstream_stage = downstream_stage
        if downstream_stage is not None:
            self.downstream_stage.req_queue.notify_list.append(self)

    def set_downstream_stage(self, stage):
        self.downstream_stage = stage
        stage.req_queue.notify_list.append(self)

    def send(self):
        if self.debug:
            print "send called with queue_sizes:", [(client_id, self.queues[client_id].qsize()) for client_id in
                                                    self.queues], "lastVirFinish:", self.lastVirFinish, "at time:", self.env.now
        minVirFinish = None
        curMinQueue = None
        req = None
        for client_id in self.queues:
            if not self.queues[client_id].empty() and (
                    minVirFinish is None or self.queues[client_id].queue[0].virFinish < minVirFinish) \
                    and self.downstream_stage.req_queue.can_take_more_reqs(client_id):
                req = self.queues[client_id].queue[0]
                minVirFinish = self.queues[client_id].queue[0].virFinish
                curMinQueue = self.queues[client_id]
        if curMinQueue is not None:
            req = curMinQueue.get()
            if curMinQueue.empty():
                assert req.client_id not in self.queue_empty_time
                self.queue_empty_time[req.client_id] = self.env.now
        return req


class LimitInFlightWFQScheduler(WFQScheduler, LimitInFlight):
    """
    Same as WFQScheduler, but you can set limit on number of in-flight requests
    """

    def __init__(self, env, capacity, log_prefix=""):
        WFQScheduler.__init__(self, env, capacity, log_prefix)
        self.flight_limit = {}
        self.current_flight = {}

    def complete_one_req(self, req):
        LimitInFlight.complete_one_req(self, req)
        BaseResource._trigger_get(self, None)

    def send(self):
        minVirFinish = None
        curMinQueue = None
        req = None
        for client_id in self.queues:
            if not self.queues[client_id].empty() and (minVirFinish is None or self.queues[client_id].queue[
                0].virFinish < minVirFinish) and self.satisfy_flight(client_id):
                req = self.queues[client_id].queue[0]
                minVirFinish = self.queues[client_id].queue[0].virFinish
                curMinQueue = self.queues[client_id]
        if curMinQueue is not None:
            req = curMinQueue.get()
            if curMinQueue.empty():
                self.totalActiveWeight -= self.weights[req.client_id]
        return req

    def _do_get(self, event):
        req = self.send()
        if req is not None:
            self.occupancy_calc_take_one_from_queue()  # for OccupancyCalc
            self.send_one_req(req)  # for LimitInFlight
            event.succeed(req)
            for scheduler in self.notify_list:
                scheduler._trigger_get(event)

        if self.currentInStore == 0:
            return False
        else:
            return True


class PriorityWFQScheduler(WFQScheduler):
    """
    Same as WFQScheduler, but schedulers priority requests (request.priority > 0) first;
    other requests are scheduled based on the client weight.
    Note that the specific priority value does not matter
    """

    def __init__(self, env, capacity, log_prefix=""):
        WFQScheduler.__init__(self, env, capacity, log_prefix)
        self.high_prio_list = []
        self.is_debug = False

    def _do_put(self, event):
        req = event.item
        if req.priority > 0:
            self.high_prio_list.append(req)
            self.occupancy_calc_add_one_to_queue()
            event.succeed()
            return
        WFQScheduler._do_put(self, event)

    def _do_get(self, event):
        if len(self.high_prio_list) > 0:
            self.occupancy_calc_take_one_from_queue()
            print "get one high piro req"
            event.succeed(self.high_prio_list.pop(0))
            for scheduler in self.notify_list:
                scheduler._trigger_get(event)
            return
        else:
            if self.is_debug:
                print 'Warning: the priority list is empty'
        return WFQScheduler._do_get(self, event)


class LtcAwareWFQScheduler(WFQScheduler):
    """
    A scheduler using dynamic adjust of weight to full-fill the latency-slo
    regard the weight as the rate that the scheduler pick up the request from certain client
    """

    def __init__(self, env, capacity, increase_pct_thr, decrease_pct_thr, slo_client_id, adjust_interval,
                 log_prefix=''):
        """
        :param env:
        :param capacity:
        :param increase_pct_thr: e.g if average latency exceed %pct, then need to increase the weight of target cilent
        :param decrease_pct_thr: if average latency lower %pct than slo, then can decrease the weight to increase the
                throughput of other clients
        :param slo_client_id: the client_id of the client who need the slo-guarantee (just support one)
        :param adjust_interval: the time interval to adjust the weight
        :param log_prefix:
        """
        super(LtcAwareWFQScheduler, self).__init__(env, capacity, log_prefix)
        assert 0 < decrease_pct_thr <= 1
        assert increase_pct_thr >= 1
        self.increase_pct_thr = increase_pct_thr
        self.decrease_pct_thr = decrease_pct_thr
        self.slo_client_id = slo_client_id
        self.last_avg_ltc_dict = {}
        self.ltc_slo_dict = {}
        self.adjust_interval = adjust_interval
        self.is_debug = False
        # self.adjuster = self.env.process(self.weight_adjuster_run())

    def _do_put(self, event):
        req = event.item
        assert hasattr(req, 'latency_slo')
        assert hasattr(req, 'cur_avg_ltc')
        assert hasattr(req, 'client_id')
        self.last_avg_ltc_dict[req.client_id] = req.cur_avg_ltc
        self.ltc_slo_dict[req.client_id] = req.latency_slo
        assert len(self.last_avg_ltc_dict) <= 2  # FIXME: now just support two clients
        super(LtcAwareWFQScheduler, self)._do_put(event)

    def weight_adjuster_run(self):
        """
        periodically adjust the weight of the clients according to latency data
        """
        while True:
            yield self.env.timeout(self.adjust_interval)
            # to see if need adjust
            if self.slo_client_id in self.last_avg_ltc_dict.keys():
                meet_pct = float(self.last_avg_ltc_dict[self.slo_client_id]) / float(
                    self.ltc_slo_dict[self.slo_client_id])
                # print self.last_avg_ltc_dict[self.slo_client_id]
                if meet_pct > self.increase_pct_thr or meet_pct < self.decrease_pct_thr:
                    # actual latency is too high, need to increase the weight
                    my_old_weight = self.weights[self.slo_client_id]
                    # print my_old_weight
                    other_weights_multi_ltc = sum([v * self.last_avg_ltc_dict[k] for k, v in self.weights.items()]) - \
                                              my_old_weight * self.last_avg_ltc_dict[self.slo_client_id]
                    # print other_weights_multi_ltc
                    if other_weights_multi_ltc == 0:
                        continue
                    my_new_weight = float(other_weights_multi_ltc) / self.ltc_slo_dict[self.slo_client_id]
                    if math.isnan(my_new_weight):
                        my_new_weight = my_old_weight / 2
                    if my_new_weight > 50:
                        my_new_weight = 50
                    print 'weight_adjust to :' + str(my_new_weight)
                    self.set_weight(self.slo_client_id, my_new_weight)


class DDLWFQScheduler(WFQScheduler):
    def __init__(self, env, capacity, log_prefix=""):
        WFQScheduler.__init__(self, env, capacity, log_prefix)
        self.all_req_list = []
        self.lazy_save_time_percentage = 0.1  # 10%
        self.is_debug = False
        self.low_latency_thr = 0.1  # s

    def _do_put(self, event):
        req = event.item
        assert hasattr(req, 'arrive_sys_time')
        assert hasattr(req, 'latency_sla')
        self.all_req_list.append(req)
        event.succeed()
        return

    def _do_get(self, event):
        if len(self.all_req_list) > 0:
            cur_need_to_save_dict = {}
            cur_latency_dict = {}
            cur_time = self.env.now
            for cur_req in self.all_req_list:
                cur_duration = cur_time - cur_req.arrive_sys_time
                cur_rest_time = cur_req.latency_sla - cur_duration
                cur_percentage = float(cur_rest_time) / cur_req.latency_sla
                if cur_req.latency_sla < self.low_latency_thr and cur_percentage < self.lazy_save_time_percentage:
                    if cur_percentage not in cur_need_to_save_dict.keys():
                        cur_need_to_save_dict[cur_percentage] = []
                    cur_need_to_save_dict[cur_percentage].append(cur_req)
                else:
                    if cur_rest_time not in cur_latency_dict.keys():
                        cur_latency_dict[cur_rest_time] = []
                    cur_latency_dict[cur_rest_time].append(cur_req)
            in_use_dict = cur_latency_dict
            if len(cur_need_to_save_dict.keys()) > 0:
                in_use_dict = cur_need_to_save_dict
            if len(in_use_dict.keys()) > 0:
                cur_sorted_list = sorted(in_use_dict.keys())
                select_key = cur_sorted_list[0]
                select_req = in_use_dict[select_key][0]
                self.all_req_list.remove(select_req)
                event.succeed(select_req)
                for scheduler in self.notify_list:
                    scheduler._trigger_get(event)
                return


class PriorityNoStarveWFQScheduler(PriorityWFQScheduler):
    """
    Same as  PriorityWFQScheduler, but added mechanism to avoid starvation of low priority requests
    """

    def __init__(self, env, capacity, log_prefix=''):
        super(PriorityNoStarveWFQScheduler, self).__init__(env, capacity, log_prefix=log_prefix)
        self.low_prio_starve_level = 0
        self.STARVE_MAX = 10

    def _is_starving(self):
        return self.low_prio_starve_level > self.STARVE_MAX

    def _reset_starving(self):
        self.low_prio_starve_level = 0

    def _incre_starving(self):
        self.low_prio_starve_level += 1

    def _do_put(self, event):
        PriorityWFQScheduler._do_put(self, event)

    def _do_get(self, event):
        if len(self.high_prio_list) and (not self._is_starving()):
            self.occupancy_calc_take_one_from_queue()
            self._incre_starving()
            event.succeed(self.high_prio_list.pop(0))
            for scheduler in self.notify_list:
                scheduler._trigger_get(event)
            return

        self._reset_starving()
        return WFQScheduler._do_get(self, event)


class UnifiedWFQScheduler(OccupancyCalc, simpy.resources.base.BaseResource):
    """
    WFQScheduler that can accept the request from different stages which all fetch request from this same scheduler
    """

    def __init__(self, env, capacity, log_prefix=''):
        """

        :param env:
        :param capacity: queue capacity
        :param log_prefix:
        """

        BaseResource.__init__(self, env, capacity)
        self.env = env
        self.log_prefix = log_prefix
        self.per_client_stage_queue = {}
        """dict of ({stage: Queue.Queue}), key is client_id: store the requests"""
        self._put_lock = Lock()
        self._get_lock = Lock()
        self._v_start = 0
        self._active_client_list = []
        self._client_last_finish_time = {}
        """dict of float, key is client_id: record when last scheduled request of the client
        will finish, that is the current virtual time progress of this client, Note the time line is per client,
        while the requests are stored per stage"""
        self._stage_queue_empty_time = {}
        """dict of ({stage: float}), key is client_id: record when last time the queues become empty.
        records the time the client's queue becomes empty for all clients
        that don't have requests pending but are still considered active.
        we periodically check it and deactivate client that haven't had requests for a while"""
        self._deactivate_period = 1
        """int: we periodically check it and deactivate client that haven't had requests for a while"""
        self._total_active_weight = 0
        self._c_weights = {}

        ######################################
        # for statistics
        self.currentInStore = 0
        self.queueStartFill = None
        self.occupiedTime = 0
        self.credit_time = 0
        self.last_report_time = self.env.now
        ######################################

        # for debug
        self.is_debug = False

    put = BoundClass(simpy.resources.store.StorePut)
    get = BoundClass(StageGet)

    def __str__(self):
        out_str = 'UnifiedWFQScheduler queues: {'
        for client_id in self.per_client_stage_queue:
            out_str += str(client_id) + ' : {'
            for stg in self.per_client_stage_queue[client_id]:
                out_str += stg.name + ' : ' + str((self.per_client_stage_queue[client_id])[stg].qsize()) + ', '
            out_str += '}'
        out_str += '}'
        return out_str

    def _do_put(self, event):
        req = event.item
        self._put_lock.acquire()
        self._receive(req)
        self.occupancy_calc_take_one_from_queue()
        event.succeed()
        cur_id = req.client_id
        # TODO: add support to collect statistics for enforced resource allocation
        self._put_lock.release()

    def _deactivate_clients(self):
        to_remove_clients = []
        for client_id in self._stage_queue_empty_time:
            cur_client_stage_queue_empty_time = self._stage_queue_empty_time[client_id]
            to_remove = True
            # The client will be deactivated if all its queue has been empty for _deactivate_period time
            for stg in cur_client_stage_queue_empty_time:
                if cur_client_stage_queue_empty_time[stg] + self._deactivate_period >= self.env.now:
                    to_remove = False
                    break
            if to_remove:
                assert client_id in self._active_client_list
                self._total_active_weight -= self._c_weights[client_id]
                to_remove_clients.append(client_id)
        for client_id in to_remove_clients:
            del self._stage_queue_empty_time[client_id]

    def _receive(self, req):
        # we do deactivation before each receive, so that the virtual time we set reflects the current active clients
        self._deactivate_clients()

        if req.client_id not in self._c_weights:
            self.set_weight(req.client_id, 1)  # default weight is 1
        if req.client_id not in self.per_client_stage_queue:
            self.per_client_stage_queue[req.client_id] = {}
        if req.client_id not in self._client_last_finish_time:
            self._client_last_finish_time[req.client_id] = 0
        cur_client_stage_queues = self.per_client_stage_queue[req.client_id]
        if req.stage not in cur_client_stage_queues:
            cur_client_stage_queues[req.stage] = Queue.Queue()
        if req.client_id not in self._active_client_list:
            self._active_client_list.append(req.client_id)
            self._total_active_weight += self._c_weights[req.client_id]
        if req.client_id in self._stage_queue_empty_time and req.stage in self._stage_queue_empty_time[req.client_id]:
            del (self._stage_queue_empty_time[req.client_id])[req.stage]
        cur_client_stage_queues[req.stage].put(req)
        # record the time the requests enter the scheduler
        req.unfwfq_enter_time_dict = {self: self.env.now}

    def _do_get(self, event):
        stg = event.stage
        self._get_lock.acquire()
        req = self._send(stg)
        if req is not None:
            # TODO: add for decesion data
            self.occupancy_calc_take_one_from_queue()
            event.succeed(req)
            # FIXME: not use notify_list, let's see if some problem
        else:
            if self.is_debug:
                print 'do get return None: '
        self._get_lock.release()

    def _send(self, fetch_stg):
        req = None
        min_v_finish = None
        min_queue = None
        # print 'in _send():' + str(self.per_client_stage_queue)
        for client_id in self.per_client_stage_queue:
            cur_client_min_finish = None
            cur_client_min_queue = None
            cur_client_queues = self.per_client_stage_queue[client_id]
            # for stg in cur_client_queues:
            if (fetch_stg in cur_client_queues) and (not cur_client_queues[fetch_stg].empty()):
                # calculate the virtual finish time of the head of queue if get scheduled
                expect_req = cur_client_queues[fetch_stg].queue[0]
                assert hasattr(expect_req, 'unfwfq_enter_time_dict')
                assert self in expect_req.unfwfq_enter_time_dict
                expect_v_start_time = max(expect_req.unfwfq_enter_time_dict[self],
                                          self._client_last_finish_time[client_id])
                expect_v_finish_time = expect_v_start_time + expect_req.cost() * float(
                    self._total_active_weight / self._c_weights[client_id])
                # if cur_client_min_finish is None or expect_v_finish_time < cur_client_min_finish:
                cur_client_min_finish = expect_v_finish_time
                cur_client_min_queue = cur_client_queues[fetch_stg]
            if cur_client_min_finish is not None:
                if min_v_finish is None or cur_client_min_finish < min_v_finish:
                    min_v_finish = cur_client_min_finish
                    min_queue = cur_client_min_queue
        if min_queue is not None:
            req = min_queue.get()
            # print 'send req:' + str(req)
            # update the client's virtual timeline
            # print 'update min_v_finish to:' + str(min_v_finish) + ' for client: ' + str(req.client_id)
            self._client_last_finish_time[req.client_id] = min_v_finish
            # record the queue's empty time if it is the last request in corresponding queue
            if min_queue.empty():
                if req.client_id in self._stage_queue_empty_time:
                    if fetch_stg in self._stage_queue_empty_time[req.client_id]:
                        raise RuntimeError
                    else:
                        (self._stage_queue_empty_time[req.client_id])[fetch_stg] = self.env.now
                else:
                    self._stage_queue_empty_time[req.client_id] = {fetch_stg: self.env.now}
        else:
            if self.is_debug:
                # print 'in _send() min_queue is None'
                pass
        return req

    def set_weight(self, client_id, client_weight):
        if client_id in self._active_client_list:
            assert client_id in self._c_weights
            old_weight = self._c_weights[client_id]
            self._total_active_weight = self._total_active_weight - old_weight + client_weight
        self._c_weights[client_id] = client_weight

    def complete_one_req(self, req):
        pass

    def can_take_more_reqs(self, client_id):
        raise NotImplementedError


class TokenBucket(object):
    """
    bucket to hold tokens, used for TBScheduler
    """

    def __init__(self, env, init_tokens, max_tokens, fill_rate, log_prefix):
        self.env = env
        self.capacity = float(max_tokens)
        self.tokens = float(init_tokens)
        self.fill_rate = float(fill_rate)
        self.pending_reqs = Queue.Queue()
        self.timestamp = self.env.now
        self.last_consume = self.env.now
        self.log_prefix = log_prefix

    def __str__(self):
        return "tokens: " + str(self.tokens) + " last_consume: " + str(self.last_consume)

    def consume(self, tokens):
        self.tokens -= tokens
        self.last_consume = self.env.now

    def peek(self):
        assert not self.pending_reqs.empty()
        return self.pending_reqs.queue[0]

    def has_enough(self, tokens):
        if self.tokens >= tokens:
            return True
        return False

    def get_tokens(self):
        if self.tokens < self.capacity:
            delta = self.fill_rate * (self.env.now - self.timestamp)
            self.tokens = min(self.capacity, self.tokens + delta)
            self.timestamp = self.env.now
        # print self.log_prefix, "time:", self.env.now, "set tokens to: ", self.tokens
        return self.tokens


class TBScheduler(OccupancyCalc, simpy.resources.base.BaseResource):
    """
    Token-bucket based scheduler, a client's request only gets scheduled if the client has enough tokens
    When a request is scheduled, req.cost() tokens are consumed
    Tokens are refilled every refill_interval, based on each client's rate
    """

    def __init__(self, env, default_rate, default_capacity, refill_interval, store_capacity, log_prefix=""):
        self.env = env
        self.log_prefix = log_prefix
        self.default_rate = default_rate
        self.default_capacity = default_capacity
        self.refill_interval = refill_interval
        self.currentInStore = 0
        self.queueStartFill = None
        self.occupiedTime = 0
        self.credit_time = 0
        self.last_report_time = self.env.now
        self.buckets = {}
        self.token_filler = env.process(self.fill_tokens())
        self.notify_list = []
        BaseResource.__init__(self, env, store_capacity)

    put = BoundClass(simpy.resources.store.StorePut)
    get = BoundClass(simpy.resources.store.StoreGet)

    def __str__(self):
        string = "TBScheduler states, tokens: {"
        for client_id in self.buckets:
            string += str(client_id) + ":" + str(self.buckets[client_id].tokens) + " "
        string += "}"

        string += " queues: {"
        for client_id in self.buckets:
            string += str(client_id) + ":" + str(self.buckets[client_id].pending_reqs.qsize()) + " "
        string += "}"

        return string

    def set_rate(self, client_id, rate):
        if client_id not in self.buckets:
            self.buckets[client_id] = TokenBucket(self.env, rate * self.refill_interval, self.default_capacity, rate,
                                                  self.log_prefix + "_client_" + str(client_id))
        else:
            self.buckets[client_id].fill_rate = rate
        if self.buckets[client_id].tokens > rate * self.refill_interval:
            self.buckets[client_id].tokens = rate * self.refill_interval
            # print self.log_prefix, "set_rate:", self.buckets[client_id].fill_rate, "for client_id:", client_id

    def get_rate(self, client_id):
        if client_id not in self.buckets:
            return self.default_rate
        return self.buckets[client_id].fill_rate

    def _do_put(self, event):
        if self.currentInStore < self._capacity:
            req = event.item
            if req.client_id not in self.buckets:
                self.buckets[req.client_id] = TokenBucket(self.env, self.default_rate * self.refill_interval,
                                                          self.default_capacity, self.default_rate,
                                                          self.log_prefix + "_client_" + str(req.client_id))
            self.buckets[req.client_id].pending_reqs.put(req)
            self.occupancy_calc_add_one_to_queue()
            event.succeed()

    def _do_get(self, event):
        oldest_bucket = None
        for client_id in self.buckets:
            b = self.buckets[client_id]
            if (
                    oldest_bucket is None or b.last_consume < oldest_bucket.last_consume) and not b.pending_reqs.empty() and b.has_enough(
                b.peek().cost()):
                oldest_bucket = b

        if oldest_bucket is not None:
            assert not oldest_bucket.pending_reqs.empty()
            req = oldest_bucket.pending_reqs.get()
            assert oldest_bucket.has_enough(req.cost())
            oldest_bucket.consume(req.cost())
            self.occupancy_calc_take_one_from_queue()
            for scheduler in self.notify_list:
                scheduler._trigger_get(event)
            event.succeed(req)
        else:
            # print "oldest_bucket is None at time:", self.env.now, self
            pass

        # Note: if _do_get() returns False, simpy assumes there is no more items left in the queue
        # and will not perform any more gets until new items are added
        # However, for token-bucket scheduler, get() returns nothing for one client does not mean
        # other clients cannot get more requests processed (because they still have tokens left)
        # so _do_get() should return True whenever currentInStore is not 0
        if self.currentInStore == 0:
            return False
        else:
            return True

    def complete_one_req(self, req):
        pass

    def can_take_more_reqs(self, client_id):
        raise NotImplementedError

    def fill_tokens(self):
        while True:
            yield self.env.timeout(self.refill_interval)
            for b in self.buckets.values():
                b.get_tokens()
            # Note: by default, BaseResource in simpy only calls get() when new items are added
            # so for token-bucket scheduler and alike, where get() could return additional items
            # not only because new item is added, but also because more tokens are available
            # scheduler is responsible to call _trigger_get() on every occasion where get() could return more items
            BaseResource._trigger_get(self, None)


class RefundTBScheduler(TBScheduler):
    """
    Same as TBScheduler, but consume tokens based on the predicted cost (calculated by the predict_cost_func) instead
    of a request's actual cost.
    After processing a request, its real cost is examined, and the scheduler will refund any tokens that was charged
    beyond the request's real cost (or re-charge any tokens that was insufficient).
    """

    def __init__(self, env, predict_cost_func, default_rate, default_capacity, refill_interval, store_capacity,
                 log_prefix=""):
        TBScheduler.__init__(self, env, default_rate, default_capacity, refill_interval, store_capacity, log_prefix)
        self.predict_cost_func = predict_cost_func

    def _do_put(self, event):
        req = event.item
        req.real_cost = req._cost
        req._cost = self.predict_cost_func(req)
        return TBScheduler._do_put(self, event)

    def complete_one_req(self, req):
        refund_amt = req._cost - req.real_cost
        req._cost = req.real_cost
        self.buckets[req.client_id].consume(0 - refund_amt)
        BaseResource._trigger_get(self, None)


class PriorityTBScheduler(TBScheduler):
    def __init__(self, env, default_rate, default_capacity, store_capacity, refill_interval, log_prefix=""):
        self.high_prio_list = []
        TBScheduler.__init__(self, env, default_rate, default_capacity, store_capacity, refill_interval, log_prefix)

    def _do_put(self, event):
        req = event.item
        if req.priority > 0:
            self.high_prio_list.append(req)
            self.occupancy_calc_add_one_to_queue()
            event.succeed()
            return
        TBScheduler._do_put(self, event)

    def _do_get(self, event):
        if len(self.high_prio_list) > 0:
            self.occupancy_calc_take_one_from_queue()
            event.succeed(self.high_prio_list.pop(0))
            for scheduler in self.notify_list:
                scheduler._trigger_get(event)
            return
        return TBScheduler._do_get(self, event)

    def complete_one_req(self, req):
        pass


# TODO: Implement an unified version of DRF Scheduler, that can be shared by multiple stages
# Suli Yang 2018/10/28

class DRFScheduler(OccupancyCalc, simpy.resources.base.BaseResource):
    """
    # Dominate resource fairness scheduler
    # This scheduler only deals with a list of resources and costs
    # Each resource has capacity 1
    # The stage is responsible for converting each request's resource consumption to a vector based on the total
    # resource using the req_cost_func
    #
    # Note that this is an online version of DRF
    # and it only looks at the first request in each client's request queue
    # so it may lose some nice properties that DRF has, and certainly some optimization opportunities
    #
    # Currently this scheduler does not clear the resources_allocation or dominate share
    # so they just keep accumulating
    # it also does not support adding clients (new clients will get too much advantage because of
    # old clients's resources usage accumulation)
    """

    def __init__(self, env, capacity, num_resources, log_prefix="", debug=False):
        self.env = env
        self.log_prefix = log_prefix
        self.debug = debug
        self.queues = {}
        self.num_resources = num_resources
        self.consumed_resources = [0] * num_resources
        self.dominate_shares = {}  # each client's dominate shares, initially 0
        self.allocated_resources = {}  # resources allocated to each client, initially each client has an empty dic
        self.weights = {}
        self.notify_list = []

        # For statistics calculation (queue occupancy, utilization, etc.)
        self.currentInStore = 0
        self.queueStartFill = None
        self.occupiedTime = 0
        self.credit_time = 0
        self.last_report_time = self.env.now

        self.log_prefix = 'DRFScheduler queues: {'

        simpy.resources.base.BaseResource.__init__(self, env, capacity)

    put = BoundClass(simpy.resources.store.StorePut)
    get = BoundClass(simpy.resources.store.StoreGet)

    def __str__(self):
        string = self.log_prefix
        for client_id in self.queues:
            string += str(client_id) + ":" + str(len(self.queues[client_id])) + ", "
        string += "}"
        return string

    def set_weight(self, client_id, client_weight):
        self.weights[client_id] = client_weight

    def get_weight(self, client_id):
        if client_id not in self.weights:
            return 1
        return self.weights[client_id]

    def _do_put(self, event):
        self.receive(event.item)
        event.succeed()

    def receive(self, req):
        # if it is a new client, add request queues, allocated_res, etc. for it
        if req.client_id not in self.queues:
            self.queues[req.client_id] = []
            self.dominate_shares[req.client_id] = 0
            self.allocated_resources[req.client_id] = [0] * self.num_resources

        self.queues[req.client_id].append(req)
        if self.debug:
            def print_queue(q):
                rt = {}
                for id in q:
                    if id not in rt:
                        rt[id] = 0
                    rt[id] += len(q[id])
                return rt

            print 'receive req for client:', req.client_id, 'at time:', self.env.now, ' stage:', req.stage.name, ' cur_queue:', print_queue(
                self.queues)

        # for occupancy calc
        self.occupancy_calc_add_one_to_queue()

    def _do_get(self, event):
        req = self.send()
        if req is not None:
            if self.debug:
                print "get return req for client:", req.client_id, "at time:", self.env.now

            self.occupancy_calc_take_one_from_queue()
            event.succeed(req)
            for scheduler in self.notify_list:
                scheduler._trigger_get(event)

    def check_client_id_pick(self, picked_client_id, client_id):
        if len(self.queues[client_id]) == 0:
            return False

        share = self.dominate_shares[client_id]
        if picked_client_id is None or share < self.dominate_shares[picked_client_id]:
            return True

        return False

    def pick_req(self):
        if self.debug:
            print "drf send called with queue_sizes:", [(client_id, len(self.queues[client_id])) for client_id in
                                                        self.queues], "at time:", self.env.now
            print "dominate_shares:", self.dominate_shares

        picked_client_id = None
        for client_id in self.dominate_shares:
            if self.check_client_id_pick(picked_client_id, client_id):
                picked_client_id = client_id

        # cannot find a client with requests pending, return None
        if picked_client_id is None:
            return None

        req_queue = self.queues[picked_client_id]
        assert len(req_queue) > 0
        req = req_queue.pop(0)
        assert req.client_id == picked_client_id
        if self.debug:
            print 'pick req for stage ' + str(req.stage.name) + ' pick client: ' + str(
                req.client_id) + ' num_workers:' + str(req.stage.num_workers)
        return req

    def pick_req_from_client_id(self, client_id):
        if client_id not in self.queues:
            print "pick_req_from_client_id return None for client_id", client_id, "because no req_queue for it"
            return None
        if len(self.queues[client_id]) == 0:
            print "pick_req_from_client_id return None for client_id", client_id, "because its req_queue is empty"
            return None
        for req in self.queues[client_id]:
            if not hasattr(req, "put_back"):
                self.queues[client_id].remove(req)
                return req
        print "pick_req_from_client_id return None for client_id", client_id, "because all reqs are put back"
        return None
        # return self.queues[client_id].pop(0)

    def update_allocation_shares(self, req):
        # update consumed_resource vector
        for i in range(len(req.cost())):
            self.consumed_resources[i] += req.cost()[i] / self.get_weight(req.client_id)

        # update picked client's allocation vector
        assert req.client_id in self.allocated_resources
        allocation = self.allocated_resources[req.client_id]
        assert len(allocation) == self.num_resources
        for i in range(len(req.cost())):
            allocation[i] += req.cost()[i] / self.get_weight(req.client_id)
        # update dominate share
        # we simply use the allocation because each resource's capacity is 1

        self.dominate_shares[req.client_id] = max(allocation)

    def send(self):
        req = self.pick_req()
        if req is None:
            return None

        self.update_allocation_shares(req)

        return req

    def complete_one_req(self, req):
        pass

    def can_take_more_reqs(self, client_id):
        raise NotImplementedError


class PriorityDRFScheduler(DRFScheduler):
    """
    Same as DRFScheduler, but schedulers priority requests (request.priority > 0) first;
    """

    def __init__(self, env, capacity, num_resources, log_prefix="", debug=False):
        self.high_prio_list = []
        DRFScheduler.__init__(self, env, capacity, num_resources, log_prefix=log_prefix, debug=debug)

    def _do_put(self, event):
        req = event.item
        if req.priority > 0:
            self.high_prio_list.append(req)
            self.occupancy_calc_add_one_to_queue()
            event.succeed()
            return
        DRFScheduler._do_put(self, event)

    def _do_get(self, event):
        if len(self.high_prio_list) > 0:
            self.occupancy_calc_take_one_from_queue()
            event.succeed(self.high_prio_list.pop(0))
            for scheduler in self.notify_list:
                scheduler._trigger_get(event)
            return
        DRFScheduler._do_get(self, event)


class PriorityNoStarveDRFScheduler(PriorityDRFScheduler):
    """
    Same as PriorityDRFScheduler, but with mechanism to avoid starvationn on low-priority requests
    """

    def __init__(self, env, capacity, num_resources, log_prefix='', debug=False):
        super(PriorityNoStarveDRFScheduler, self).__init__(env, capacity, num_resources,
                                                           log_prefix=log_prefix, debug=debug)
        self.low_prio_starve_level = 0
        self.STARVE_MAX = 10

    def _is_starving(self):
        return self.low_prio_starve_level > self.STARVE_MAX

    def _reset_starving(self):
        self.low_prio_starve_level = 0

    def _incre_starving(self):
        self.low_prio_starve_level += 1

    def _do_put(self, event):
        PriorityDRFScheduler._do_put(self, event)

    def _do_get(self, event):
        if len(self.high_prio_list) and (not self._is_starving()):
            self.occupancy_calc_take_one_from_queue()
            self._incre_starving()
            event.succeed(self.high_prio_list.pop(0))
            for scheduler in self.notify_list:
                scheduler._trigger_get(event)
            return

        self._reset_starving()
        DRFScheduler._do_get(self, event)


class AfterDRFScheduler(DRFScheduler):
    """
    Same as DRFScheduler, but update the dominate resource share after processing a request, instead of before
    """

    def send(self):
        return self.pick_req()

    def complete_one_req(self, req):
        self.update_allocation_shares(req)
        if self.debug:
            print "complete one req for client", req.client_id, "service_time", req.service_time, "cost", req.cost()


class AfterPriorityFeedbackDRFScheduler(AfterDRFScheduler):
    """
    Same as AfterDRF Scheduler, but processes high-priority requests first,
    and considers if the downstream stage can take more requests

    """

    def __init__(self, env, downstream_stage, capacity, num_resources, log_prefix="", debug=False):
        self.high_prio_list = []
        AfterDRFScheduler.__init__(self, env, capacity, num_resources, log_prefix, debug)
        self.downstream_stage = downstream_stage
        if downstream_stage is not None:
            self.downstream_stage.req_queue.notify_list.append(self)

    def set_downstream_stage(self, stage):
        self.downstream_stage = stage
        stage.req_queue.notify_list.append(self)

    def _do_put(self, event):
        req = event.item
        if req.priority > 0:
            self.high_prio_list.append(req)
            self.occupancy_calc_add_one_to_queue()
            event.succeed()
            return
        DRFScheduler._do_put(self, event)

    def check_client_id_pick(self, picked_client_id, client_id):
        if len(self.queues[client_id]) == 0:
            return False
        share = self.dominate_shares[client_id]
        if (picked_client_id is None or share < self.dominate_shares[picked_client_id]) \
                and self.downstream_stage.req_queue.can_take_more_reqs(client_id):
            return True
        return False

    def _do_get(self, event):
        if len(self.high_prio_list) > 0:
            self.occupancy_calc_take_one_from_queue()
            event.succeed(self.high_prio_list.pop(0))
            for scheduler in self.notify_list:
                scheduler._trigger_get(event)
            return
        DRFScheduler._do_get(self, event)


class PeriodicDRFScheduler(DRFScheduler):
    """
    Same  as DRFScheduler, but clears shares periodically (so that newly added clients do not get too much advantage)
    """

    def __init__(self, env, capacity, num_resources, period, log_prefix="", debug=False):
        DRFScheduler.__init__(self, env, capacity, num_resources, log_prefix, debug)
        self.period = period
        self.monitor = self.env.process(self.monitor_run())

    def monitor_run(self):
        while True:
            yield self.env.timeout(self.period)
            self.dominate_shares = {}
            self.allocated_resources = {}
            for client_id in self.queues:
                self.dominate_shares[client_id] = 0
                self.allocated_resources[client_id] = [0] * self.num_resources


class PriorityPeriodicNoStarveDRFScheduler(PriorityNoStarveDRFScheduler):
    """
    Same  as PriorityNoStarveDRFScheduler, but clears shares periodically
    """

    def __init__(self, env, capacity, num_resources, period, log_prefix="", debug=False):
        super(PriorityPeriodicNoStarveDRFScheduler, self).__init__(env, capacity, num_resources, log_prefix=log_prefix)
        self.period = period
        self.monitor = self.env.process(self.monitor_run())

    def monitor_run(self):
        while True:
            yield self.env.timeout(self.period)
            self.dominate_shares = {}
            self.allocated_resources = {}
            for client_id in self.queues:
                self.dominate_shares[client_id] = 0
                self.allocated_resources[client_id] = [0] * self.num_resources


class AfterPeriodicDRFScheduler(PeriodicDRFScheduler):
    """
    Same as PeriodicDRFScheduler, but update resource share after processing a request
    """

    def __init__(self, env, capacity, num_resources, period, log_prefix="", debug=False):
        PeriodicDRFScheduler.__init__(self, env, capacity, num_resources, period, log_prefix="", debug=False)
        self.log_prefix = self.log_prefix.replace("DRF", "AfterPeriodicDRF")

    def send(self):
        return self.pick_req()

    def complete_one_req(self, req):
        self.update_allocation_shares(req)
        if self.debug:
            print "complete one req for client", req.client_id, "service_time", req.service_time, "cost", req.cost()


class EFFScheduler(simpy.Store, OccupancyCalc):
    """
    Earliest Finish time Scheduler: Using greedy algorithm to schedule request according to [arrive time + exec time]
    Note for job exec time, it is regarded as in ideal condition that the scheduler will know the resource
    consumption before schedule, thus have perfect knowledge
    """

    def __init__(self, env, capacity, log_prefix='', is_debug=False):
        super(EFFScheduler, self).__init__(env, capacity)
        self.env = env
        self.name_str = 'EFFScheduler'
        self.currentInStore = 0
        self.queueStartFill = None
        self.occupiedTime = 0
        self.credit_time = 0
        self.last_report_time = self.env.now
        self.notify_list = []
        self.total_queue_size = 0
        self.queue_size_dict = {}
        self.complete_time_req_list_dict = {}
        self.is_debug = is_debug

    def __str__(self):
        log_str = self.name_str + ' queues: {'
        for client_id in self.queue_size_dict:
            log_str += str(client_id) + ":" + str(self.queue_size_dict[client_id]) + ", "
        log_str += "}"
        return log_str

    def _do_put(self, event):
        cur_req = event.item
        assert hasattr(cur_req, 'exec_time')
        cur_complete_time = cur_req.exec_time + self.env.now
        if cur_complete_time not in self.complete_time_req_list_dict.keys():
            self.complete_time_req_list_dict[cur_complete_time] = []
        self.complete_time_req_list_dict[cur_complete_time].append(cur_req)
        self.occupancy_calc_add_one_to_queue()
        event.succeed()

    def _do_get(self, event):
        complete_time_list = self.complete_time_req_list_dict.keys()
        print 'jingliu:EFF:_do_get() at time: ' + str(self.env.now)
        if len(complete_time_list) > 0:
            complete_time_list = sorted(complete_time_list)
            earlies_time = complete_time_list[0]
            cur_earlies_complete_req_list = self.complete_time_req_list_dict[earlies_time]
            req = cur_earlies_complete_req_list[0]
            if self.is_debug:
                print 'EFF scheduler: get:' + str(req) + ' time: ' + str(self.env.now) + \
                      ' cur_dict:' + str(self.complete_time_req_list_dict.keys())
            if len(cur_earlies_complete_req_list) > 1:
                self.complete_time_req_list_dict[earlies_time].remove(req)
            else:
                del self.complete_time_req_list_dict[earlies_time]
            self.occupancy_calc_take_one_from_queue()
            print 'jingliu:EFF:_do_get() will succeed the req at time:' + str(self.env.now)
            event.succeed(req)
            for scheduler in self.notify_list:
                scheduler._trigger_get(event)
            return

    def complete_one_req(self, req):
        pass

    def can_take_more_reqs(self, client_id):
        return True


class EDFScheduler(simpy.Store, OccupancyCalc):
    """
    Earliest deadline first scheduler
    """

    def __init__(self, env, capacity, log_prefix='', is_debug=False):
        super(EDFScheduler, self).__init__(env, capacity)
        self.env = env
        self.name_str = 'EDFScheduler'
        self.currentInStore = 0
        self.queueStartFill = None
        self.occupiedTime = 0
        self.credit_time = 0
        self.last_report_time = self.env.now
        self.notify_list = []
        self.total_queue_size = 0
        self.queue_size_dict = {}
        self.queue_size_dict[1] = 0
        self.queue_size_dict[2] = 0
        self.deadline_req_list_dict = {}
        self.is_debug = is_debug

    def __str__(self):
        log_str = self.name_str + ' queues: {'
        for client_id in self.queue_size_dict:
            log_str += str(client_id) + ":" + str(self.queue_size_dict[client_id]) + ", "
        log_str += "}"
        return log_str

    def _do_put(self, event):
        cur_req = event.item
        assert hasattr(cur_req, "deadline")
        if cur_req.deadline not in self.deadline_req_list_dict:
            self.deadline_req_list_dict[cur_req.deadline] = []
        self.deadline_req_list_dict[cur_req.deadline].append(cur_req)
        self.occupancy_calc_add_one_to_queue()
        self.queue_size_dict[cur_req.client_id] += 1
        event.succeed()

    def _do_get(self, event):
        deadline_list = self.deadline_req_list_dict.keys()
        if len(deadline_list) > 0:
            deadline_list = sorted(deadline_list)
            deadline = deadline_list[0]
            cur_deadline_req_list = self.deadline_req_list_dict[deadline]
            req = cur_deadline_req_list[0]
            if self.is_debug:
                print 'time' + str(self.env.now) + 'EDF scheduler: get:' + str(req.stage.name) + '-' + str(
                    req.client_id) + ' deadline: ' + str(req.deadline)
            if len(cur_deadline_req_list) > 1:
                self.deadline_req_list_dict[deadline].remove(req)
            else:
                del self.deadline_req_list_dict[deadline]
            self.occupancy_calc_take_one_from_queue()
            event.succeed(req)
            self.queue_size_dict[req.client_id] -= 1
            for scheduler in self.notify_list:
                scheduler._trigger_get(event)
            return

    def complete_one_req(self, req):
        pass

    def can_take_more_reqs(self, client_id):
        return True


#################### Property Function for Schedulers #################################

def is_unified_scheduler(scheduler):
    """
    is scheduler unified (can be shared by multiple stages)?

    :param scheduler:
    :return: True or False
    """

    if isinstance(scheduler, UnifiedWFQScheduler):
        return True

    return False


def is_priority_scheduler(scheduler):
    """
    is this a priority-based scheduelr (high-prioirty requests will be processed first)?

    :param scheduler:
    :return: True or False
    """

    if isinstance(scheduler, PriorityWFQScheduler):
        return True
    if isinstance(scheduler, PriorityDRFScheduler):
        return True
    if isinstance(scheduler, AfterPriorityFeedbackDRFScheduler):
        return True
    if isinstance(scheduler, PriorityTBScheduler):
        return True

    return False


def is_after_scheduler(scheduler):
    """
    Does this scheduler accounts cost after processing the request?
    If true, can be used to process requests whose cost are initially unknown (until after the processing)

    :param scheduler:
    :return: True or False
    """

    if isinstance(scheduler, AfterDRFScheduler):
        return True
    if isinstance(scheduler, AfterPeriodicDRFScheduler):
        return True
    if isinstance(scheduler, AfterPriorityFeedbackDRFScheduler):
        return True

    return False


def is_feedback_scheduler(scheduler):
    """
    Does this scheduler consider if downstream stage/scheduler can take more requests when issuing requests?

    :param scheduler:
    :return: True or False
    """

    if isinstance(scheduler, FeedbackWFQScheduler):
        return True
    if isinstance(scheduler, AfterPriorityFeedbackDRFScheduler):
        return True
    return False


#################### Helper Functions to Generate Schedulers Based on Config #####################

def get_scheduler(env, config, section):
    """

    :param env:
    :param config:  ConfigParser object
    :param section: section name describing the scheduler
    :return: scheduler instance
    """

    try:
        type = config.get(section, "type")
        if type == "FIFO":
            return FIFOScheduler(env, float('inf'))
        if type == "LimitInFlightFIFO":
            return LimitInFlightFIFOScheduler(env, float('inf'))
        elif type == "TokenBucket":
            default_rate = config.getfloat(section, "default_rate")
            default_capacity = config.getfloat(section, "default_capacity")
            refill_interval = 1
            if "refill_interval" in config.options(section):
                refill_interval = config.getfloat(section, "refill_interval")
            tb_scheduler = TBScheduler(env, default_rate, default_capacity, refill_interval, float('inf'))
            if "rate_map" in config.options(section):
                rate_map = ast.literal_eval(config.get(section, "rate_map"))
                for client_id in rate_map:
                    tb_scheduler.set_rate(client_id, rate_map[client_id])
            return tb_scheduler
        elif type == "Refund_TokenBucket":
            predict_cost = config.getfloat(section, "predict_cost")
            predict_cost_func = lambda req: predict_cost
            default_rate = config.getfloat(section, "default_rate")
            default_capacity = config.getfloat(section, "default_capacity")
            refill_interval = 1
            if "refill_interval" in config.options(section):
                refill_interval = config.getfloat(section, "refill_interval")
            tb_scheduler = RefundTBScheduler(env, predict_cost_func, default_rate, default_capacity, refill_interval,
                                             float('inf'))
            if "rate_map" in config.options(section):
                rate_map = ast.literal_eval(config.get(section, "rate_map"))
                for client_id in rate_map:
                    tb_scheduler.set_rate(client_id, rate_map[client_id])
            return tb_scheduler
        elif type == "WFQ":
            scheduler = WFQScheduler(env, float('inf'))
            if "weight_map" in config.options(section):
                weight_map = ast.literal_eval(config.get(section, "weight_map"))
                for client_id in weight_map:
                    scheduler.set_weight(client_id, weight_map[client_id])
            return scheduler
        elif type == "PRIORITY_WFQ" or type == "Priority_WFQ":
            scheduler = PriorityWFQScheduler(env, float('inf'))
            if "weight_map" in config.options(section):
                weight_map = ast.literal_eval(config.get(section, "weight_map"))
                for client_id in weight_map:
                    scheduler.set_weight(client_id, weight_map[client_id])
            return scheduler
        elif type == "Feedback_WFQ":
            scheduler = FeedbackWFQScheduler(env, None, float('inf'))
            if "weight_map" in config.options(section):
                weight_map = ast.literal_eval(config.get(section, "weight_map"))
                for client_id in weight_map:
                    scheduler.set_weight(client_id, weight_map[client_id])
            return scheduler
        elif type == "LimitInFlightWFQ":
            scheduler = LimitInFlightWFQScheduler(env, float('inf'))
            if "weight_map" in config.options(section):
                weight_map = ast.literal_eval(config.get(section, "weight_map"))
                for client_id in weight_map:
                    scheduler.set_weight(client_id, weight_map[client_id])
            return scheduler
        elif type == "DRF":
            num_res = config.getint(section, "number_of_resources")
            scheduler = DRFScheduler(env, float('inf'), num_res)
            if "weight_map" in config.options(section):
                weight_map = ast.literal_eval(config.get(section, "weight_map"))
                for client_id in weight_map:
                    scheduler.set_weight(client_id, weight_map[client_id])
            return scheduler
        elif type == "PRIORITY_DRF" or type == "Priority_DRF":
            num_res = config.getint(section, "number_of_resources")
            scheduler = PriorityDRFScheduler(env, float('inf'), num_res)
            if "weight_map" in config.options(section):
                weight_map = ast.literal_eval(config.get(section, "weight_map"))
                for client_id in weight_map:
                    scheduler.set_weight(client_id, weight_map[client_id])
            return scheduler
        elif type == "After_DRF":
            num_res = config.getint(section, "number_of_resources")
            scheduler = AfterDRFScheduler(env, float('inf'), num_res)
            if "weight_map" in config.options(section):
                weight_map = ast.literal_eval(config.get(section, "weight_map"))
                for client_id in weight_map:
                    scheduler.set_weight(client_id, weight_map[client_id])
            return scheduler
        elif type == "After_Priority_Feedback_DRF":
            num_res = config.getint(section, "number_of_resources")
            scheduler = AfterPriorityFeedbackDRFScheduler(env, None, float('inf'), num_res)
            if "weight_map" in config.options(section):
                weight_map = ast.literal_eval(config.get(section, "weight_map"))
                for client_id in weight_map:
                    scheduler.set_weight(client_id, weight_map[client_id])
            return scheduler
        elif type == "After_Periodic_DRF":
            num_res = config.getint(section, "number_of_resources")
            period = config.getfloat(section, "period")
            scheduler = AfterPeriodicDRFScheduler(env, float('inf'), num_res, period)
            if "weight_map" in config.options(section):
                weight_map = ast.literal_eval(config.get(section, "weight_map"))
                for client_id in weight_map:
                    scheduler.set_weight(client_id, weight_map[client_id])
            return scheduler
        else:
            raise Exception("Unknown scheduler type: " + type)
    except ConfigParser.NoOptionError:
        raise Exception("incomplete configuration of scheduler: " + section)


def get_scheduler_from_config(env, config_util):
    if type(config_util) is dict:
        configs = config_util
    else:
        configs = config_util.configs
    sched_type = configs['type']
    if sched_type == "FIFO":
        return FIFOScheduler(env, float('inf'))
    elif sched_type == "TokenBucket":
        default_rate = configs['default_rate']
        default_capacity = configs['default_capacity']
        refill_interval = 1
        if "refill_interval" in config_util.keys:
            refill_interval = configs['refill_interval']
        tb_scheduler = TBScheduler(env, default_rate, default_capacity, refill_interval, float('inf'))
        if "rate_map" in configs.keys():
            rate_map = configs['rate_map']
            for client_id in rate_map:
                tb_scheduler.set_rate(client_id, rate_map[client_id])
        return tb_scheduler
    elif sched_type == "Refund_TokenBucket":
        predict_cost = configs['predict_cost']
        predict_cost_func = lambda req: predict_cost
        default_rate = configs['default_rate']
        default_capacity = configs['default_capacity']
        refill_interval = 1
        if "refill_interval" in config_util.keys:
            refill_interval = configs['refill_interval']
        tb_scheduler = RefundTBScheduler(env, predict_cost_func, default_rate, default_capacity, refill_interval,
                                         float('inf'))
        if "rate_map" in configs.keys():
            rate_map = configs['rate_map']
            for client_id in rate_map:
                tb_scheduler.set_rate(client_id, rate_map[client_id])
        return tb_scheduler
    elif sched_type == "WFQ":
        scheduler = WFQScheduler(env, float('inf'))
        if "weight_map" in configs.keys():
            weight_map = configs['weight_map']
            for client_id in weight_map:
                scheduler.set_weight(client_id, weight_map[client_id])
        return scheduler
    elif sched_type == "DRF":
        num_res = configs['number_of_resources']
        return DRFScheduler(env, float('inf'), num_res, debug=False)
    elif sched_type == "PRIORITY_DRF":
        num_res = configs['number_of_resources']
        return PriorityDRFScheduler(env, float('inf'), num_res)
    elif sched_type == 'APFV_Stage_DRF':
        num_res = configs['number_of_resources']
        scheduler = APFVStageDRFScheduler(env, None, float('inf'), num_res)
        if "weight_map" in configs.keys():
            weight_map = configs['weight_map']
            for client_id in weight_map:
                scheduler.set_weight(client_id, weight_map[client_id])
        return scheduler

    else:
        raise Exception("Unknown Scheduler sched_type" + sched_type)
