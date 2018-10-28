import simpy


class StageReq(object):
    """
    Request to Stage that blocks for certain events and then consume resource for this request
    Use this class unless you need more complicated behaviors that cannot be handled;
    in that case, consider ConditionalStageReq or BlkStageReq
    """
    debug = False

    def __init__(self, env, stage, client_id, resource_profile, blocking_evts, downstream_reqs, priority=0):
        self.env = env
        self.stage = stage
        self.client_id = client_id
        self.resource_profile = resource_profile
        self.blocking_evts = blocking_evts
        self.downstream_reqs = downstream_reqs
        self.priority = priority
        self._cost = None
        self.done = env.event()
        self.done.req = self
        self.service_time = 0
        self.blocking_time = 0
        self.sched_done = False
        self.client_submit_time = -1

    def cost(self):
        return self._cost

    def __str__(self):
        return "StageReq (stage:%s client_id:%d resource_profile:%s priority:%d done:%s " \
               "blocking_evts:%s downstream_reqs:%s)" % (
                   self.stage.name, self.client_id, self.resource_profile, self.priority, self.done, self.blocking_evts,
                   self.downstream_reqs)

    def __repr__(self):
        return self.__str__()

    # get one resource's consumption, *in this request*, i.e., does not include downstream requests
    def get_res_consumption(self, res):
        if res not in self.resource_profile:
            return 0
        return self.resource_profile[res]

    def add_downstream_req(self, downstream_req):
        self.downstream_reqs.append(downstream_req)

    def add_blocking_event(self, evt):
        self.blocking_evts.append(evt)

    def all_done_list(self):
        done_list = [self.done]

        for req in self.downstream_reqs:
            req_done_list = req.all_done_list()
            done_list = done_list + req_done_list

        return done_list

    def get_all_dependent_reqs(self):
        req_list = [self]
        for req in self.downstream_reqs:
            d_req_list = req.get_all_dependent_reqs()
            req_list = req_list + d_req_list
        return req_list

    def submit(self):
        return self.stage.put_req(self)

    def wait_submit(self):
        yield self.stage.put_req(self)

    def process(self, w_id=None, enforce_res=None):
        if StageReq.debug:
            print "start process req of client_id", self.client_id, "for stage", self.stage.name, "at time", self.env.now
        # access each resource sequentially
        for res in self.resource_profile:
            if self.resource_profile[res] == 0:
                continue
            if (enforce_res is not None) and (res == enforce_res):
                assert hasattr(self, 'predict_resource_profile')
                if (self.predict_resource_profile[res] == 0) and (self.resource_profile[res] > 0):
                    # print "resubmit req with resource_profile:", self.resource_profile.values(),\
                    #    "predict_resource_profile:", self.predict_resource_profile.values()
                    # resubmit this request
                    yield self.submit()
                    self.env.exit(0)

            yield res.request(self.client_id, self.resource_profile[res], self.stage.name, self.priority)

        for req in self.downstream_reqs:
            if StageReq.debug and isinstance(req, StageReq):
                print self.stage.name, "submit downstream req to stage", req.stage.name
            if StageReq.debug and not isinstance(req, StageReq):
                print self.stage.name, "submit downstream pre-sched req"
            yield req.submit()

        # we wait for blocking req in parallel
        if w_id is not None:
            self.stage.start_blocking(w_id, self)
        for evt in self.blocking_evts:
            yield evt
        if w_id is not None:
            self.stage.finish_blocking(w_id, self)

        self.done.time = self.env.now
        self.done.succeed()
        if StageReq.debug:
            print "finish process req of client_id", self.client_id, "for stage", self.stage.name, "at time", self.env.now

    def process_no_res(self, w_id, enforce_res):
        """
        process a request, but requires this request does not use enforce_res; otherwise abort.
        :param w_id:
        :param enforce_res:
        :return: environment exit with 0 if requests completes, -1 if aborted because enforce_res is required
        """
        if StageReq.debug:
            print "start process req of client_id", self.client_id, "for stage", self.stage.name, "at time", self.env.now

        has_enforce_res = False
        # access each resource sequentially
        print "now process_no_res", self
        for res in self.resource_profile:
            if self.resource_profile[res] == 0:
                continue
            if (enforce_res is not None) and (res == enforce_res):
                has_enforce_res = True
                continue
            yield res.request(self.client_id, self.resource_profile[res], self.stage.name, self.priority)

        if has_enforce_res:
            self.env.exit(-1)

        for req in self.downstream_reqs:
            if StageReq.debug and isinstance(req, StageReq):
                print self.stage.name, "submit downstream req to stage", req.stage.name
            if StageReq.debug and not isinstance(req, StageReq):
                print self.stage.name, "submit downstream pre-sched req"
            yield req.submit()

        # we wait for blocking req in parallel
        if w_id is not None:
            self.stage.start_blocking(w_id, self)
        for evt in self.blocking_evts:
            yield evt
        if w_id is not None:
            self.stage.finish_blocking(w_id, self)

        self.done.time = self.env.now
        self.done.succeed()
        if StageReq.debug:
            print "finish process req of client_id", self.client_id, "for stage", self.stage.name, "at time", self.env.now

        self.env.exit(0)


class ResBlockingProfile(object):
    def __init__(self, res_cost_list):
        """
        :param res_cost_list: format is a list of dicts use stage's resource as key and cost (number) as value
        : that is, the request can consume a set of cpu, io, net resource, and another set of resource
        : to support flexibility and order of resource consuming
        : eg: could be: [{stage.net_res:1, stage.io_res:1}, {stage.cpu_res:1} {stage.io_res:1}]
        :param downstream_req_list: [] of reqs that issue after complete the res_cost_list's consumption
        :param evt_list: the list[] of events that this piece of res_cost need to wait for
        """
        self.res_cost_list = res_cost_list
        self.downstream_req_list = []
        self.evt_list = []

    def __str__(self):
        return "res_cost_list: %s down_stream_req_list: %s evt_list: %s " % (
            self.res_cost_list, self.downstream_req_list,
            self.evt_list)

    def get_res_cost_list(self):
        return self.res_cost_list

    def get_evt_list(self):
        return self.evt_list

    def get_downstream_list(self):
        return self.downstream_req_list

    def add_downstream_req(self, req):
        self.downstream_req_list.append(req)

    def add_blocking_event(self, evt):
        self.evt_list.append(evt)


class ConditionalStageReq(object):
    """
    to support the condition that a request serves as the downstream request of multiple requests, and submit iff
    the two upstream request satisfy certain condition
    """
    debug = False

    def __init__(self, env, stage, client_id, resource_profile, blocking_evts, downstream_reqs, priority=0,
                 condition_func=None):
        '''
        condition_func: param should be , change the resource consumption of this request according to condition
        '''
        self.env = env
        self.stage = stage
        self.client_id = client_id
        self.resource_profile = resource_profile
        self.blocking_evts = blocking_evts
        self.downstream_reqs = downstream_reqs
        self.priority = priority
        self._cost = None
        self.done = env.event()
        self.done.req = self
        self.service_time = 0
        self.blocking_time = 0
        self.sched_done = False
        self.condition_func = condition_func
        self.can_be_done = False
        self.cur_client = None
        self.client_submit_time = -1

    def cost(self):
        return self._cost

    def __str__(self):
        return "ConditionalStageReq (stage:%s client_id:%d resource_profile:%s priority:%d done:%s " \
               "blocking_evts:%s downstream_reqs:%s can_be_done:%r )" % (
                   self.stage.name, self.client_id, self.resource_profile, self.priority, self.done, self.blocking_evts,
                   self.downstream_reqs, self.can_be_done)

    def __repr__(self):
        return self.__str__()

    def get_res_consumption(self, res):
        """get one resource's consumption, *in this request*, i.e., does not include downstream requests"""
        if res not in self.resource_profile:
            return 0
        return self.resource_profile[res]

    def all_done_list(self):
        done_list = [self.done]

        for req in self.downstream_reqs:
            req_done_list = req.all_done_list()
            done_list = done_list + req_done_list

        return done_list

    def get_all_dependent_reqs(self):
        req_list = [self]
        for req in self.downstream_reqs:
            d_req_list = req.get_all_dependent_reqs()
            req_list = req_list + d_req_list
        return req_list

    def submit(self):
        tmp_req, tmp_res_profile = self.condition_func(self)
        tmp_req.resource_profile = tmp_res_profile
        if not tmp_req.can_be_done:
            tmp_req.done = tmp_req.env.event()
            tmp_req.done.req = tmp_req
        return tmp_req.stage.put_req(tmp_req)

    def process(self, w_id=None, enforce_res=None):
        if ConditionalStageReq.debug:
            print "start process req of client_id", self.client_id, "for stage", self.stage.name, "at time", self.env.now, ' req is:', str(
                self)
        # access each resource sequentially
        for res in self.resource_profile:
            if self.resource_profile[res] == 0:
                continue
            '''
            if (enforce_res is not None) and (res == enforce_res):
                assert hasattr(self, 'predict_resource_profile')
                if (self.predict_resource_profile[res] == 0) and (self.resource_profile[res] > 0):
                    # print "resubmit req with resource_profile:", self.resource_profile.values(),\
                    #    "predict_resource_profile:", self.predict_resource_profile.values()
                    # resubmit this request
                    yield self.submit()
                    self.env.exit(0)
            '''
            yield res.request(self.client_id, self.resource_profile[res], self.stage.name, self.priority)

        for req in self.downstream_reqs:
            if ConditionalStageReq.debug and isinstance(req, ConditionalStageReq):
                print self.stage.name, "submit downstream req to stage", req.stage.name
            if ConditionalStageReq.debug and not isinstance(req, ConditionalStageReq):
                print self.stage.name, "submit downstream pre-sched req"
            yield req.submit()

        if w_id is not None:
            self.stage.start_blocking(w_id, self)
        for evt in self.blocking_evts:
            yield evt
        if w_id is not None:
            self.stage.finish_blocking(w_id, self)

        self.done.time = self.env.now
        if self.can_be_done:
            assert self.cur_client is not None
            assert self.client_submit_time != -1
            if hasattr(self.cur_client, 'is_sync_client') and self.cur_client.is_sync_client() is False:
                self.cur_client.record_async_req_complete(self.client_submit_time)

        self.done.succeed()
        if ConditionalStageReq.debug:
            print "finish process req of client_id", self.client_id, "for stage", self.stage.name, "at time", self.env.now

    def reset_downstream_req(self):
        self.downstream_reqs = []

    def reset_blocking_event(self):
        self.blocking_evts = []

    def add_downstream_req(self, req):
        self.downstream_reqs.append(req)

    def add_blocking_event(self, evt):
        self.blocking_evts.append(evt)


class BlkStageReq(StageReq):
    def __init__(self, env, stage, client_id, priority=0):
        StageReq.__init__(self, env, stage, client_id, None, [], [], priority)
        self.res_blk_profile_list = []

    def __str__(self):
        return "BlkStageReq (stage:%s client_id:%s res_blk_profile_list:%s priority: %d done: %s" % (
            self.stage, self.client_id,
            self.res_blk_profile_list, self.priority, self.done)

    def append_res_blk_profile(self, res_blk_prof):
        # order matters
        self.res_blk_profile_list.append(res_blk_prof)

    def get_one_type_res(self, name):
        """ get the sum of all resources usage whose name contains name """
        res_sum = 0
        for res_blk_prof in self.res_blk_profile_list:
            cur_res_cost_list = res_blk_prof.get_res_cost_list()
            for res_cost_dict in cur_res_cost_list:
                for res in res_cost_dict:
                    if name in res.name:
                        res_sum += res_cost_dict[res]
        return res_sum

    def get_res_consumption(self, res):
        res_sum = 0
        for res_blk_prof in self.res_blk_profile_list:
            cur_res_cost_list = res_blk_prof.get_res_cost_list()
            for res_cost_dict in cur_res_cost_list:
                for r in res_cost_dict:
                    if r == res:
                        res_sum += res_cost_dict[res]
        # print 'get_res_consumption return ' + str(res_sum)
        return res_sum

    def process(self, w_id=None, enforce_res=None):
        if StageReq.debug:
            print "start process req for stage", self.stage.name, "at time", self.env.now
        for res_blk_prof in self.res_blk_profile_list:
            cur_res_cost_list = res_blk_prof.get_res_cost_list()
            cur_downstream_list = res_blk_prof.get_downstream_list()
            cur_blk_evt_list = res_blk_prof.get_evt_list()
            for res_cost_dict in cur_res_cost_list:
                for res in res_cost_dict:
                    if res_cost_dict[res] == 0:
                        continue
                        # if (enforce_res is not None) and (res == enforce_res):
                        # not fully understand enforce_res
                        # assert hasattr(self, 'predict_resource_profile')
                    yield res.request(self.client_id, res_cost_dict[res], self.stage.name, self.priority)
            for req in cur_downstream_list:
                if StageReq.debug and isinstance(req, StageReq):
                    print self.stage.name, "submit downstream req to stage", req.stage.name
                if StageReq.debug and not isinstance(req, StageReq):
                    print self.stage.name, "submit downstream pre-sched req"
                yield req.submit()

            if len(cur_blk_evt_list) != 0:
                if w_id is not None:
                    self.stage.start_blocking(w_id, self)
                for evt in cur_blk_evt_list:
                    yield evt
                if w_id is not None:
                    self.stage.finish_blocking(w_id, self)

        # finally, success
        self.done.time = self.env.now
        self.done.succeed()
        if StageReq.debug:
            print "finish process req for stage", self.stage.name, "at time", self.env.now

    def process_no_res(self, w_id, enforce_res):
        if StageReq.debug:
            print "start process req for stage", self.stage.name, "at time", self.env.now

        print "process_no_res got req with io:", self.get_one_type_res("io"), "enforce_res", enforce_res

        use_enforce_res = False
        for res_blk_prof in self.res_blk_profile_list:
            cur_res_cost_list = res_blk_prof.get_res_cost_list()
            cur_downstream_list = res_blk_prof.get_downstream_list()
            cur_blk_evt_list = res_blk_prof.get_evt_list()
            print "now process profile with res_cost_list:", cur_res_cost_list
            for res_cost_dict in cur_res_cost_list:
                for res in res_cost_dict:
                    if res_cost_dict[res] == 0:
                        continue
                        # if (enforce_res is not None) and (res == enforce_res):
                        # not fully understand enforce_res
                        # assert hasattr(self, 'predict_resource_profile')
                    if (enforce_res is not None) and (res == enforce_res):
                        print "set has_enforce_res to True"
                        use_enforce_res = True
                        break
                    print "now cousuming res", res
                    yield res.request(self.client_id, res_cost_dict[res], self.stage.name, self.priority)

                if use_enforce_res:
                    print "speculated aborted req with res_profile", [str(profile) for profile in
                                                                      self.res_blk_profile_list]
                    self.env.exit(-1)

            for req in cur_downstream_list:
                if StageReq.debug and isinstance(req, StageReq):
                    print self.stage.name, "submit downstream req to stage", req.stage.name
                if StageReq.debug and not isinstance(req, StageReq):
                    print self.stage.name, "submit downstream pre-sched req"
                yield req.submit()

            if len(cur_blk_evt_list) != 0:
                if w_id is not None:
                    self.stage.start_blocking(w_id, self)
                for evt in cur_blk_evt_list:
                    yield evt
                if w_id is not None:
                    self.stage.finish_blocking(w_id, self)

        # finally, success
        self.done.time = self.env.now
        self.done.succeed()
        if StageReq.debug:
            print "finish process req for stage", self.stage.name, "at time", self.env.now

        print "speculated executed req with res_profile", [str(profile) for profile in self.res_blk_profile_list]

        self.env.exit(0)

