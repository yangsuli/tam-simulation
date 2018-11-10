import random
import sys

import numpy as np
import simpy
import ConfigParser
from simpy.resources.base import BaseResource

from client import Client
from seda_config import ConfigError
from scheduler import LimitInFlight, is_after_scheduler, FIFOScheduler, is_feedback_scheduler, TBScheduler
from stage import Stage, ActiveWorkerStage, AfterCostStage, OnDemandStage, SpeculativeExecStage
from stage_req import StageReq, ResBlockingProfile, BlkStageReq
from util.schedule_util import ScheduleUtil


class UpdateMemStoreReq(object):
    """
    Request the represents updating the HBase MetaStore
    (In HBase, after a write, the written content is added to the memstore, and later flushed)
    """

    def __init__(self, stage, client_id, change_size):
        """

        :param stage:  the memstore flush stage
        :param client_id:
        :param change_size: the updated data size
        """
        self.stage = stage
        self.client_id = client_id
        self.change_size = change_size

    def submit(self):
        """
        :return: an event the indicating the submission
        """
        return self.stage.put_req(self)


class FlushMemStoreReq(object):
    """
    Represents flushing the memstore to HDFS.
    HBase will periodically flush memstore to hdfs
    """

    def __init__(self, client_id, flush_size):
        """

        :param client_id:
        :param flush_size:  size of data to be flushed to hdfs (and deleted from the memstore)
        """

        self.client_id = client_id
        self.flush_size = flush_size


class MemStore(simpy.Store):
    """
    MemStore can be though of a special type of scheduler.
    Regular scheduler takes a set of requests and schedules them (the same requests).
    MemStore take a set of UpdateMemStoreReq, and when the memstore is full (or under memory pressure), schedules
    FlushMemStoreReq. Each update to memstore may be small, but usually a FlushMemStoreReq will flush a significant
    size of data.
    """

    def __init__(self, env, total_mem, mem_quota, flush_threshold):
        """

        :param env:
        :param total_mem: total memory size MemStore manages
        :param mem_quota:  the memory quota each client has. When the client data exceeds the quota, MemStore will flush
                           its data to hdfs; when the client data exceeds 2 * mem_quota, further UpdateMemStoreReq will
                           fail
        :param flush_threshold: when data in memstore (aggregated over all clients) exceeds total_mem * flush_threshold,
                                MemStore will flush data to hdfs.
        """

        self.env = env
        self.total_mem = total_mem
        self.mem_quota = mem_quota
        self.flush_threshold = flush_threshold
        self.stores = {}  # client_id: store_size I am assuming one client just has one store
        self.flushing_clients = []  # list of client id that are currently flushing their memstore
        self.cur_flushing_size = 0  # size of mem that is currently being flushed
        self.occupied = 0
        self.notify_list = []
        BaseResource.__init__(self, env, float('inf'))

    def under_mem_pressure(self):
        if (self.occupied - self.cur_flushing_size) > self.flush_threshold * self.total_mem:
            return True
        return False

    def put_modification(self, client_id, change_size):
        self.stores[client_id] += change_size
        self.occupied += change_size

    def _do_put(self, event):
        memstore_req = event.item
        if memstore_req.client_id not in self.stores:
            self.stores[memstore_req.client_id] = 0
        if self.stores[memstore_req.client_id] < self.mem_quota * 2:
            self.put_modification(memstore_req.client_id, memstore_req.change_size)
            event.succeed()
        # regardless of whether current put should succeed or not
        # we should check all putters in the put_queue because other puts from other clients might succeed
        return True

    def finish_flush(self, client_id, flush_size):
        """
        called when a flush is completed

        :param client_id:
        :param flush_size:
        """

        assert self.stores[client_id] >= flush_size
        assert self.occupied >= flush_size
        assert self.cur_flushing_size >= flush_size
        self.stores[client_id] -= flush_size
        self.occupied -= flush_size
        self.cur_flushing_size -= flush_size
        self.flushing_clients.remove(client_id)

    def _do_get(self, event):
        max_client = None
        max_size = 0
        for client_id in self.stores:
            store_size = self.stores[client_id]
            if store_size > max_size and client_id not in self.flushing_clients:
                max_client = client_id
                max_size = store_size

        # cannot find anything to flush
        if max_client is None:
            return False

        # flush if the client's data exceeds mem_quota
        if max_size >= self.mem_quota:
            flush_req = FlushMemStoreReq(max_client, max_size)
            # return the flush_req (note this does *not* complete the flush)
            event.succeed(flush_req)
            self.cur_flushing_size += max_size
            self.flushing_clients.append(max_client)
            # continue to trigger other gets
            # because there might be more stores exceeding quota size
            return True

        # flush if the aggregated data size is too large to cause memory pressure (even when no single client's data
        # exceeds quota
        if self.under_mem_pressure():
            flush_req = FlushMemStoreReq(max_client, max_size)
            event.succeed(flush_req)
            self.cur_flushing_size += max_size
            self.flushing_clients.append(max_client)
            # continue to trigger other gets
            # because even after this flush, we may still under memory pressure
            return True

        # no one exceeds their quota, and we are not under memory pressure
        # don't need to do any flush, do't need to check further
        return False

    def complete_one_req(self, req):
        self.finish_flush(req.client_id, req.flush_size)
        self._trigger_put(None)
        for scheduler in self.notify_list:
            scheduler._trigger_get(None)

    def can_take_more_reqs(self, client_id):
        if client_id in self.stores and self.stores[client_id] >= self.mem_quota * 2:
            return False
        # could also return False based on total mem consumption, we don't do that yet
        return True

    # FIXME: needs to appropriately inherit OccupancyCal to calculate occupancy
    # This is just an workaround to avoid exceptions when monitor tries to get queue occupancy for MemStore
    def get_occupancy(self):
        return 0.0


class MemFlushStage(Stage):
    """
    Stage that performs memsotre flushes (uses MemStore as its scheduler)
    Instead of directly call request.process() when processing requests, MemFlushStage verifies the request it gets
    is a FlushMemStoreReq, and performs hdfs writes based on the flush size
    """

    def __init__(self, env, region_server, name, num_workers, mem_store, log_file=sys.stdout):
        """
        :param env:
        :param region_server: region_server this stage belongs to
        :param name:
        :param num_workers:
        :param mem_store:
        :param log_file: open file handler
        """

        assert isinstance(mem_store, MemStore)
        Stage.__init__(self, env, name, num_workers, mem_store, None, log_file)
        self.region_server = region_server
        self.cur_flushing = 0
        self.debug = False

    # the mem flush stage itself is not using any resources here
    def worker_run(self, w_id):
        assert self.region_server is not None
        while True:
            if w_id not in self.workers:
                self.env.exit()
            req = yield self.req_queue.get()
            assert isinstance(req, FlushMemStoreReq)
            self.cur_flushing += 1
            if self.debug:
                print "w_id", w_id, "start flush (now", self.cur_flushing, ") of size", req.flush_size, "client_id", req.client_id, "at time", self.env.now, "stores:", self.req_queue.stores
            self.start_service(w_id, req)
            write_submit, write_done = self.region_server.submit_hdfs_write_req(req.client_id, req.flush_size)
            yield write_submit
            self.start_blocking(w_id, req)
            yield write_done
            self.finish_blocking(w_id, req)
            self.finish_service(w_id, req)
            self.req_queue.complete_one_req(req)
            self.cur_flushing -= 1
            if self.debug:
                print "w_id", w_id, "finish flush (now", self.cur_flushing, ") of size", req.flush_size, "client_id", req.client_id, "at time", self.env.now, "stores:", self.req_queue.stores


class AsyncMemFlushStage(MemFlushStage):
    """
    Same as MemFlushStage, but asynchronously write to hdfs instead of blocking on the write completion
    """

    def flush_complete_callback(self, event):
        req = event.req
        w_id = event.w_id
        self.req_queue.complete_one_req(req)
        self.cur_flushing -= 1
        if self.debug:
            print "w_id", w_id, "finish flush (now", self.cur_flushing, ") of size", req.flush_size, "client_id", req.client_id, "at time", self.env.now, "stores:", self.req_queue.stores

    def worker_run(self, w_id):
        assert self.region_server is not None
        assert self.region_server is not None
        while True:
            if w_id not in self.workers:
                self.env.exit()
            req = yield self.req_queue.get()
            assert isinstance(req, FlushMemStoreReq)
            self.cur_flushing += 1
            if self.debug:
                print "w_id", w_id, "start flush (now", self.cur_flushing, ") of size", req.flush_size, "client_id", req.client_id, "at time", self.env.now, "stores:", self.req_queue.stores
            self.start_service(w_id, req)
            write_submit, write_done = self.region_server.submit_hdfs_write_req(req.client_id, req.flush_size)
            write_done.req = req
            write_done.w_id = w_id
            write_done.callbacks.append(self.flush_complete_callback)
            yield write_submit
            self.finish_service(w_id, req)


class HBaseReq(object):
    """
    Represents an hbase request.
    A request includes a rpc message (of rpc_size) and a reply (of reply_size); it may also include hdfs namenode lookup,
    hdfs read , and data modification (in which case memory will be updated, and log will be appended, both would lead
    to hdfs writes).
    """

    def __init__(self, client_id, rpc_size, reply_size, rpc_time, mem_update_size, log_append_size,
                 namenode_lookup_time, datanode_read_size, log_sync=True, short_circuit=False):
        """

        :param client_id:
        :param rpc_size: size of the rpc request region server has to read in
        :param reply_size: rpc reply size sent back to the client
        :param rpc_time: cpu time used to handle hbase rpc calls
        :param mem_update_size:
        :param log_append_size:
        :param namenode_lookup_time: timed used look up namenode metadata (holding hdfs namespace lock)
        :param datanode_read_size: read from datanode
        :param log_sync: do we wait until log is persisted to hdfs before sending reply to client?
        :param short_circuit: hdfs short-circuit read

        all sizes are in MBs
        """
        self.client_id = client_id
        self.rpc_size = rpc_size
        self.reply_size = reply_size
        self.rpc_time = rpc_time
        self.mem_update_size = mem_update_size
        self.log_append_size = log_append_size
        self.log_sync = log_sync
        self.namenode_lookup_time = namenode_lookup_time
        self.datanode_read_size = datanode_read_size
        self.short_circuit = short_circuit


class RegionServer(object):
    """
    Original RegionServer, as how the current HBase RegionServer implementation works.
    However, you can also tweak its configuration so that its behaviors are slighted different
    (e.g., by configuring some stages to be on-demand, or adjust the number of workers in each stage)
    """

    # CPU time will be scaled based on CPU frequency; here we give the number based on a 1 GHz CPU
    # for a CPU with 2 GHz frequency, for example, the SEND_REPLY_CPU_TIME will be 25 us
    SEND_REPLY_CPU_TIME = 50e-6  # 50 us
    RECEIVE_DATA_CPU_TIME = 50e-6  # 50us
    READ_RPC_CPU_TIME = 50e-6  # 50 us
    STREAM_CPU_RATIO = 50e-6 / 64e3  # 50 us for every 64KB data
    LOG_APPEND_CPU_TIME = 50e-6  # 50 us

    def __init__(self, env, phy_node, hbase, rpc_read_stage, rpc_handle_stage, rpc_respond_stage, mem_flush_stage,
                 log_append_stage,
                 log_sync_stage, data_stream_stage, ack_process_stage):
        """

        :param env:
        :param phy_node: phy_node this region server is on (region server access phy_node's resources)
        :param hbase: hbase cluster this region server belongs to
        :param rpc_read_stage:
        :param rpc_handle_stage:
        :param rpc_respond_stage:
        :param mem_flush_stage:
        :param log_append_stage:
        :param log_sync_stage:
        :param data_stream_stage:
        :param ack_process_stage:
        """

        self.env = env
        self.phy_node = phy_node
        self.hbase = hbase
        self.wal_id = 9999
        self.rpc_read_stage = rpc_read_stage
        self.rpc_handle_stage = rpc_handle_stage
        self.rpc_respond_stage = rpc_respond_stage
        self.mem_flush_stage = mem_flush_stage
        mem_flush_stage.region_server = self
        self.log_append_stage = log_append_stage
        self.log_sync_stage = log_sync_stage
        self.data_stream_stage = data_stream_stage
        self.ack_process_stage = ack_process_stage

    def get_hdfs_write_req(self, client_id, write_size):
        """
        writing to hdfs includes three parts:
        1. stream the data from the client side to hdfs data node
        2. hdfs writes
        3. processing the write acknowledgements

        :param client_id:
        :param write_size:
        :return: (req, done)
        """

        # network resource consumption is 0 because the first data replica will be written to the local node
        # so data is streamed to the same node (no network transfer)
        stream_req = StageReq(self.env, self.data_stream_stage, client_id,
                              {self.phy_node.cpu_res: RegionServer.STREAM_CPU_RATIO * write_size,
                               self.phy_node.net_res: 0}, [], [])
        ack_process_req = StageReq(self.env, self.ack_process_stage, client_id, {}, [], [])
        write_req, ack_done = self.hbase.hdfs.get_write_req(self.phy_node, client_id, write_size)
        stream_req.downstream_reqs.append(write_req)
        stream_req.downstream_reqs.append(ack_process_req)
        ack_process_req.blocking_evts.append(ack_done)
        return stream_req, ack_process_req.done

    def submit_hdfs_write_req(self, client_id, write_size):
        """
        :param client_id:
        :param write_size:
        :return:  (submission_event, write_completion event)
        """

        stream_req, write_done = self.get_hdfs_write_req(client_id, write_size)
        return stream_req.submit(), write_done

    def submit_req(self, hbase_req):
        """
        :param hbase_req: HBaseReq
        :return: submit_event and done_event, indicating the submission of the hbase request, and the completion of it,
                 respectively
        """
        nn_lookup_req = None
        nn_lookup_done = None
        if hbase_req.namenode_lookup_time > 0:
            nn_lookup_req, nn_lookup_done = self.hbase.hdfs.get_namenode_req(hbase_req.client_id,
                                                                             hbase_req.namenode_lookup_time)

        dn_read_req = None
        dn_read_done = None
        if hbase_req.datanode_read_size > 0 and not hbase_req.short_circuit:
            dn_read_req, dn_read_done = self.hbase.hdfs.get_read_req(hbase_req.client_id, hbase_req.datanode_read_size)

        log_sync_req = None
        if hbase_req.log_sync and hbase_req.log_append_size > 0:
            log_sync_req = StageReq(self.env, self.log_sync_stage, self.wal_id, {}, [], [])

        log_append_req = None
        if hbase_req.log_append_size > 0:
            log_append_req = StageReq(self.env, self.log_append_stage, hbase_req.client_id,
                                      {self.phy_node.cpu_res: RegionServer.LOG_APPEND_CPU_TIME}, [], [])
            log_write_req, log_write_done = self.get_hdfs_write_req(self.wal_id, hbase_req.log_append_size)
            log_append_req.downstream_reqs.append(log_write_req)
            if log_sync_req is not None:
                log_append_req.downstream_reqs.append(log_sync_req)
                log_sync_req.blocking_evts.append(log_write_done)

        mem_update_req = None
        if hbase_req.mem_update_size > 0:
            mem_update_req = UpdateMemStoreReq(self.mem_flush_stage, hbase_req.client_id, hbase_req.mem_update_size)

        # rpc respond stage
        rpc_respond_req = StageReq(self.env, self.rpc_respond_stage, hbase_req.client_id,
                                   {self.phy_node.cpu_res: RegionServer.SEND_REPLY_CPU_TIME,
                                    self.phy_node.net_res: hbase_req.reply_size}, [], [])

        # rpc handling stage, the processing is too complicated for StageReq, so we use BlkStageReq instead
        rpc_handle_req = BlkStageReq(self.env, self.rpc_handle_stage, hbase_req.client_id)
        # First part of rpc handling: cpu processing, issue hdfs metadata lookup and wait until it is done
        rpc_handle_io_res = 0
        if hbase_req.datanode_read_size > 0 and hbase_req.short_circuit:
            # short-circuited read is performed within the rpc handle stage
            rpc_handle_io_res = hbase_req.datanode_read_size
        rpc_handle_res_cost = [{self.phy_node.cpu_res: hbase_req.rpc_time, self.phy_node.io_res: rpc_handle_io_res}]
        rpc_handle_res_profile = ResBlockingProfile(rpc_handle_res_cost)
        if nn_lookup_req is not None:
            rpc_handle_res_profile.add_downstream_req(nn_lookup_req)
            rpc_handle_res_profile.add_blocking_event(nn_lookup_done)
        rpc_handle_req.append_res_blk_profile(rpc_handle_res_profile)
        # Second part of rpc handling: issue hdfs reads and wait for their completion
        if dn_read_req is not None:
            dn_blocking_res_cost = [{}]
            dn_blocking_profile = ResBlockingProfile(dn_blocking_res_cost)
            dn_blocking_profile.add_downstream_req(dn_read_req)
            dn_blocking_profile.add_blocking_event(dn_read_done)
            rpc_handle_req.append_res_blk_profile(dn_blocking_profile)
        # Third part of rpc handling: process data read from the datanode and append log
        rpc_handle_net_res = 0
        if hbase_req.datanode_read_size > 0 and not hbase_req.short_circuit:
            rpc_handle_net_res += hbase_req.datanode_read_size  # network cost for receiving data read
        rpc_handle_cpu_res = 0
        if hbase_req.datanode_read_size > 0 and not hbase_req.short_circuit:
            rpc_handle_cpu_res += RegionServer.RECEIVE_DATA_CPU_TIME  # cpu cost for receiving data read
        rpc_handle_res_cost2 = [{self.phy_node.cpu_res: rpc_handle_cpu_res, self.phy_node.net_res: rpc_handle_net_res}]
        rpc_handle_res_profile2 = ResBlockingProfile(rpc_handle_res_cost2)
        if log_append_req is not None:
            rpc_handle_res_profile2.add_downstream_req(log_append_req)
        if log_sync_req is not None:
            rpc_handle_res_profile2.add_blocking_event(log_sync_req.done)
        rpc_handle_req.append_res_blk_profile(rpc_handle_res_profile2)
        # Forth part of rpc handling: update mem
        rpc_handle_res_cost3 = [{}]
        rpc_handle_res_profile3 = ResBlockingProfile(rpc_handle_res_cost3)
        if mem_update_req is not None:
            rpc_handle_res_profile3.add_downstream_req(mem_update_req)
        rpc_handle_req.append_res_blk_profile(rpc_handle_res_profile3)
        # Final part of rpc handling: pass to rpc_respond to send reply
        rpc_handle_res_cost4 = [{}]
        rpc_handle_res_profile4 = ResBlockingProfile(rpc_handle_res_cost4)
        rpc_handle_res_profile4.add_downstream_req(rpc_respond_req)
        rpc_handle_req.append_res_blk_profile(rpc_handle_res_profile4)

        # rpc read stage
        assert hbase_req.rpc_size > 0
        rpc_read_res_profile = {self.phy_node.cpu_res: RegionServer.READ_RPC_CPU_TIME,
                                self.phy_node.net_res: hbase_req.rpc_size}
        rpc_read_req = StageReq(self.env, self.rpc_read_stage, hbase_req.client_id, rpc_read_res_profile, [], [])
        rpc_read_req.downstream_reqs.append(rpc_handle_req)

        return rpc_read_req.submit(), rpc_respond_req.done

    @staticmethod
    def get_datanode_io_from_handle_req(handle_stage_req):
        """
        :param handle_stage_req:
        :return:  the total io size needed to process handle_stage_req (and its downstream requests)
        """

        datanode_io = 0
        for blocking_profile in handle_stage_req.res_blk_profile_list:
            for res_dict in blocking_profile.res_cost_list:
                for res in res_dict:
                    if "_io" in res.name:
                        datanode_io += res_dict[res]

            # we rely on that all downstream requests are of StageReq type
            for d_req in blocking_profile.downstream_req_list:
                if "xceive" not in d_req.stage.name:
                    continue
                for res in d_req.resource_profile:
                    if "_io" in res.name:
                        datanode_io += d_req.resource_profile[res]
        return datanode_io

    @staticmethod
    def get_cost_func(to_schedule, phy_node):
        """

        :param to_schedule: resource type to schedule (cpu, net, io or others)
        :param phy_node: the phy_node the resources are (typically) on
        :return: lambda object mapping req to cost
        """
        if to_schedule == "datanode_io":
            return lambda req: RegionServer.get_datanode_io_from_handle_req(req)
        else:
            return ScheduleUtil.get_cost_func(to_schedule, phy_node)

    @staticmethod
    def construct_stages(env, name, phy_node, hbase, rs_conf, stage_log, async_flush=False):
        """

        :param env:
        :param name:
        :param phy_node:
        :param hbase:
        :param rs_conf: RegionServerConf
        :param stage_log: open file handle
        :param async_flush: whether the mem_flush stage performs flushes asynchronously or block on flush completions
        :return:
        """
        rpc_read_scheduler = rs_conf.rpc_read_scheduler_generator(env)
        rpc_read_cost_func = RegionServer.get_cost_func(rs_conf.rpc_read_stage_schedule_resource, phy_node)
        rpc_read_stage = Stage(env, name + "_rpc_read", rs_conf.num_rpc_readers, rpc_read_scheduler,
                               rpc_read_cost_func, log_file=stage_log)
        rpc_read_stage.monitor_interval = rs_conf.stage_monitor_interval

        rpc_handle_scheduler = rs_conf.rpc_handle_scheduler_generator(env)
        rpc_handle_cost_func = RegionServer.get_cost_func(rs_conf.rpc_handle_stage_schedule_resource, phy_node)
        rpc_handle_stage = Stage(env, name + "_rpc_handle", rs_conf.num_rpc_handlers, rpc_handle_scheduler,
                                 rpc_handle_cost_func, log_file=stage_log)
        rpc_handle_stage.monitor_interval = rs_conf.stage_monitor_interval

        rpc_respond_scheduler = rs_conf.rpc_respond_scheduler_generator(env)
        rpc_respond_cost_func = RegionServer.get_cost_func(rs_conf.rpc_respond_stage_schedule_resource, phy_node)
        rpc_respond_stage = Stage(env, name + "_rpc_respond", rs_conf.num_rpc_responders, rpc_respond_scheduler,
                                  rpc_respond_cost_func, log_file=stage_log)
        rpc_respond_stage.monitor_interval = rs_conf.stage_monitor_interval

        mem_store = MemStore(env, rs_conf.total_mem_size, rs_conf.mem_quota, rs_conf.flush_threshold)
        if async_flush:
            mem_flush_stage = AsyncMemFlushStage(env, None, name + "_mem_flush", rs_conf.num_mem_flushers, mem_store,
                                                 log_file=stage_log)
        else:
            mem_flush_stage = MemFlushStage(env, None, name + "_mem_flush", rs_conf.num_mem_flushers, mem_store,
                                            log_file=stage_log)
        mem_flush_stage.monitor_interval = rs_conf.stage_monitor_interval
        if is_feedback_scheduler(rpc_handle_scheduler):
            rpc_handle_scheduler.set_downstream_stage(mem_flush_stage)

        # single ring buffer consumer, FIFO order
        log_append_stage = Stage(env, name + "_log_append", 1, FIFOScheduler(env, float('inf')), None,
                                 log_file=stage_log)
        log_append_stage.monitor_interval = rs_conf.stage_monitor_interval

        # log sync can be processed at any order; it waits for the finishing of writes that are already issued
        # but here we use FIFO, it doesn't really matter because it does not use any resources
        log_sync_stage = Stage(env, name + "_log_sync", rs_conf.num_log_syncers, FIFOScheduler(env, float('inf')), None,
                               log_file=stage_log)
        log_sync_stage.monitor_interval = rs_conf.stage_monitor_interval

        if rs_conf.data_stream_scheduler_generator is None:
            data_stream_stage = OnDemandStage(env, name + "_data_stream", log_file=stage_log)
        else:
            data_stream_scheduler = rs_conf.data_stream_scheduler_generator(env)
            data_stream_cost_func = ScheduleUtil.get_cost_func(rs_conf.data_stream_stage_schedule_resource, phy_node)
            data_stream_stage = Stage(env, name + "_data_stream", rs_conf.num_data_streamers, data_stream_scheduler,
                                      data_stream_cost_func, log_file=stage_log)
        data_stream_stage.monitor_interval = rs_conf.stage_monitor_interval

        ack_process_stage = OnDemandStage(env, name + "_ack_process", log_file=stage_log)
        ack_process_stage.monitor_interval = rs_conf.stage_monitor_interval

        return rpc_read_stage, rpc_handle_stage, rpc_respond_stage, mem_flush_stage, log_append_stage, log_sync_stage, data_stream_stage, ack_process_stage

    @staticmethod
    def get_region_server(env, name, phy_node, hbase, rs_conf, stage_log=sys.stdout):
        """

        :param env:
        :param name:
        :param phy_node:
        :param hbase cluster this region server belongs to
        :param rs_conf: RegionServerConf object
        :param stage_log:
        :return:
        """
        rpc_read_stage, rpc_handle_stage, rpc_respond_stage, mem_flush_stage, log_append_stage, log_sync_stage, data_stream_stage, ack_process_stage = \
            RegionServer.construct_stages(env, name, phy_node, hbase, rs_conf, stage_log)
        region_server = RegionServer(env, phy_node, hbase, rpc_read_stage, rpc_handle_stage, rpc_respond_stage,
                                     mem_flush_stage,
                                     log_append_stage, log_sync_stage, data_stream_stage, ack_process_stage)

        return region_server


class SCRegionServer(RegionServer):
    """
    same as RegionServer, but perform short-cuicuited reads at the datanode's xceive stage (in hdfs), instead of
    in the rpc handle stage
    """

    def submit_req(self, hbase_req):
        nn_lookup_req = None
        nn_lookup_done = None
        if hbase_req.namenode_lookup_time > 0:
            nn_lookup_req, nn_lookup_done = self.hbase.hdfs.get_namenode_req(hbase_req.client_id,
                                                                             hbase_req.namenode_lookup_time)

        dn_read_req = None
        dn_read_done = None
        if hbase_req.datanode_read_size > 0:
            if hbase_req.short_circuit:
                dn_read_req, dn_read_done = self.hbase.hdfs.get_short_circuit_req(self.phy_node, hbase_req.client_id,
                                                                                  hbase_req.datanode_read_size)
            else:
                dn_read_req, dn_read_done = self.hbase.hdfs.get_read_req(hbase_req.client_id,
                                                                         hbase_req.datanode_read_size)

        log_sync_req = None
        if hbase_req.log_sync and hbase_req.log_append_size > 0:
            log_sync_req = StageReq(self.env, self.log_sync_stage, self.wal_id, {}, [], [])

        log_append_req = None
        if hbase_req.log_append_size > 0:
            log_append_req = StageReq(self.env, self.log_append_stage, hbase_req.client_id,
                                      {self.phy_node.cpu_res: RegionServer.LOG_APPEND_CPU_TIME}, [], [])
            log_write_req, log_write_done = self.get_hdfs_write_req(self.wal_id, hbase_req.log_append_size)
            log_append_req.downstream_reqs.append(log_write_req)
            if log_sync_req is not None:
                log_append_req.downstream_reqs.append(log_sync_req)
                log_sync_req.blocking_evts.append(log_write_done)

        mem_update_req = None
        if hbase_req.mem_update_size > 0:
            mem_update_req = UpdateMemStoreReq(self.mem_flush_stage, hbase_req.client_id, hbase_req.mem_update_size)

        # rpc respond stage
        rpc_respond_req = StageReq(self.env, self.rpc_respond_stage, hbase_req.client_id,
                                   {self.phy_node.cpu_res: RegionServer.SEND_REPLY_CPU_TIME,
                                    self.phy_node.net_res: hbase_req.reply_size}, [], [])

        # rpc handling stage
        rpc_handle_req = BlkStageReq(self.env, self.rpc_handle_stage, hbase_req.client_id)
        # First part of rpc handling: cpu processing, issue hdfs related requests and wait until they are done
        rpc_handle_res_cost = [{self.phy_node.cpu_res: hbase_req.rpc_time}]
        rpc_handle_res_profile = ResBlockingProfile(rpc_handle_res_cost)
        if nn_lookup_req is not None:
            rpc_handle_res_profile.add_downstream_req(nn_lookup_req)
            rpc_handle_res_profile.add_blocking_event(nn_lookup_done)
        rpc_handle_req.append_res_blk_profile(rpc_handle_res_profile)
        # Second part of rpc handling: issue hdfs reads and wait for the completion
        if dn_read_req is not None:
            dn_blocking_res_cost = [{}]
            dn_blocking_profile = ResBlockingProfile(dn_blocking_res_cost)
            dn_blocking_profile.add_downstream_req(dn_read_req)
            dn_blocking_profile.add_blocking_event(dn_read_done)
            rpc_handle_req.append_res_blk_profile(dn_blocking_profile)
        # Third part of rpc handling: process data back read from the datanode and append log
        rpc_handle_net_res = 0
        if hbase_req.datanode_read_size > 0 and not hbase_req.short_circuit:
            rpc_handle_net_res += hbase_req.datanode_read_size
        rpc_handle_cpu_res = 0
        if hbase_req.datanode_read_size > 0 and not hbase_req.short_circuit:
            rpc_handle_cpu_res += RegionServer.RECEIVE_DATA_CPU_TIME
        rpc_handle_res_cost2 = [{self.phy_node.cpu_res: rpc_handle_cpu_res, self.phy_node.net_res: rpc_handle_net_res}]
        rpc_handle_res_profile2 = ResBlockingProfile(rpc_handle_res_cost2)
        if log_append_req is not None:
            rpc_handle_res_profile2.add_downstream_req(log_append_req)
        if log_sync_req is not None:
            rpc_handle_res_profile2.add_blocking_event(log_sync_req.done)
        rpc_handle_req.append_res_blk_profile(rpc_handle_res_profile2)
        # Forth part of rpc handling: update mem
        rpc_handle_res_cost3 = [{}]
        rpc_handle_res_profile3 = ResBlockingProfile(rpc_handle_res_cost3)
        if mem_update_req is not None:
            rpc_handle_res_profile3.add_downstream_req(mem_update_req)
        rpc_handle_req.append_res_blk_profile(rpc_handle_res_profile3)
        # Final part of rpc handling: pass request to rpc_respond stage
        rpc_handle_res_cost4 = [{}]
        rpc_handle_res_profile4 = ResBlockingProfile(rpc_handle_res_cost4)
        rpc_handle_res_profile4.add_downstream_req(rpc_respond_req)
        rpc_handle_req.append_res_blk_profile(rpc_handle_res_profile4)

        # rpc read stage
        assert hbase_req.rpc_size > 0
        rpc_read_res_profile = {self.phy_node.cpu_res: RegionServer.READ_RPC_CPU_TIME,
                                self.phy_node.net_res: hbase_req.rpc_size}
        rpc_read_req = StageReq(self.env, self.rpc_read_stage, hbase_req.client_id, rpc_read_res_profile, [], [])
        rpc_read_req.downstream_reqs.append(rpc_handle_req)

        return rpc_read_req.submit(), rpc_respond_req.done

    @staticmethod
    def get_region_server(env, name, phy_node, hbase, rs_conf, stage_log=sys.stdout):
        """

        :param env:
        :param name:
        :param phy_node:
        :param hbase: HBase this region server belongs to
        :param rs_conf: RegionServerConf object
        :param stage_log:
        :return:
        """
        rpc_read_stage, rpc_handle_stage, rpc_respond_stage, mem_flush_stage, log_append_stage, log_sync_stage, \
        data_stream_stage, ack_process_stage = \
            RegionServer.construct_stages(env, name, phy_node, hbase, rs_conf, stage_log)

        region_server = SCRegionServer(env, phy_node, hbase, rpc_read_stage, rpc_handle_stage, rpc_respond_stage,
                                       mem_flush_stage,
                                       log_append_stage, log_sync_stage, data_stream_stage, ack_process_stage)
        return region_server


class SpeculativeRegionServer(RegionServer):
    """
    Same as RegionServer, but in addition to normally executing requests, rpc_handle stage will speculatively executing
    requests from client 2 (client_id = 2).
    These speculatively executed requests will be aborted and subject to normal scheduling if they are found to require
    I/O during processing (here we assume CPU and network are idle but I/O is in contention, so SpeculativeRegioNServer
    will opportunistically execute requests to see if they only require CPU/network, and we assume that client 2 mostly
    issues CPU-intensive requests (so we will speculative exectue its requests).

    FIXME: too many things are hard coded; need a more generic version of SpeculativeRegionServer
    """

    def __init__(self, env, phy_node, hbase, rpc_read_stage, rpc_handle_stage, rpc_respond_stage, mem_flush_stage,
                 log_append_stage,
                 log_sync_stage, data_stream_stage, ack_process_stage):
        self.env = env
        self.phy_node = phy_node
        self.hbase = hbase
        self.wal_id = 9999
        self.rpc_read_stage = rpc_read_stage
        self.rpc_handle_stage = rpc_handle_stage
        self.rpc_respond_stage = rpc_respond_stage
        self.mem_flush_stage = mem_flush_stage
        mem_flush_stage.region_server = self
        self.log_append_stage = log_append_stage
        self.log_sync_stage = log_sync_stage
        self.data_stream_stage = data_stream_stage
        self.ack_process_stage = ack_process_stage

    @staticmethod
    def construct_stages(env, name, phy_node, hbase, rs_conf, stage_log, async_flush=False):
        rpc_read_scheduler = rs_conf.rpc_read_scheduler_generator(env)
        rpc_read_cost_func = RegionServer.get_cost_func(rs_conf.rpc_read_stage_schedule_resource, phy_node)
        rpc_read_stage = Stage(env, name + "_rpc_read", rs_conf.num_rpc_readers, rpc_read_scheduler,
                               rpc_read_cost_func, log_file=stage_log)
        rpc_read_stage.monitor_interval = rs_conf.stage_monitor_interval

        speculate_client_id = 2
        rpc_handle_scheduler = rs_conf.rpc_handle_scheduler_generator(env)
        rpc_handle_cost_func = RegionServer.get_cost_func(rs_conf.rpc_handle_stage_schedule_resource, phy_node)
        rpc_handle_stage = SpeculativeExecStage(env, "rpc_handle", phy_node.io_res, speculate_client_id,
                                                rs_conf.num_rpc_handlers,
                                                rpc_handle_scheduler, rpc_handle_cost_func, log_file=stage_log)
        rpc_handle_stage.monitor_interval = rs_conf.stage_monitor_interval

        rpc_respond_scheduler = rs_conf.rpc_respond_scheduler_generator(env)
        rpc_respond_cost_func = RegionServer.get_cost_func(rs_conf.rpc_respond_stage_schedule_resource, phy_node)
        rpc_respond_stage = Stage(env, name + "_rpc_respond", rs_conf.num_rpc_responders, rpc_respond_scheduler,
                                  rpc_respond_cost_func, log_file=stage_log)
        rpc_respond_stage.monitor_interval = rs_conf.stage_monitor_interval

        mem_store = MemStore(env, rs_conf.total_mem_size, rs_conf.mem_quota, rs_conf.flush_threshold)
        if async_flush:
            mem_flush_stage = AsyncMemFlushStage(env, None, name + "_mem_flush", rs_conf.num_mem_flushers, mem_store,
                                                 log_file=stage_log)
        else:
            mem_flush_stage = MemFlushStage(env, None, name + "_mem_flush", rs_conf.num_mem_flushers, mem_store,
                                            log_file=stage_log)
        mem_flush_stage.monitor_interval = rs_conf.stage_monitor_interval
        if is_feedback_scheduler(rpc_handle_scheduler):
            rpc_handle_scheduler.set_downstream_stage(mem_flush_stage)

        # single ring buffer consumer, FIFO order
        log_append_stage = Stage(env, name + "_log_append", 1, FIFOScheduler(env, float('inf')), None,
                                 log_file=stage_log)
        log_append_stage.monitor_interval = rs_conf.stage_monitor_interval

        # log sync can be processed at any order; it waits for the finishing of writes that are already issued
        # but here we use FIFO, it doesn't really matter because it does not use any resources
        log_sync_stage = Stage(env, name + "_log_sync", rs_conf.num_log_syncers, FIFOScheduler(env, float('inf')), None,
                               log_file=stage_log)
        log_sync_stage.monitor_interval = rs_conf.stage_monitor_interval

        if rs_conf.data_stream_scheduler_generator is None:
            data_stream_stage = OnDemandStage(env, name + "_data_stream", log_file=stage_log)
        else:
            data_stream_scheduler = rs_conf.data_stream_scheduler_generator(env)
            data_stream_cost_func = ScheduleUtil.get_cost_func(rs_conf.data_stream_stage_schedule_resource, phy_node)
            data_stream_stage = Stage(env, name + "_data_stream", rs_conf.num_data_streamers, data_stream_scheduler,
                                      data_stream_cost_func, log_file=stage_log)
        data_stream_stage.monitor_interval = rs_conf.stage_monitor_interval

        ack_process_stage = OnDemandStage(env, name + "_ack_process", log_file=stage_log)
        ack_process_stage.monitor_interval = rs_conf.stage_monitor_interval

        return rpc_read_stage, rpc_handle_stage, rpc_respond_stage, mem_flush_stage, log_append_stage, log_sync_stage, \
               data_stream_stage, ack_process_stage

    @staticmethod
    def get_region_server(env, name, phy_node, hbase, rs_conf, stage_log=sys.stdout):
        rpc_read_stage, rpc_handle_stage, rpc_respond_stage, mem_flush_stage, log_append_stage, log_sync_stage, data_stream_stage, ack_process_stage = \
            SpeculativeRegionServer.construct_stages(env, name, phy_node, hbase, rs_conf, stage_log)
        region_server = SpeculativeRegionServer(env, phy_node, hbase, rpc_read_stage, rpc_handle_stage,
                                                rpc_respond_stage, mem_flush_stage,
                                                log_append_stage, log_sync_stage, data_stream_stage, ack_process_stage)
        return region_server


class AsyncFlusherRegionServer(SCRegionServer):
    @staticmethod
    def get_region_server(env, name, phy_node, hbase, rs_conf, stage_log=sys.stdout):
        rpc_read_stage, rpc_handle_stage, rpc_respond_stage, mem_flush_stage, log_append_stage, log_sync_stage, \
        data_stream_stage, ack_process_stage = \
            RegionServer.construct_stages(env, name, phy_node, hbase, rs_conf, stage_log, async_flush=True)
        region_server = AsyncFlusherRegionServer(env, phy_node, hbase, rpc_read_stage, rpc_handle_stage,
                                                 rpc_respond_stage,
                                                 mem_flush_stage, log_append_stage, log_sync_stage, data_stream_stage,
                                                 ack_process_stage)
        return region_server


class AsyncHandlerRegionServer(SCRegionServer):
    """
    same as SCRegionServer, but rpc_handle stage does not block on datanode io or namenode lookup,
    instead doing them asynchronously
    """

    def submit_req(self, hbase_req):

        log_sync_req = None
        if hbase_req.log_sync and hbase_req.log_append_size > 0:
            assert hbase_req.log_append_size > 0
            log_sync_req = StageReq(self.env, self.log_sync_stage, self.wal_id, {}, [], [])

        log_append_req = None
        if hbase_req.log_append_size > 0:
            log_append_req = StageReq(self.env, self.log_append_stage, hbase_req.client_id,
                                      {self.phy_node.cpu_res: RegionServer.LOG_APPEND_CPU_TIME}, [], [])
            log_write_req, log_write_done = self.get_hdfs_write_req(self.wal_id, hbase_req.log_append_size)
            log_append_req.downstream_reqs.append(log_write_req)
            if log_sync_req is not None:
                log_append_req.downstream_reqs.append(log_sync_req)
                log_sync_req.blocking_evts.append(log_write_done)

        mem_update_req = None
        if hbase_req.mem_update_size > 0:
            mem_update_req = UpdateMemStoreReq(self.mem_flush_stage, hbase_req.client_id, hbase_req.mem_update_size)

        # processing in rpc handle stage after the data read from datanode is ready, including:
        # 1. processing the data (receive it via network, incur network and cpu cost)
        # 2. if needed, append to the wal log and wait until log is synchronized
        # 3. send reply back to the client
        #
        # note: here we simplify a bit and reply to client directly in the rpc_handle stage (instead of passing the
        # reply to the rpc_respond stage).
        # The HBase implementation does this too when network is idle.
        # FIXME-yansuli: move sending reply to the rpc_respond stage
        rpc_reply_req = BlkStageReq(self.env, self.rpc_handle_stage, hbase_req.client_id)
        # First part of rpc replying: get data back from the datanode and append log
        rpc_reply_net_res = 0
        if hbase_req.datanode_read_size > 0 and not hbase_req.short_circuit:
            rpc_reply_net_res += hbase_req.datanode_read_size
        rpc_reply_cpu_res = 0
        if hbase_req.datanode_read_size > 0 and not hbase_req.short_circuit:
            rpc_reply_cpu_res += RegionServer.RECEIVE_DATA_CPU_TIME
        rpc_reply_res_cost1 = [{self.phy_node.cpu_res: rpc_reply_cpu_res, self.phy_node.net_res: rpc_reply_net_res}]
        rpc_reply_res_profile1 = ResBlockingProfile(rpc_reply_res_cost1)
        if log_append_req is not None:
            rpc_reply_res_profile1.add_downstream_req(log_append_req)
        if log_sync_req is not None:
            rpc_reply_res_profile1.add_blocking_event(log_sync_req.done)
        rpc_reply_req.append_res_blk_profile(rpc_reply_res_profile1)
        # Second part of rpc replying: update mem and reply back to client
        rpc_reply_net_res = hbase_req.reply_size
        rpc_reply_cpu_res = RegionServer.SEND_REPLY_CPU_TIME
        rpc_reply_res_cost2 = [{self.phy_node.cpu_res: rpc_reply_cpu_res, self.phy_node.net_res: rpc_reply_net_res}]
        rpc_reply_res_profile2 = ResBlockingProfile(rpc_reply_res_cost2)
        if mem_update_req is not None:
            rpc_reply_res_profile2.add_downstream_req(mem_update_req)
        rpc_reply_req.append_res_blk_profile(rpc_reply_res_profile2)

        # datanode read
        dn_read_req = None
        if hbase_req.datanode_read_size > 0:
            if hbase_req.short_circuit:
                dn_read_req, dn_read_done = self.hbase.hdfs.get_short_circuit_req(self.phy_node, hbase_req.client_id,
                                                                                  hbase_req.datanode_read_size)
            else:
                dn_read_req, dn_read_done = self.hbase.hdfs.get_read_req(hbase_req.client_id,
                                                                         hbase_req.datanode_read_size)
            # Note: here we are simplifying a bit, when nn_lookup is completed, rpc_handle should be involved to issue
            # rpc_reply_req, but here we omitted this part, assuming it will take no time (all cpu cost are aggregated
            # in the rpc_time)
            dn_read_done.callbacks.append(lambda evt: self.env.process(rpc_reply_req.wait_submit()))

        # namenode look up
        nn_lookup_req = None
        if hbase_req.namenode_lookup_time > 0:
            nn_lookup_req, nn_lookup_done = self.hbase.hdfs.get_namenode_req(hbase_req.client_id,
                                                                             hbase_req.namenode_lookup_time)
            # Note: here we are simplifying a bit, when nn_lookup is completed, rpc_handle should be involved to issue
            # dn_read_req, but here we omitted this part, assuming it will take no time (all cpu cost are aggregated in
            # the rpc_time)
            if dn_read_req is not None:
                nn_lookup_req.downstream_reqs.append(dn_read_req)
            else:
                nn_lookup_req.downstream_reqs.append(rpc_reply_req)

        # initial processing in rpc_handle stage
        rpc_process_req = StageReq(self.env, self.rpc_handle_stage, hbase_req.client_id,
                                   {self.phy_node.cpu_res: hbase_req.rpc_time}, [], [])
        if nn_lookup_req is not None:
            rpc_process_req.downstream_reqs.append(nn_lookup_req)
        elif dn_read_req is not None:
            rpc_process_req.downstream_reqs.append(dn_read_req)
        else:
            rpc_process_req.downstream_reqs.append(rpc_reply_req)

        # rpc read stage
        rpc_read_res_profile = {self.phy_node.cpu_res: RegionServer.READ_RPC_CPU_TIME,
                                self.phy_node.net_res: hbase_req.rpc_size}
        rpc_read_req = StageReq(self.env, self.rpc_read_stage, hbase_req.client_id, rpc_read_res_profile, [], [])
        rpc_read_req.downstream_reqs.append(rpc_process_req)

        return rpc_read_req.submit(), rpc_reply_req.done

    @staticmethod
    def get_region_server(env, name, phy_node, hbase, rs_conf, stage_log=sys.stdout):
        rpc_read_stage, rpc_handle_stage, rpc_respond_stage, mem_flush_stage, log_append_stage, log_sync_stage, data_stream_stage, ack_process_stage = \
            RegionServer.construct_stages(env, name, phy_node, hbase, rs_conf, stage_log)
        region_server = AsyncHandlerRegionServer(env, phy_node, hbase, rpc_read_stage, rpc_handle_stage,
                                                 rpc_respond_stage,
                                                 mem_flush_stage, log_append_stage, log_sync_stage, data_stream_stage,
                                                 ack_process_stage)
        return region_server


class SchedHandlerTimeRegionServer(SCRegionServer):
    """
    Same as SCRegionServer, however, the rpc handle scheduler will schedule the rpc handler time as a resource, as an
    attempt to solve the blocking problem in the rpc handle stage.

    When using SchedHandlerTimeRegionServer, the rpc handle scheduler has to be DRF (considered resources are handler
    time, cpu, io and network).

    Note: Schedule handling time, though works with regular downstream requests blocking, is inefficient in solving the
    blocking problem caused by mem quota/pressure
    this is because usually memory updates happen fast, during which period the handle time cost (which will be accounted
    when only when the flushing is completed) is not reflected, so one client who is blocked due to memory quota and whose
    flush is in progress (or a few of them) tends to be selected repeatedly and monopolize the handlers
    cpu, io, network and rpc handle time).
    """

    @staticmethod
    def get_handle_cost_func(rs_conf, phy_node):
        """

        :param rs_conf:
        :param phy_node:
        :return: lambda function, which maps a request to a four vector [handle time, cpu, io, net].
                 cpu, io, and net are normalized to the phy_node's total cpu/io/network.
        """
        to_schedule = rs_conf.rpc_handle_stage_schedule_resource
        if to_schedule == "handle_time":
            return lambda req: [req.service_time]
        if to_schedule == "cpu":
            return lambda req: [
                req.get_res_consumption(phy_node.cpu_res) / phy_node.cpu_res.rate / len(phy_node.cpu_res.workers),
                req.service_time / rs_conf.num_rpc_handlers]
        if to_schedule == "net":
            return lambda req: [
                req.get_res_consumption(phy_node.net_res) / phy_node.net_res.rate / len(phy_node.net_res.workers),
                req.service_time / rs_conf.num_rpc_handlers]
        if to_schedule == "io":
            return lambda req: [
                req.get_res_consumption(phy_node.io_res) / phy_node.io_res.rate / len(phy_node.io_res.workers),
                req.service_time / rs_conf.num_rpc_handlers]
        elif to_schedule == "all":
            return lambda req: [
                req.get_res_consumption(phy_node.cpu_res) / (phy_node.cpu_res.rate * len(phy_node.cpu_res.workers)),
                req.get_res_consumption(phy_node.io_res) / (phy_node.io_res.rate * len(phy_node.io_res.workers)),
                req.get_res_consumption(phy_node.net_res) / (phy_node.net_res.rate * len(phy_node.net_res.workers)),
                req.service_time / rs_conf.num_rpc_handlers]
        else:
            raise Exception("Don't know this!")

    @staticmethod
    def get_region_server(env, name, phy_node, hbase, rs_conf, stage_log=sys.stdout):
        rpc_read_scheduler = rs_conf.rpc_read_scheduler_generator(env)
        rpc_read_cost_func = RegionServer.get_cost_func(rs_conf.rpc_read_stage_schedule_resource, phy_node)
        rpc_read_stage = Stage(env, name + "_rpc_read", rs_conf.num_rpc_readers, rpc_read_scheduler,
                               rpc_read_cost_func, log_file=stage_log)
        rpc_read_stage.monitor_interval = rs_conf.stage_monitor_interval

        rpc_handle_scheduler = rs_conf.rpc_handle_scheduler_generator(env)
        assert is_after_scheduler(rpc_handle_scheduler)
        rpc_handle_cost_func = SchedHandlerTimeRegionServer.get_handle_cost_func(rs_conf, phy_node)
        # here we use AfterCostStage, because the rpc handle time a request consumed is only known after processing
        # this request
        rpc_handle_stage = AfterCostStage(env, name + "_rpc_handle", rs_conf.num_rpc_handlers, rpc_handle_scheduler,
                                          rpc_handle_cost_func, log_file=stage_log)
        rpc_handle_stage.monitor_interval = rs_conf.stage_monitor_interval

        rpc_respond_scheduler = rs_conf.rpc_respond_scheduler_generator(env)
        rpc_respond_cost_func = RegionServer.get_cost_func(rs_conf.rpc_respond_stage_schedule_resource, phy_node)
        rpc_respond_stage = Stage(env, name + "_rpc_respond", rs_conf.num_rpc_responders, rpc_respond_scheduler,
                                  rpc_respond_cost_func, log_file=stage_log)
        rpc_respond_stage.monitor_interval = rs_conf.stage_monitor_interval

        mem_store = MemStore(env, rs_conf.total_mem_size, rs_conf.mem_quota, rs_conf.flush_threshold)
        mem_flush_stage = MemFlushStage(env, None, name + "_mem_flush", rs_conf.num_mem_flushers, mem_store,
                                        log_file=stage_log)
        mem_flush_stage.monitor_interval = rs_conf.stage_monitor_interval
        if is_feedback_scheduler(rpc_handle_scheduler):
            rpc_handle_scheduler.set_downstream_stage(mem_flush_stage)

        log_append_stage = Stage(env, name + "_log_append", 1, FIFOScheduler(env, float('inf')), None,
                                 log_file=stage_log)
        log_append_stage.monitor_interval = rs_conf.stage_monitor_interval

        log_sync_stage = Stage(env, name + "_log_sync", rs_conf.num_log_syncers, FIFOScheduler(env, float('inf')), None,
                               log_file=stage_log)
        log_sync_stage.monitor_interval = rs_conf.stage_monitor_interval

        if rs_conf.data_stream_scheduler_generator is None:
            data_stream_stage = OnDemandStage(env, name + "_data_stream", log_file=stage_log)
        else:
            data_stream_scheduler = rs_conf.data_stream_scheduler_generator(env)
            data_stream_cost_func = ScheduleUtil.get_cost_func(rs_conf.data_stream_stage_schedule_resource, phy_node)
            data_stream_stage = Stage(env, name + "_data_stream", rs_conf.num_data_streamers, data_stream_scheduler,
                                      data_stream_cost_func, log_file=stage_log)
        data_stream_stage.monitor_interval = rs_conf.stage_monitor_interval

        ack_process_stage = OnDemandStage(env, name + "_ack_process", log_file=stage_log)
        ack_process_stage.monitor_interval = rs_conf.stage_monitor_interval

        region_server = SchedHandlerTimeRegionServer(env, phy_node, hbase, rpc_read_stage, rpc_handle_stage,
                                                     rpc_respond_stage, mem_flush_stage, log_append_stage,
                                                     log_sync_stage, data_stream_stage, ack_process_stage)
        return region_server


class SchedHandlerSepWALRegionServer(SchedHandlerTimeRegionServer):
    """
    Same as SchedHandlerTimeRegionServer, but we maintain separate WAL for each client (the log append request will
    have client_id from each client, not a generic wal_id, and the scheduler can decide which client's request to
    process instead of having to enforce FIFO order across all clients).
    """

    def submit_req(self, hbase_req):
        """
        Note: here we again make the simplification and send rpc reply at the rpc_handle stage

        :param hbase_req:
        :return: hbase_req submission and completion event
        """
        nn_lookup_req = None
        nn_lookup_done = None
        if hbase_req.namenode_lookup_time > 0:
            nn_lookup_req, nn_lookup_done = self.hbase.hdfs.get_namenode_req(hbase_req.client_id,
                                                                             hbase_req.namenode_lookup_time)

        dn_read_req = None
        dn_read_done = None
        if hbase_req.datanode_read_size > 0:
            if hbase_req.short_circuit:
                dn_read_req, dn_read_done = self.hbase.hdfs.get_short_circuit_req(self.phy_node, hbase_req.client_id,
                                                                                  hbase_req.datanode_read_size)
            else:
                dn_read_req, dn_read_done = self.hbase.hdfs.get_read_req(hbase_req.client_id,
                                                                         hbase_req.datanode_read_size)

        log_sync_req = None
        if hbase_req.log_sync and hbase_req.log_append_size > 0:
            log_sync_req = StageReq(self.env, self.log_sync_stage, hbase_req.client_id, {}, [], [])

        log_append_req = None
        if hbase_req.log_append_size > 0:
            log_append_req = StageReq(self.env, self.log_append_stage, hbase_req.client_id,
                                      {self.phy_node.cpu_res: RegionServer.LOG_APPEND_CPU_TIME}, [], [])
            log_write_req, log_write_done = self.get_hdfs_write_req(hbase_req.client_id, hbase_req.log_append_size)
            log_append_req.downstream_reqs.append(log_write_req)
            if log_sync_req is not None:
                log_append_req.downstream_reqs.append(log_sync_req)
                log_sync_req.blocking_evts.append(log_write_done)

        mem_update_req = None
        if hbase_req.mem_update_size > 0:
            mem_update_req = UpdateMemStoreReq(self.mem_flush_stage, hbase_req.client_id, hbase_req.mem_update_size)

        # rpc handling stage
        rpc_handle_req = BlkStageReq(self.env, self.rpc_handle_stage, hbase_req.client_id)
        # First part of rpc handling: cpu processing, issue hdfs related requests and wait until they are done
        rpc_handle_res_cost = [{self.phy_node.cpu_res: hbase_req.rpc_time}]
        rpc_handle_res_profile = ResBlockingProfile(rpc_handle_res_cost)
        if nn_lookup_req is not None:
            rpc_handle_res_profile.add_downstream_req(nn_lookup_req)
            rpc_handle_res_profile.add_blocking_event(nn_lookup_done)
        rpc_handle_req.append_res_blk_profile(rpc_handle_res_profile)
        # issuing and blocking on dn_read_req would be another ResBlockingProfile
        # so that it happens after the nn_lookup_req
        if dn_read_req is not None:
            dn_blocking_res_cost = [{}]
            dn_blocking_profile = ResBlockingProfile(dn_blocking_res_cost)
            dn_blocking_profile.add_downstream_req(dn_read_req)
            dn_blocking_profile.add_blocking_event(dn_read_done)
            rpc_handle_req.append_res_blk_profile(dn_blocking_profile)
        # Second part of rpc handling: get data back from the datanode and append log
        rpc_handle_net_res = 0
        if hbase_req.datanode_read_size > 0 and not hbase_req.short_circuit:
            rpc_handle_net_res += hbase_req.datanode_read_size
        rpc_handle_cpu_res = 0
        if hbase_req.datanode_read_size > 0 and not hbase_req.short_circuit:
            rpc_handle_cpu_res += RegionServer.RECEIVE_DATA_CPU_TIME
        rpc_handle_res_cost2 = [{self.phy_node.cpu_res: rpc_handle_cpu_res, self.phy_node.net_res: rpc_handle_net_res}]
        rpc_handle_res_profile2 = ResBlockingProfile(rpc_handle_res_cost2)
        rpc_handle_req.append_res_blk_profile(rpc_handle_res_profile2)
        if log_append_req is not None:
            rpc_handle_res_profile2.add_downstream_req(log_append_req)
        if log_sync_req is not None:
            rpc_handle_res_profile2.add_blocking_event(log_sync_req.done)
        rpc_handle_res_profile2 = ResBlockingProfile(rpc_handle_res_cost2)
        # Third part of rpc handling: update mem and reply back to client
        rpc_handle_net_res = hbase_req.reply_size
        rpc_handle_cpu_res = RegionServer.SEND_REPLY_CPU_TIME
        rpc_handle_res_cost3 = [{self.phy_node.cpu_res: rpc_handle_cpu_res, self.phy_node.net_res: rpc_handle_net_res}]
        rpc_handle_res_profile3 = ResBlockingProfile(rpc_handle_res_cost3)
        if mem_update_req is not None:
            rpc_handle_res_profile3.add_downstream_req(mem_update_req)
        rpc_handle_req.append_res_blk_profile(rpc_handle_res_profile3)

        # rpc read stage
        assert hbase_req.rpc_size > 0
        rpc_read_res_profile = {self.phy_node.cpu_res: RegionServer.READ_RPC_CPU_TIME,
                                self.phy_node.net_res: hbase_req.rpc_size}
        rpc_read_req = StageReq(self.env, self.rpc_read_stage, hbase_req.client_id, rpc_read_res_profile, [], [])
        rpc_read_req.downstream_reqs.append(rpc_handle_req)

        return rpc_read_req.submit(), rpc_handle_req.done

    @staticmethod
    def get_region_server(env, name, phy_node, hbase, rs_conf, stage_log=sys.stdout):
        rpc_read_scheduler = rs_conf.rpc_read_scheduler_generator(env)
        rpc_read_cost_func = RegionServer.get_cost_func(rs_conf.rpc_read_stage_schedule_resource, phy_node)
        rpc_read_stage = Stage(env, name + "_rpc_read", rs_conf.num_rpc_readers, rpc_read_scheduler,
                               rpc_read_cost_func, log_file=stage_log)
        rpc_read_stage.monitor_interval = rs_conf.stage_monitor_interval

        rpc_handle_scheduler = rs_conf.rpc_handle_scheduler_generator(env)
        assert is_after_scheduler(rpc_handle_scheduler)
        rpc_handle_cost_func = SchedHandlerTimeRegionServer.get_handle_cost_func(rs_conf, phy_node)
        rpc_handle_stage = AfterCostStage(env, name + "_rpc_handle", rs_conf.num_rpc_handlers, rpc_handle_scheduler,
                                          rpc_handle_cost_func, log_file=stage_log)
        rpc_handle_stage.monitor_interval = rs_conf.stage_monitor_interval

        rpc_respond_scheduler = rs_conf.rpc_respond_scheduler_generator(env)
        rpc_respond_cost_func = RegionServer.get_cost_func(rs_conf.rpc_respond_stage_schedule_resource, phy_node)
        rpc_respond_stage = Stage(env, name + "_rpc_respond", rs_conf.num_rpc_responders, rpc_respond_scheduler,
                                  rpc_respond_cost_func, log_file=stage_log)
        rpc_respond_stage.monitor_interval = rs_conf.stage_monitor_interval

        mem_store = MemStore(env, rs_conf.total_mem_size, rs_conf.mem_quota, rs_conf.flush_threshold)
        mem_flush_stage = MemFlushStage(env, None, name + "_mem_flush", rs_conf.num_mem_flushers, mem_store,
                                        log_file=stage_log)
        mem_flush_stage.monitor_interval = rs_conf.stage_monitor_interval
        if is_feedback_scheduler(rpc_handle_scheduler):
            rpc_handle_scheduler.set_downstream_stage(mem_flush_stage)

        log_append_scheduler = rs_conf.log_append_scheduler_generator(env)
        log_append_cost_func = RegionServer.get_cost_func(rs_conf.log_append_stage_schedule_resource, phy_node)
        log_append_stage = Stage(env, name + "_log_append", 1, log_append_scheduler, log_append_cost_func,
                                 log_file=stage_log)
        log_append_stage.monitor_interval = rs_conf.stage_monitor_interval

        log_sync_stage = Stage(env, name + "_log_sync", rs_conf.num_log_syncers, FIFOScheduler(env, float('inf')), None,
                               log_file=stage_log)
        log_sync_stage.monitor_interval = rs_conf.stage_monitor_interval

        if rs_conf.data_stream_scheduler_generator is None:
            data_stream_stage = OnDemandStage(env, name + "_data_stream", log_file=stage_log)
        else:
            data_stream_scheduler = rs_conf.data_stream_scheduler_generator(env)
            data_stream_cost_func = ScheduleUtil.get_cost_func(rs_conf.data_stream_stage_schedule_resource, phy_node)
            data_stream_stage = Stage(env, name + "_data_stream", rs_conf.num_data_streamers, data_stream_scheduler,
                                      data_stream_cost_func, log_file=stage_log)
        data_stream_stage.monitor_interval = rs_conf.stage_monitor_interval

        ack_process_stage = OnDemandStage(env, name + "_ack_process", log_file=stage_log)
        ack_process_stage.monitor_interval = rs_conf.stage_monitor_interval

        region_server = SchedHandlerSepWALRegionServer(env, phy_node, hbase, rpc_read_stage, rpc_handle_stage,
                                                       rpc_respond_stage, mem_flush_stage, log_append_stage,
                                                       log_sync_stage,
                                                       data_stream_stage, ack_process_stage)
        return region_server


class ActiveHandlerRegionServer(SCRegionServer):
    """
    Same as SCRegionServer, but instead of having fixed number of rpc handlers, it maintains the number of active (not
    currently blocked) rpc handlers.

    This is an attempt to solve the blocking problem at the rpc handle stage; however, it does not work very well
    because you may need *a lot of* rpc handlers since many of them are blocked, which causes large overhead.
    """

    @staticmethod
    def get_region_server(env, name, phy_node, hbase, rs_conf, stage_log=sys.stdout):
        """

        :param env:
        :param name:
        :param phy_node:
        :param hbase:
        :param rs_conf:
        :param stage_log:
        :return:
        """
        rpc_read_scheduler = rs_conf.rpc_read_scheduler_generator(env)
        rpc_read_cost_func = RegionServer.get_cost_func(rs_conf.rpc_read_stage_schedule_resource, phy_node)
        rpc_read_stage = Stage(env, name + "_rpc_read", rs_conf.num_rpc_readers, rpc_read_scheduler,
                               rpc_read_cost_func, log_file=stage_log)
        rpc_read_stage.monitor_interval = rs_conf.stage_monitor_interval

        rpc_handle_scheduler = rs_conf.rpc_handle_scheduler_generator(env)
        rpc_handle_cost_func = RegionServer.get_cost_func(rs_conf.rpc_handle_stage_schedule_resource, phy_node)
        rpc_handle_stage = ActiveWorkerStage(env, name + "_rpc_handle", rs_conf.num_rpc_handlers, rpc_handle_scheduler,
                                             rpc_handle_cost_func, log_file=stage_log)
        rpc_handle_stage.monitor_interval = rs_conf.stage_monitor_interval

        rpc_respond_scheduler = rs_conf.rpc_respond_scheduler_generator(env)
        rpc_respond_cost_func = RegionServer.get_cost_func(rs_conf.rpc_respond_stage_schedule_resource, phy_node)
        rpc_respond_stage = Stage(env, name + "_rpc_respond", rs_conf.num_rpc_responders, rpc_respond_scheduler,
                                  rpc_respond_cost_func, log_file=stage_log)
        rpc_respond_stage.monitor_interval = rs_conf.stage_monitor_interval

        mem_store = MemStore(env, rs_conf.total_mem_size, rs_conf.mem_quota, rs_conf.flush_threshold)
        mem_flush_stage = MemFlushStage(env, None, name + "_mem_flush", rs_conf.num_mem_flushers, mem_store,
                                        log_file=stage_log)
        mem_flush_stage.monitor_interval = rs_conf.stage_monitor_interval
        if is_feedback_scheduler(rpc_handle_scheduler):
            rpc_handle_scheduler.set_downstream_stage(mem_flush_stage)

        log_append_stage = Stage(env, name + "_log_append", 1, FIFOScheduler(env, float('inf')), None,
                                 log_file=stage_log)
        log_sync_stage = Stage(env, name + "_log_sync", rs_conf.num_log_syncers, FIFOScheduler(env, float('inf')), None,
                               log_file=stage_log)

        if rs_conf.data_stream_scheduler_generator is None:
            data_stream_stage = OnDemandStage(env, name + "_data_stream", log_file=stage_log)
        else:
            data_stream_scheduler = rs_conf.data_stream_scheduler_generator(env)
            data_stream_cost_func = ScheduleUtil.get_cost_func(rs_conf.data_stream_stage_schedule_resource)
            data_stream_stage = Stage(env, name + "_data_stream", rs_conf.num_data_streamers, data_stream_scheduler,
                                      data_stream_cost_func, log_file=stage_log)
        data_stream_stage.monitor_interval = rs_conf.stage_monitor_interval

        ack_process_stage = OnDemandStage(env, name + "_ack_process", log_file=stage_log)
        ack_process_stage.monitor_interval = rs_conf.stage_monitor_interval

        region_server = ActiveHandlerRegionServer(env, phy_node, hbase, rpc_read_stage, rpc_handle_stage,
                                                  rpc_respond_stage,
                                                  mem_flush_stage, log_append_stage, log_sync_stage, data_stream_stage,
                                                  ack_process_stage)
        return region_server


class OneStagePerResRegionServer(SCRegionServer):
    """
    OneStagePerResRegionServer has separate cpu_stage, net_stage and io_stage, which handles cpu-, network- and
    io-intensive activities. This is an attempt to solve the hidden contention problem in hbase.
    OneStagePerResRegionServer can only be used in OneStagePerResHBase.

    For example, read/write is handled in the io_stage (instead of the hdfs xceive stage); reading rpc/sending rpc
    replies is handled in the net_stage (instead of the rpc_read or rpc_respond stage).
    The log_sync, log_append, packet_ack and ack_process stages are still kept.

    FIXME-yangsuli: for now OneStagePerResRegionServer does not do namenode_lookup or memstore update
    habase_req.namenode_lookup_time and hbase_req.mem_update_size will be ignored; the corresponding resources are
    not consumed; as a result, no memory flush will be initiated neither (thus no mem_flush stage)
    """

    def __init__(self, env, phy_node, hbase, cpu_stage, net_stage, io_stage, log_sync_stage,
                 log_append_stage, packet_ack_stage, ack_process_stage):
        """

        :param env:
        :param phy_node:
        :param hbase:
        :type hbase: OneStagePerResHBase
        :param cpu_stage:
        :param net_stage:
        :param io_stage:
        :param log_sync_stage:
        :param log_append_stage:
        :param packet_ack_stage:
        :param ack_process_stage:
        """
        assert isinstance(hbase, OneStagePerResHBase)
        self.env = env
        self.phy_node = phy_node
        self.wal_id = 9999
        self.hbase = hbase
        self.cpu_stage = cpu_stage
        self.net_stage = net_stage
        self.io_stage = io_stage
        self.log_sync_stage = log_sync_stage
        self.log_append_stage = log_append_stage
        self.packet_ack_stage = packet_ack_stage
        self.ack_process_stage = ack_process_stage

    def get_write_req(self, client_id, write_size, downstream_list):
        """
        Just as hdfs writes, data are streamed to the downstream regionservers for replication

        :param client_id:
        :param write_size:
        :param downstream_list:
        :return: request and completion events
        """
        io_req = StageReq(self.env, self.io_stage, client_id, {self.phy_node.io_res: write_size}, [], [])
        packet_ack_req = StageReq(self.env, self.packet_ack_stage, client_id, {}, [], [])
        io_req.downstream_reqs.append(packet_ack_req)

        if len(downstream_list) != 0:
            downstream_node = downstream_list[0]
            d_io_req, d_packet_ack_done = downstream_node.get_write_req(client_id, write_size,
                                                                        [] if len(
                                                                            downstream_list) == 1 else downstream_list[
                                                                                                       1:])
            io_req.downstream_reqs.append(d_io_req)
            packet_ack_req.blocking_evts.append(d_packet_ack_done)

        return io_req, packet_ack_req.done

    def submit_req(self, hbase_req):
        dn_read_req = None
        dn_read_done = None
        if hbase_req.datanode_read_size > 0:
            if hbase_req.short_circuit:
                dn_read_req = StageReq(self.env, self.io_stage, hbase_req.client_id, {self.phy_node.io_res:
                                                                                          hbase_req.datanode_read_size},
                                       [], [])
                dn_read_done = dn_read_req.done
            else:
                dn_read_req, dn_read_done = self.hbase.get_hbase_read_req(self, hbase_req.client_id,
                                                                          hbase_req.datanode_read_size)

        log_sync_req = None
        if hbase_req.log_sync and hbase_req.log_append_size > 0:
            log_sync_req = StageReq(self.env, self.log_sync_stage, self.wal_id, {}, [], [])

        log_append_req = None
        if hbase_req.log_append_size > 0:
            log_append_req = StageReq(self.env, self.log_append_stage, hbase_req.client_id,
                                      {self.phy_node.cpu_res: RegionServer.LOG_APPEND_CPU_TIME}, [], [])
            log_write_req, log_write_done = self.hbase.get_hbase_write_req(self, self.wal_id, hbase_req.log_append_size)
            log_append_req.downstream_reqs.append(log_write_req)
            if log_sync_req is not None:
                log_append_req.downstream_reqs.append(log_sync_req)
                log_sync_req.blocking_evts.append(log_write_done)

        rpc_respond_req = StageReq(self.env, self.net_stage, hbase_req.client_id,
                                   {self.phy_node.net_res: hbase_req.reply_size}, [], [])

        rpc_handle_req = StageReq(self.env, self.cpu_stage, hbase_req.client_id,
                                  {self.phy_node.cpu_res: hbase_req.rpc_time}, [], [])
        if dn_read_req is not None:
            rpc_handle_req.downstream_reqs.append(dn_read_req)
            rpc_handle_req.blocking_evts.append(dn_read_done)
        if log_append_req is not None:
            rpc_handle_req.downstream_reqs.append(log_append_req)
        if log_sync_req is not None:
            rpc_handle_req.blocking_evts.append(log_sync_req.done)
        rpc_handle_req.downstream_reqs.append(rpc_respond_req)

        assert hbase_req.rpc_size > 0
        rpc_read_req = StageReq(self.env, self.net_stage, hbase_req.client_id,
                                {self.phy_node.net_res: hbase_req.rpc_size}, [], [])
        rpc_read_req.downstream_reqs.append(rpc_handle_req)

        return rpc_read_req.submit(), rpc_respond_req.done

    @staticmethod
    def construct_stages(env, name, phy_node, hbase, rs_conf, stage_log, async_flush=False):
        cpu_stage_scheduler = rs_conf.cpu_stage_scheduler_generator(env)
        cpu_stage_cost_func = RegionServer.get_cost_func("cpu", phy_node)
        cpu_stage = Stage(env, name + "_cpu", rs_conf.num_cpu_stage_workers, cpu_stage_scheduler,
                          cpu_stage_cost_func, log_file=stage_log)
        cpu_stage.monitor_interval = rs_conf.stage_monitor_interval

        io_stage_scheduler = rs_conf.io_stage_scheduler_generator(env)
        io_stage_cost_func = RegionServer.get_cost_func("io", phy_node)
        io_stage = Stage(env, name + "_io", rs_conf.num_io_stage_workers, io_stage_scheduler, io_stage_cost_func,
                         log_file=stage_log)
        io_stage.monitor_interval = rs_conf.stage_monitor_interval

        net_stage_scheduler = rs_conf.net_stage_scheduler_generator(env)
        net_stage_cost_func = RegionServer.get_cost_func("net", phy_node)
        net_stage = Stage(env, name + "_net", rs_conf.num_net_stage_workers, net_stage_scheduler,
                          net_stage_cost_func, log_file=stage_log)
        net_stage.monitor_interval = rs_conf.stage_monitor_interval

        # single ring buffer consumer, FIFO order
        log_append_stage = Stage(env, name + "_log_append", 1, FIFOScheduler(env, float('inf')), None,
                                 log_file=stage_log)
        log_append_stage.monitor_interval = rs_conf.stage_monitor_interval

        # log sync can be processed at any order, because it waits for the finishing of writes that are already issued
        # but here we use FIFO, it doesn't really matter because it does not use any resources
        log_sync_stage = Stage(env, name + "_log_sync", rs_conf.num_log_syncers, FIFOScheduler(env, float('inf')), None,
                               log_file=stage_log)
        log_sync_stage.monitor_interval = rs_conf.stage_monitor_interval

        packet_ack_stage = OnDemandStage(env, name + "_packet_ack", log_file=stage_log)
        packet_ack_stage.monitor_interval = rs_conf.stage_monitor_interval

        ack_process_stage = OnDemandStage(env, name + "_ack_process", log_file=stage_log)
        ack_process_stage.monitor_interval = rs_conf.stage_monitor_interval

        return cpu_stage, io_stage, net_stage, log_append_stage, log_sync_stage, packet_ack_stage, ack_process_stage

    @staticmethod
    def get_region_server(env, name, phy_node, hbase, rs_conf, stage_log=sys.stdout):
        """

        :param env: 
        :param name: 
        :param phy_node: 
        :param hbase: 
        :param rs_conf: 
        :param stage_log: 
        :return: 
        """

        assert isinstance(hbase, OneStagePerResHBase)

        cpu_stage, io_stage, net_stage, log_append_stage, log_sync_stage, packet_ack_stage, ack_process_stage = \
            OneStagePerResRegionServer.construct_stages(env, name, phy_node, hbase, rs_conf, stage_log)
        region_server = OneStagePerResRegionServer(env, phy_node, hbase, cpu_stage, net_stage, io_stage,
                                                   log_sync_stage, log_append_stage, packet_ack_stage,
                                                   ack_process_stage)

        return region_server


class AsyncOneStagePerResRegionServer(OneStagePerResRegionServer):
    """
    Same as OneStagePerResRegionServer, but asynchronously perform data reads.
    Only works with OneStagePreResHBase

    FIXME-yangsuli: for now AsyncOneStagePerResRegionServer does not do namenode_lookup or memstore update
    habase_req.namenode_lookup_time and hbase_req.mem_update_size will be ignored; the corresponding resources are
    not consumed; as a result, no memory flush will be initiated neither (thus no mem_flush stage)
    """

    def __init__(self, env, phy_node, hbase, cpu_stage, net_stage, io_stage, log_sync_stage, log_append_stage,
                 packet_ack_stage, ack_process_stage):
        OneStagePerResRegionServer.__init__(self, env, phy_node, hbase, cpu_stage, net_stage, io_stage,
                                            log_sync_stage, log_append_stage, packet_ack_stage, ack_process_stage)

    def submit_req(self, hbase_req):
        """
        :param hbase_req:
        :return:
        """

        dn_read_req = None
        dn_read_done = None
        if hbase_req.datanode_read_size > 0:
            if hbase_req.short_circuit:
                dn_read_req = StageReq(self.env, self.io_stage, hbase_req.client_id, {self.phy_node.io_res:
                                                                                          hbase_req.datanode_read_size},
                                       [], [])
                dn_read_done = dn_read_req.done
            else:
                dn_read_req, dn_read_done = self.hbase.get_hbase_read_req(self, hbase_req.client_id,
                                                                         hbase_req.datanode_read_size)

        log_sync_req = None
        if hbase_req.log_sync and hbase_req.log_append_size > 0:
            log_sync_req = StageReq(self.env, self.log_sync_stage, self.wal_id, {}, [], [])

        log_append_req = None
        if hbase_req.log_append_size > 0:
            log_append_req = StageReq(self.env, self.log_append_stage, hbase_req.client_id,
                                      {self.phy_node.cpu_res: RegionServer.LOG_APPEND_CPU_TIME}, [], [])
            log_write_req, log_write_done = self.hbase.get_hbase_write_req(self, self.wal_id, hbase_req.log_append_size)
            log_append_req.downstream_reqs.append(log_write_req)
            if log_sync_req is not None:
                log_append_req.downstream_reqs.append(log_sync_req)
                log_sync_req.blocking_evts.append(log_write_done)

        rpc_respond_req = StageReq(self.env, self.net_stage, hbase_req.client_id,
                                   {self.phy_node.net_res: hbase_req.reply_size}, [], [])

        rpc_handle_req = StageReq(self.env, self.cpu_stage, hbase_req.client_id,
                                  {self.phy_node.cpu_res: hbase_req.rpc_time}, [], [])
        if dn_read_req is not None:
            rpc_handle_req.downstream_reqs.append(dn_read_req)
        if log_append_req is not None:
            rpc_handle_req.downstream_reqs.append(log_append_req)
        if log_sync_req is not None:
            rpc_handle_req.blocking_evts.append(log_sync_req.done)
        rpc_handle_req.downstream_reqs.append(rpc_respond_req)

        assert hbase_req.rpc_size > 0
        rpc_read_req = StageReq(self.env, self.net_stage, hbase_req.client_id,
                                {self.phy_node.net_res: hbase_req.rpc_size}, [], [])
        rpc_read_req.downstream_reqs.append(rpc_handle_req)

        # FIXME-yangsuli: this is inaccurate, as we should not initiate rpc respond until dn_read is done
        if dn_read_req is not None:
            done_evt = simpy.AllOf(self.env, [rpc_respond_req.done, dn_read_req.done])
        else:
            done_evt = rpc_respond_req.done
        return rpc_read_req.submit(), done_evt

    @staticmethod
    def get_region_server(env, name, phy_node, hbase, rs_conf, stage_log=sys.stdout):
        """

        :param env: 
        :param name: 
        :param phy_node: 
        :param hbase: 
        :param rs_conf: 
        :param stage_log: 
        :return: 
        """

        assert isinstance(hbase, OneStagePerResHBase)

        cpu_stage, io_stage, net_stage, log_append_stage, log_sync_stage, packet_ack_stage, ack_process_stage = OneStagePerResRegionServer.construct_stages(
            env, name, phy_node, hbase, rs_conf, stage_log)
        region_server = AsyncOneStagePerResRegionServer(env, phy_node, hbase, cpu_stage, net_stage, io_stage,
                                                        log_sync_stage, log_append_stage, packet_ack_stage,
                                                        ack_process_stage)

        return region_server


class HBase(object):
    """
    HBase cluster
    """

    def __init__(self, env, hdfs):
        self.env = env
        self.hdfs = hdfs
        self.rs_list = []

    def add_region_server(self, rs):
        self.rs_list.append(rs)

    @staticmethod
    def get_cluster(env, phy_cluster, hdfs, hbase_conf, resource_log=sys.stdout, stage_log=sys.stdout):
        """

        :param env:
        :param phy_cluster: PhysicalCluster object
        :param hdfs: HDFS object
        :param hbase_conf: HBaseConf object
        :param resource_log:
        :param stage_log:
        :return: HBase object
        """
        hbase_cluster = HBase(env, hdfs)

        assert len(phy_cluster.node_list) >= 2
        for phy_node in phy_cluster.node_list[1:]:
            if hbase_conf.rs_type == "original":
                region_server = RegionServer.get_region_server(env, "rs_" + str(len(hbase_cluster.rs_list)), phy_node,
                                                               hbase_cluster, hbase_conf.rs_conf, stage_log)
            elif hbase_conf.rs_type == "speculative":
                region_server = SpeculativeRegionServer.get_region_server(env, "rs_" + str(len(hbase_cluster.rs_list)),
                                                                          phy_node,
                                                                          hbase_cluster, hbase_conf.rs_conf, stage_log)
            elif hbase_conf.rs_type == "short_circuit":
                region_server = SCRegionServer.get_region_server(env, "rs_" + str(len(hbase_cluster.rs_list)), phy_node,
                                                                 hbase_cluster, hbase_conf.rs_conf, stage_log)
            elif hbase_conf.rs_type == "async_handler":
                region_server = AsyncHandlerRegionServer.get_region_server(env, "rs_" + str(len(hbase_cluster.rs_list)),
                                                                           phy_node,
                                                                           hbase_cluster, hbase_conf.rs_conf, stage_log)
            elif hbase_conf.rs_type == "async_flusher":
                region_server = AsyncFlusherRegionServer.get_region_server(env, "rs_" + str(len(hbase_cluster.rs_list)),
                                                                           phy_node,
                                                                           hbase_cluster, hbase_conf.rs_conf, stage_log)
            elif hbase_conf.rs_type == "active_handler":
                region_server = ActiveHandlerRegionServer.get_region_server(env,
                                                                            "rs_" + str(len(hbase_cluster.rs_list)),
                                                                            phy_node,
                                                                            hbase_cluster, hbase_conf.rs_conf,
                                                                            stage_log)
            elif hbase_conf.rs_type == "sched_handler_time":
                region_server = SchedHandlerTimeRegionServer.get_region_server(env,
                                                                               "rs_" + str(len(hbase_cluster.rs_list)),
                                                                               phy_node,
                                                                               hbase_cluster, hbase_conf.rs_conf,
                                                                               stage_log)
            elif hbase_conf.rs_type == "sched_handler_sep_wal":
                region_server = SchedHandlerSepWALRegionServer.get_region_server(env, "rs_" + str(
                    len(hbase_cluster.rs_list)), phy_node,
                                                                                 hbase_cluster, hbase_conf.rs_conf,
                                                                                 stage_log)
            else:
                raise ConfigError("Unsupported region server type: " + hbase_conf.rs_type)

            region_server.wal_id = 9999 + len(hbase_cluster.rs_list)

            hbase_cluster.add_region_server(region_server)

        return hbase_cluster


class OneStagePerResHBase(HBase):
    """
    HBase cluster that works with OneStagePerResRegionServer or AsyncOneStagePerResRegionServer

    Handles read/write within the regionservers (in the i/o stage), no need for an hdfs

    """

    def __init__(self, env, replica):
        self.env = env
        self.replica = replica # number of replicas a write has
        self.rs_list = []

    def get_hbase_write_req(self, local_node, client_id, write_size):
        """

        :param local_node:
        :param client_id:
        :param write_size:
        :return: (write_req, completion event)
        """

        assert self.replica <= len(self.rs_list)
        assert local_node in self.rs_list

        dst_nodes = [local_node]

        while len(dst_nodes) < self.replica:
            node = self.rs_list[random.randrange(len(self.rs_list))]
            if node not in dst_nodes:
                dst_nodes.append(node)

        io_req, ack_done = dst_nodes[0].get_write_req(client_id, write_size,
                                                      [] if len(dst_nodes) == 1 else dst_nodes[1:])

        return io_req, ack_done

    def get_base_read_req(self, local_node, client_id, read_size):
        # type: (OneStagePerResRegionServer, int, int) -> (StageReq, Event)
        """

        :param local_node:
        :param client_id:
        :param read_size:
        :return: (read_req, completion event)
        """

        dst_node = self.rs_list[random.randrange(len(self.rs_list))]

        io_req = StageReq(self.env, dst_node.io_stage, client_id, {dst_node.phy_node.io_res: read_size}, [], [])
        remote_net_req = StageReq(self.env, dst_node.net_stage, client_id, {dst_node.phy_node.net_res: read_size}, [],
                                  [])
        local_net_req = StageReq(self.env, local_node.net_stage, client_id, {local_node.phy_node.net_res:
                                                                                 read_size}, [], [])
        io_req.downstream_reqs.append(remote_net_req)
        remote_net_req.downstream_reqs.append(local_net_req)

        return io_req, local_net_req.done

    @staticmethod
    def get_cluster(env, phy_cluster, hbase_conf, resource_log=sys.stdout, stage_log=sys.stdout):
        hbase_cluster = OneStagePerResHBase(env, hbase_conf.replica)

        assert len(phy_cluster.node_list) >= 2
        for phy_node in phy_cluster.node_list[1:]:
            if hbase_conf.rs_type == "resource_stage":
                region_server = OneStagePerResRegionServer.get_region_server(env, "rs_" + str(len(
                    hbase_cluster.rs_list)), phy_node, hbase_cluster, hbase_conf.rs_conf, stage_log)
            elif hbase_conf.rs_type == "async_resource_stage":
                region_server = AsyncOneStagePerResRegionServer.get_region_server(env, "rs_" + str(len(
                    hbase_cluster.rs_list)), phy_node, hbase_cluster, hbase_conf.rs_conf, stage_log)
            else:
                raise ConfigError("Unsupported region server type: " + hbase_conf.rs_type)

            region_server.wal_id = 9999 + len(hbase_cluster.rs_list)

            hbase_cluster.add_region_server(region_server)

        return hbase_cluster


class HBaseClient(Client):
    """
    HBaseClient continuously issues hbase requests to HBase/OneStagePerResHBase cluster, and measures bandwidth/latency
    etc.
    """

    # noinspection PyMissingConstructor
    def __init__(self, env, hbase, name, client_id, num_instances, rpc_size, reply_size, rpc_time, mem_update_size,
                 log_append_size, namenode_lookup_time,
                 datanode_read_size, short_circuit_ratio, think_time, random_req=False, log_file=sys.stdout):
        self.env = env
        self.hbase = hbase
        self.client_name = name
        self.client_id = client_id
        self.rpc_size = rpc_size
        self.reply_size = reply_size
        self.rpc_time = rpc_time
        self.mem_update_size = mem_update_size
        self.log_append_size = log_append_size
        self.namenode_lookup_time = namenode_lookup_time
        self.datanode_read_size = datanode_read_size
        self.short_circuit_ratio = short_circuit_ratio
        self.think_time = think_time
        self.random_req = random_req
        self.log_file = log_file
        self.instances = []
        for i in range(num_instances):
            self.instances.append(env.process(self.run()))
        self.monitor_interval = 10
        self.monitor = env.process(self.monitor_run())
        self.num_reqs = 0
        self.total_latency = 0
        self.max_latency = 0
        self.last_report = self.env.now

    def run(self):
        while True:
            # randomly choose a region_server to send request
            # can change this policy later
            region_server = self.hbase.rs_list[random.randrange(len(self.hbase.rs_list))]
            short_circuit = np.random.choice([True, False], p=[self.short_circuit_ratio, 1 - self.short_circuit_ratio])
            rpc_size = self.rpc_size
            reply_size = self.reply_size
            rpc_time = self.rpc_time
            mem_update_size = self.mem_update_size
            log_append_size = self.log_append_size
            namenode_lookup_time = self.namenode_lookup_time
            datanode_read_size = self.datanode_read_size
            if self.random_req:
                rpc_size = random.uniform(0, 2 * rpc_size)
                reply_size = random.uniform(0, 2 * reply_size)
                rpc_time = random.uniform(0, 2 * rpc_time)
                namenode_lookup_time = random.uniform(0, 2 * namenode_lookup_time)
                datanode_read_size = random.uniform(0, 2 * datanode_read_size)
            hbase_req = HBaseReq(self.client_id, rpc_size, reply_size, rpc_time, mem_update_size, log_append_size,
                                 namenode_lookup_time, datanode_read_size, log_sync=True, short_circuit=short_circuit)

            start = self.env.now
            submit_evt, done_evt = region_server.submit_req(hbase_req)
            yield submit_evt
            yield done_evt

            latency = self.env.now - start
            self.num_reqs += 1
            self.total_latency += latency
            if latency > self.max_latency:
                self.max_latency = latency

            if self.think_time != 0:
                yield self.env.timeout(self.think_time)

    @staticmethod
    def get_client(env, hbase, config, section, client_log_file=sys.stdout):
        assert section in config.sections()
        try:
            client_id = config.getint(section, "client_id")
            client_name = config.get(section, "client_name")
            num_instances = config.getint(section, "num_instances")
            rpc_size = config.getfloat(section, "rpc_size")
            reply_size = config.getfloat(section, "reply_size")
            rpc_time = config.getfloat(section, "rpc_time")
            mem_update_size = config.getfloat(section, "mem_update_size")
            log_append_size = config.getfloat(section, "log_append_size")
            namenode_lookup_time = config.getfloat(section, "namenode_lookup_time")
            datanode_read_size = config.getfloat(section, "datanode_read_size")
            short_circuit_ratio = config.getfloat(section, "short_circuit_ratio")
            think_time = config.getfloat(section, "think_time")
            random_req = False
            if "type" in config.options(section):
                if config.get(section, "type") == "random":
                    random_req = True
            monitor_interval = 10
            if "monitor_interval" in config.options(section):
                monitor_interval = config.getfloat(section, "monitor_interval")
        except ConfigParser.NoOptionError as error:
            raise ConfigError(
                "client " + error.section + " configuration not complete, missing option: " + error.option)

        client = HBaseClient(env, hbase, client_name, client_id, num_instances, rpc_size, reply_size, rpc_time,
                             mem_update_size, log_append_size,
                             namenode_lookup_time, datanode_read_size, short_circuit_ratio, think_time, random_req,
                             client_log_file)
        client.monitor_interval = monitor_interval

        return client

    @staticmethod
    def get_clients(env, hbase, config, client_log_file=sys.stdout):
        """
        :param env:
        :param hdfs: the hbase cluster that the clients issue requests to
        :param config: ConfigParser object, have sections named "hbase_client*"
        :param client_log_file: file handler which clients write statistics to
        :return: a list of clients specified in the config_file
        """
        client_list = []

        for section in config.sections():
            if "hbase_client" not in section:
                continue
            client = HBaseClient.get_client(env, hbase, config, section, client_log_file)
            client_list.append(client)

        if len(client_list) == 0:
            print "Warning: no hbase_client configuration found in", config, "returning an empty list"

        return client_list
