import simpy

from client import Client
from scheduler import WFQScheduler
from seda_resource import Resource
from stage import Stage
from stage_req import StageReq

""" a very simple simulation to show how things work"""


def run_sim():
    """ Step 1: initialize simulation environment of simpy"""
    env = simpy.Environment()
    """ Step 2: create resource of the system"""
    resource = Resource(env, "resource", 1, 10, WFQScheduler(env, float('inf')))
    """ Setp 3: define stages that takes requests"""
    handle_req_cost_func = lambda req: req.resource_profile[resource]
    stage = Stage(env, "stage", 10, WFQScheduler(env, float('inf')), handle_req_cost_func)

    """ Setp 4: define clients that issue requests"""
    # Client(environment, client_name, client_id, num_instances, new_req_func, think_time, log_file)
    new_req_func = lambda: StageReq(env, stage, 1, {resource: 1}, [], [])
    client = Client(env, "client", 1, 10, new_req_func, 0)

    """ Finally , run the simulation"""
    env.run(until=100)


if __name__ == '__main__':
    run_sim()
