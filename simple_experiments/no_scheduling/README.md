# No Scheduling Experiment

This experiment shows that when one stage suffers the no scheduling problem,  client performance cannot be properly 
isolated. 

### Experiment Setting
We compare two settings. 

In the first setting (on-demand), we simulate a two-stage server. 
The first stage consumes resource r1 and  is managed by a DRF scheduler; 
the second stage consumes resource r2 and is on demand (i.e., suffers no scheduling).

In the second setting (sched), we also simulate a two-stage server.
However, both stages are managed by the WFQ scheduler.

Two clients, C1 and C2, both issue requests that consume 0.01 unit of r1 and 0.1 unit of r2.

When client C2 issues requests more intensively (with more threads), we can see in the on-demand setting the resources 
are not fairly allocated and client C1 continues to suffer;
in contrast, in the sched setting, resources are fairly allocated and C1 maintains stable throughput despite C2.

### How to run
bash run_exp.sh

A log directory will be generated that contain simulation logs.

ondemand_sim.eps and sched_sim.eps will also be generated, in which C1 throughput and the percentage of resource r2
(the reosurce in contention) allocated to C2 are plotted.
