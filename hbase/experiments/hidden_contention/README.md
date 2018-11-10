# The Hidden-Contention Problem in HBase

The experiment demonstrates that the original HBase suffers from the hidden contention problem, and how we can change HBase to fix this problem.

For definition of the hidden contention problem; see section 3.3 of 
[Principled Schedulability Analysis for Distributed Storage Systems using Thread Architecture Models](http://research.cs.wisc.edu/wind/Publications/osdi18-tam.pdf);
for the hidden contention problem in HBase, see section 4.1.3 of 
[Principled Schedulability Analysis for Distributed Storage Systems using Thread Architecture Models](http://research.cs.wisc.edu/wind/Publications/osdi18-tam.pdf).


### Experiment Setting
We simulate a workload where C1 and C2 keeps issuing 1 KB rpc requests.
C1's response size remains 20 KB,
while C2's response size varies from 10 to 200 KB.
The scheduling goal is to fairly allocate network resources between C1 and C2, and isolate C1 from C2's workload (reply size) change.

We compare three settings.

In the first setting (original), HBase has the original architecture and behavior (default FIFO scheduling at both the
rpc_read and rpc_respond stage). 

In the second setting (original_sched), HBase architecture remains the same, but we use WFQ (weighted fair queuing) at
the rpc_read and rpc_respond stage, attempting to allocate the network resources fairly among C1 and C2.
Note that in original_sched, we set the number of rpc_respond threads to 10 for the hidden contention problem to manifest.

In the last setting (resource_stage), we solve the hidden contention problem in HBase by re-architecting HBase, so that a single resource
is managed in a dedicated stage (the cpu_, io_ and net_stage; for details, see OneStagePerResHBase and OneStagePerResRegionServer in hbase.py).
Each stage has a WFQ scheduler that attempts to allocates resources fairly.

Only in the last setting network resources are fairly allocated, and C1 throughput does not decrease when C2 uses more
network with larger reply sizes.

### How to run
`$ bash run_exp.sh`
will run the simulation and generate log files.

`$ python plot.py`
will parse the log file and plot compare.eps, which shows how C1 throughput changes with C2 reply size.

