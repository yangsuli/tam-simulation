HBase Simulation
===

This directory contains the simulation of HBase under the TAM simulation framework;
the simulation is based on the thread architecture model of HBase.

Detailed description on how HBase works and its thread architecture can be found in our OSDI'18 paper: 
[Principled Schedulability Analysis for Distributed Storage Systems using Thread Architecture Models](http://research.cs.wisc.edu/wind/Publications/osdi18-tam.pdf);
in particular, check section 2.2.


#### Getting Started
HBase simulation can be configured using [ConfigParser](https://docs.python.org/3/library/configparser.html) style configuration files.
Typically, there is a client side configuration file that specifies the workload characteristics (request size, request type, etc.),
and a server side configuration file that specifies server setting (cpu frequency, I/O bandwidth, scheduling policy, etc.)

The test sub-directory contains a sample client side configuration file (client.ini),
a sample server side configuration file (hbase.ini), 
and a simple python script (run_sim.py) that reads the configuration files and runs the simulation.
Simply run `$ python run_sim.py` under the test sub-directory to run the hbase simulation; 
three files will be generated: client.log, resource.log, and stage.log, which contains the statistics of the client 
(including throughput, latency, etc.), the resources (including the resource utilization info etc.), and the stages
(including the stage servicing threads/blocking threads, stage queue occupancy, etc.).

Once you can successfully run the simulation in test;
you can change the configuration file, add more client instances, change the region server types, or add more resources 
in each node, and see how it changes the simulation results.
You can even implement your own regionserver/hbase by changing the default regionserver/hbase behavior 
(possibly by inheriting the RegionServer and HBase class in hbase.py).


#### Experiments
We have used the hbase simulation to run various experiments, in order to answer the following questions:
* What schedulng problems does hbase have?
* How can we change hbase to fix the problems?

The experiments sub-directory contains these experiments;
check the README in each sub-directory under experiments for what each experiment does.
 