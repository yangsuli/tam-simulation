TAM Simulation Framework.
====
The TAM simulation framework provides building blocks to simulate systems based on the Thread Architecture Model (TAM).
For detailed discussion on TAM and how to use TAM to understand system scheduling,  please refer to our OSDI'18 paper: [Principled Schedulability Analysis for Distributed Storage Systems using Thread Architecture Models](https://www.usenix.org/conference/osdi18/presentation/yang).

# Key Abstractions

The TAM simulation framework is based on four key abstractions: resource, stage-request, stage, and scheduler. 

### Resource

Resource can be used to model any kind of resources, e.g., cpu, I/O, network.
One can specify the rate and parallelism of a resource. 
For example, an 8-core cpu of 1 GHz has a rate of 1 (per core) and a parallelism of 8.

### Stage-Request

In a complex system, a client request typically goes through different components while being serviced,
thus is divided into multiple *stage-requests*.   
Each stage-request represents the processing that is done at one component, and consumes certain resources (described in the *resource profile* of the stage-request).  

### Stage

Stage is a component that process stage-requests. Each stage has certain number of workers(threads) that
continuously pick up stage-requests and process them.
During the processing, the stage consume resources based on the resource profile of the stage-request.

### Scheduler

A scheduler encodes the logic of how stages pick up which requests to process.

Each (bounded) stage has a scheduler and a request_cost_function that maps a stage-request to a virtual cost. The scheduler takes the virtual cost and use a policy (e.g., fairness, or priority) to decide which request to process next. 

We provide some baisc schedulers, such as FIFO, weighted fair queuing, and dominate resource fairness scheduling (see scheduler.py); 
the user is free to implement their own scheduling policies as well.  

### Putting them together  
By assembling different stages, resources, and schedulers, possibly using a TAM as a blueprint, one can form systems with various thread architectures, which may reflect existing or hypothetical  system designs. 
With a given simulated system, one can specify workload characteristics (e.g., request types and arrival distribution); the framework then simulates how requests flow through the stages and consume resources, and reports detailed performance statistics. 

# Getting Started

To run the simulation, you need simpy, an event-based simulation framework (https://simpy.readthedocs.io/en/latest/).
We strongly encourage you to read the documentation of simpy before using the TAM framework, as our framework is built on simpy. 

To show how the whole process works,  basic_example.py runs a very simple simulation under the TAM framework, with only
a single stage.  
Just run `$ python basic_example.py` and see what happens.  

When successfully run,  basic_example.py prints the client, resource and stage statistics to the standard output.
However, when running a more complicated simulation, it is recommended to print client, resource and stage statistics
each to a separate log file, as it allows easier analysis.

You can change basic_example.py and play around with it to see how things work.

Once you are more familiar with the framework, take a look at simple_experiments.
In there are slightly more complicated simulation experiments, typically with two or three stages.  
The README of each sub-directory details what each experiment does.


# Simulated Systems

The TAM simulation framework can be used to simulate realistic systems, such as HBase or Cassandra;
it can also be used to explore alternative designs for these systems.

We simulated HBase, Cassandra, MongoDB, and Riak, with both their original architecture and some other hypothetical 
architectures that can potentially support better scheduling.

The simulation code can be found in the corresponding sub-directories; check the README for details on how to configure
and run each system.

We encourage other system simulations to be written.



# Contact 

If you encounter any problems in using the framework, please contact suli@cs.wisc.edu.

Enjoy!
