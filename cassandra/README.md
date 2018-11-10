TAM-simulation(Cassandra)
===

This directory contains the simulation of Cassandra under TAM simulation framework.

#### Getting Started
* simply run `$ python run_sim.py` in test subdirectory, which shows how by default cassandra works
* Please refer to ./test/test_sys.ini and ./test/test_client.ini to see how to change the configuration
of clients(aka. workload) and system(server-side configuration).
* Again, please refer to ./test/test_sys.ini to see how to change the scheduling support (like scheduler type, stage-type)
* The simulation output cound be found in log files
