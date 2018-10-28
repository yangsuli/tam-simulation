import sys
from util.plot_helper import get_average_throughput

import matplotlib.pyplot as plt

c1_threads = 10
c2_threads_list = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
c1_throughput_list = []
c2_throughput_list = []
c2_io_bandwidth_list = []

arg = sys.argv[1]

tmp_index = 0
for c2_threads in c2_threads_list:
    client_log = "client_" + arg + "_" + str(c1_threads) + "_" + str(c2_threads) + ".log"
    c1_throughput = get_average_throughput(client_log, "c1")
    c2_throughput = get_average_throughput(client_log, "c2")
    c1_throughput_list.append(c1_throughput)
    c2_throughput_list.append(c2_throughput)
    c2_io_bandwidth_list.append(c2_throughput * 0.1)
    tmp_index += 1

print "c1_throughput_list:", c1_throughput_list
print "c2_throughput list:", c2_throughput_list

fig = plt.figure()
ax = fig.add_subplot(111)
ax.plot(c2_threads_list, c1_throughput_list, label="client-1")
ax.set_xlim([8, 102])
ax.set_xlabel("Thread Count of Client-2", fontsize=20)
ax.set_ylabel("Throughput of Client-1 (reqs/s)", color='blue', fontsize=20)
ax.tick_params('y', colors='blue')

ax2 = ax.twinx()
ax2.plot(c2_threads_list, c2_io_bandwidth_list, color='green')
ax2.set_ylabel("Percent of Res2 Allocated to Client-2", color='green', fontsize=18)
ax2.tick_params('y', colors='green')
ax.set_xlim([8, 102])
ax.set_ylim([0, 510])

plt.savefig(arg + "_sim.eps")
