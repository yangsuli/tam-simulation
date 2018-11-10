import sys
import matplotlib.pyplot as plt
import re
import ast
import numpy as np

from util.plot_helper import plot_client_throughput
from util.plot_helper import get_average_throughput
from util.plot_helper import plot_resource_utilization
from util.plot_helper import plot_time_value_list

arg_color_dic = {'original':'red', 'original_sched':'blue', 'resource_stage':'green'}

def plot_throughput_comparison(arg_list, reply_size_list, subplt, title):
        result_dict = {}
	for arg in arg_list:
		client_1_throughput_list = []
		reply_size_list_in_kb = []
		for reply_size in reply_size_list:
			log_dir = "log_" + arg + "_" + str(reply_size)
			client_log = log_dir + "/" + "client.log"
			throughput_1 = get_average_throughput(client_log, "client_1")
                        print "arg", arg, "reply_size", reply_size, "throughput", throughput_1
			reply_size_list_in_kb.append(reply_size * 1000)
			client_1_throughput_list.append(throughput_1)
		print arg, client_1_throughput_list
                result_dict[arg] = client_1_throughput_list
		subplt.scatter(reply_size_list_in_kb, client_1_throughput_list, color=arg_color_dic[arg])
		subplt.plot(reply_size_list_in_kb, client_1_throughput_list, color=arg_color_dic[arg], label=arg)

	subplt.set_xlim([0,205])
	subplt.set_xlabel("Reply Size of Client-2 (KB)")
	subplt.set_ylabel("Throughput of Client-1 (ops/s)")
	subplt.legend(loc='best')
	subplt.set_title(title)
        return result_dict

def plot_per_client_utilization(subplt, resource_log, resource_name):
	client_util_list = {}
	for line in open(resource_log):
		items = line.split()
		res_name = items[1]
		if res_name != resource_name:
			continue
		time = float(items[3])
		idx = items.index("per_client_utilization")
		util_dic_str = ' '.join(items[idx+1:])
		client_util_dic = ast.literal_eval(util_dic_str)
		for client_id in client_util_dic:
			if client_id not in client_util_list:
				client_util_list[client_id] = []
			client_util_list[client_id].append((time, client_util_dic[client_id]))
	for client_id in client_util_list:
		plot_time_value_list(subplt, client_util_list[client_id], str(client_id))
	subplt.legend(loc='best')
	subplt.set_title(resource_name + " utilization")


reply_size_list = np.linspace(0.01, 0.2, num=20)
arg_list = ["original", "original_sched", "resource_stage"]

#fig, axs = plt.subplots(3,1)
#axs = axs.ravel()

fig = plt.figure()
ax = fig.add_subplot(111)

result_dict = plot_throughput_comparison(arg_list, reply_size_list, ax, "throughput")

plt.savefig("compare.eps")
