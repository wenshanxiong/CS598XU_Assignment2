import json
import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from argparse import ArgumentParser

pd.set_option('display.max_row', None)
starting_count = 1
ending_count = 50
fault_type = 'follower_slow_cpu_5'
all_faults = [name for name in os.listdir('./log_history/')]

if __name__ == '__main__':
    parser = ArgumentParser()

    parser.add_argument('-p', '--start-port', help='Start port', type=int, default=8000)
    parser.add_argument('-n', '--processes', help='Cluster size', type=int, default=4)
    parser.add_argument('-d', '--log-dir', default=os.path.abspath('log_history/{}/'.format(fault_type)), dest='log_dir', help="Log dir")

    args = parser.parse_args()

    if not os.path.isdir(args.log_dir):
        print("Logs don't exist!")
        exit()

    data = [[] for _ in range(args.processes)]
    for i in range(args.processes):
        name = "127.0.0.1_{}.log".format(args.start_port + i)
        with open("{}/{}".format(args.log_dir, name), 'r') as fp:
            for line in fp:
                data[i].append(json.loads(line))
    
    latency = {}
    for i in range(args.processes):
        node_addr = "127.0.0.1:{}".format(args.start_port + i)
        latency[node_addr] = []
        for entry in data[i]:
            latency[node_addr].append((entry['delivered_at'] - entry['command']['data']['created_at']) * 1000)
    
    df_latency = pd.DataFrame(dict([(k,pd.Series(v)) for k,v in latency.items()]))
    med_latency = df_latency.median(axis=1)
    # med_latency = med_latency.groupby(np.arange(len(med_latency))//ending_count).mean()
    print(med_latency)
    plt.xlim(0,4000)
    # fault_point = min([len(d) for d in data])
    # plt.scatter(range(len(med_latency[:fault_point])), med_latency[:fault_point], sizes=[1])
    # plt.scatter(range(fault_point, fault_point + len(med_latency[fault_point:])), med_latency[fault_point:], c='red', sizes=[1])
    plt.scatter(range(len(med_latency)), med_latency, sizes=[1])
    plt.xlabel('write operation')
    plt.ylabel('Median Latency (ms)')
    # plt.legend(['Before fault', 'after fault'])
    plt.savefig('{}_scatter.png'.format(fault_type))
    plt.clf()

    bins = np.arange(0,6,0.5)
    plt.hist(np.clip(med_latency, bins[0], bins[-1]), bins=bins)
    xlabels = np.arange(0,6,1).astype(str)
    xlabels[-1] += '+'
    xlabels = np.insert(xlabels, 0, '0')
    plt.gca().set_xticklabels(xlabels)
    plt.xlabel('latency (ms)')
    plt.ylabel('count')
    plt.savefig('{}_hist.png'.format(fault_type))
    print(df_latency[3000:])
    print(all_faults)

    throughput = len(data[0]) / (data[0][-1]['command']['data']['created_at'] - data[0][0]['command']['data']['created_at'])
    print("throughput:{}".format(throughput))

    # throughput = {}
    # for i in range(args.processes):
    #     node_addr = "127.0.0.1:{}".format(args.start_port + i)
    #     throughput[node_addr] = []
    #     start_time = None
    #     end_time = None
    #     for entry in data[i]:
    #         if entry['command']['data']['count'] == starting_count:
    #             start_time = entry['command']['data']['created_at']
    #         if entry['command']['data']['count'] == ending_count:
    #             end_time = entry['command']['data']['created_at']
    #             throughput[node_addr].append((ending_count - starting_count) / (end_time - start_time))
    
    # df_throughput = pd.DataFrame(dict([(k,pd.Series(v)) for k,v in throughput.items()]))
    # mean_throughput = df_throughput.mean(axis=1)
    # print(mean_throughput)


    # print(med_latency.shape)
    # print(mean_throughput.shape)
    # plt.scatter(mean_throughput[:-1], med_latency[:-2], c=np.array(range(mean_throughput[:-1].size)), cmap='cool')
    # plt.xlabel('Throughput (ops/sec)')
    # plt.ylabel('Median Latency (ms)')
    # plt.colorbar(label="Timestep")
    # plt.savefig("latency_throughput.png")
