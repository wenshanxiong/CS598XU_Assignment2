#!/usr/bin/bin python3
import os
import logging
import asyncio
import random
import raftos
import raftos.serializers
from argparse import ArgumentParser
from time import monotonic
import subprocess
from multiprocessing import Process


logging.basicConfig(level=logging.INFO)
log = logging.getLogger()


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))


class Class:
    data = raftos.ReplicatedDict(name='data')


def main(log_dir, node, cluster):
    loop = asyncio.new_event_loop()

    raftos.configure({
        'log_path': log_dir,
        'serializer': raftos.serializers.JSONSerializer,
        'loop': loop
    })

    loop.run_until_complete(run(loop, node, cluster))


async def run(loop, node, cluster):
    await raftos.register(node, cluster=cluster, loop=loop)

    obj = Class()
    count = 0
    wait_time = 0.005
    id = 0
    exp = 0.9
    step = 0
    fault_config = {
        'node': 'follower',
        'faulty_node': None,
        'type': 'cpu_slow', # cpu_crash, cpu_slow, mem_contention, none
        'fault_time': 4000
    }
    faulty_follower = None
    last_leader = None

    while True:
        leader = raftos.get_leader()
        if leader != None:
            if fault_config['faulty_node'] == None:
                if fault_config['node'] == 'follower':
                    address = leader.split(':')
                    faulty_follower_port  = (int(address[-1]) + 1) % (len(cluster) + 1) + 8000
                    fault_config['faulty_node'] = ''.join([address[0], ':', str(faulty_follower_port)])
                else:
                    fault_config['faulty_node'] = leader
                
            if leader == node:
                # if count == 1:
                #     print("leader pid:{}, step:{}, wait time:{}".format(os.getpid(), step, wait_time))
                #     step += 1
                # if count == 50:
                #     if wait_time <= 0:
                #         print("end")
                #         await asyncio.sleep(1000, loop=loop)
                #     wait_time *= exp
                #     count = 0
                # count += 1

                if leader != last_leader:
                    print("leader is {}, faulty node is {}". format(os.getpid(), fault_config['faulty_node']))
                    last_leader = leader
                if count % 200 == 0:
                    print("leader wrote {} ops".format(count))
                if count == 4000:
                    print("end")
                    await asyncio.sleep(1000, loop=loop)

                log = {
                    'id': id,
                    'created_at': monotonic(),
                    'wait_time': wait_time,
                    'count': count
                }

                await obj.data.update(log)
                id += 1

            if count == fault_config['fault_time'] and node == fault_config['faulty_node']:
                if fault_config['type'] == 'cpu_crash':
                    print("{} dropped".format(node))
                    exit()
                elif fault_config['type'] == 'cpu_slow':
                    quota = '5000'
                    period= '100000'
                    subprocess.call(['sudo', 'mkdir', '/sys/fs/cgroup/cpu/raftos'])
                    with open('/sys/fs/cgroup/cpu/raftos/cpu.cfs_quota_us', 'w') as f:
                        f.write(quota)
                    with open('/sys/fs/cgroup/cpu/raftos/cpu.cfs_period_us', 'w') as f:
                        f.write(period)
                    with open('/sys/fs/cgroup/cpu/raftos/cgroup.procs', 'w') as f:
                        f.write(str(os.getpid()))
                    print('{} cpu slowed'.format(node))

                elif fault_config['type'] == 'mem_contention':
                    mem_limit = 5 * 1024 *1024
                    subprocess.call(['sudo', 'mkdir', '/sys/fs/cgroup/memory/raftos'])
                    with open('/sys/fs/cgroup/memory/raftos/memory.limit_in_bytes', 'w') as f:
                        f.write(str(mem_limit))
                    with open('/sys/fs/cgroup/memory/raftos/cgroup.procs', 'w') as f:
                        f.write(str(os.getpid()))
                    print('{} memory slowed'.format(node))
                else:
                    pass

            count += 1
        await asyncio.sleep(wait_time, loop=loop)

if __name__ == '__main__':
    parser = ArgumentParser()

    parser.add_argument('-p', '--start-port', help='Start port', type=int, default=8000)
    parser.add_argument('-n', '--processes', help='Cluster size', type=int, default=4)
    parser.add_argument('-d', '--log-dir', default=os.path.abspath('logs'),
                        dest='log_dir', help="Log dir")

    args = parser.parse_args()

    os.makedirs(args.log_dir, exist_ok=True)

    neighbours = set(
        "127.0.0.1:{}".format(args.start_port + i) for i in range(args.processes)
    )

    processes = set([])

    try:
        for neighbour in neighbours:
            node_args = (args.log_dir, neighbour, neighbours - {neighbour})
            p = Process(target=main, args=node_args)
            log.info("%r", node_args)

            p.start()
            processes.add(p)

        while processes:
            for process in tuple(processes):
                process.join()
                processes.remove(process)
    except KeyboardInterrupt:
        for process in processes:
            if process.is_alive():
                log.warning('Terminating %r', process)
                process.terminate()
