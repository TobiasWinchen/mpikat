"""
Get information on numa architecture.
"""

from __future__ import print_function, division, unicode_literals

import os
import socket
import fcntl
import struct

import logging

try:
    import mpikat.utils.pynvml as pynvml
    pynvml.nvmlInit()
    __haspynvml = True
except Exception as E:
    logging.warning("pynvml not available. Cannot use GPUs (if any)")
    __haspynvml = False

__numaInfo = None


def expandlistrange(lr):
    """
    Expands a string containing a list of numbers '1,2,3-5' to an actual list [1,2,3,4,5]
    """
    output = set()
    if not lr.isspace():
        for ir in lr.split(','):
            try:
                il = ir.split('-')
                output.update([str(n) for n in range(int(il[0]), int(il[-1]) + 1)])
            except Exception as E:
                print("lr: {}".format(lr))
                print("ir: {}".format(ir))
                print("il: {}".format(il))
                raise E
    return output



def updateInfo():
    """
    Updates the info dictionary.
    """
    logging.debug("Update numa dictionary")
    global __numaInfo
    __numaInfo = {}
    nodes = open("/sys/devices/system/node/possible").read().strip().split('-')
    nodes = [str(n) for n in range(int(nodes[0]), int(nodes[-1]) + 1)]
    if os.getenv('EDD_ALLOWED_NUMA_NODES'):
        logging.debug("Restricting numa nodes to nodes listed in EDD_ALLOWED_NUMA_NODES")
        allowed_nodes = expandlistrange(os.getenv('EDD_ALLOWED_NUMA_NODES'))
        for noderange in os.getenv('EDD_ALLOWED_NUMA_NODES').split(','):
            noderange = noderange.split('-')
            allowed_nodes.update([str(n) for n in range(int(noderange[0]), int(noderange[-1]) + 1)])
        for node in allowed_nodes.difference(nodes):
            logging.warning("Node {} in EDD_ALLOWED_NUMA_NODES, but not available on host!".format(node))
        allowed_nodes.intersection_update(nodes)
        nodes = list(allowed_nodes)

    isolated_cpus = expandlistrange(open('/sys/devices/system/cpu/isolated').read())

    for node in nodes:
        logging.debug("Preparing node {} of {}".format(node, len(nodes)))
        __numaInfo[node] = {"net_devices":{} }

        cpulist = expandlistrange(open('/sys/devices/system/node/node' + node + '/cpulist').read())

        __numaInfo[node]['cores'] = list(cpulist.difference(isolated_cpus))
        __numaInfo[node]['cores'].sort()
        __numaInfo[node]['isolated_cores'] = list(isolated_cpus.intersection(cpulist))
        __numaInfo[node]['isolated_cores'].sort()

        __numaInfo[node]['gpus'] = []
        __numaInfo[node]["net_devices"] = {}
        logging.debug("  found {} Cores.".format(len(__numaInfo[node]['cores'])))
    logging.debug("Available nodes: {}".format(__numaInfo.keys()))

    #logging.debug(__numaInfo)
    # check network devices
    for device in os.listdir("/sys/class/net/"):
        logging.debug("Associate network device {} to node".format(device))
        d = "/sys/class/net/" + device + "/device/numa_node"
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if os.path.isfile(d):
            node = open(d).read().strip()
            if node not in __numaInfo:
                logging.debug("Device on node {}, but node not in list of nodes. Possible node was deacitvated.".format(node))
                continue

            __numaInfo[node]["net_devices"][device] = {}
            logging.debug("  - found node {}".format(node))
            __numaInfo[node]["net_devices"][device]['ip'] = ""
            try:
                ip = socket.inet_ntoa(fcntl.ioctl(s.fileno(),
                    0x8915,  # SIOCGIFADDR
                    struct.pack('256s', device[:15].encode('ascii')))[20:24])
                __numaInfo[node]["net_devices"][device]['ip'] = ip
            except IOError as e:
                logging.warning(" Cannot associate device {} to a node: {}".format(device, node))

            d = "/sys/class/net/" + device + "/speed"
            speed = 0
            if os.path.isfile(d):
                try:
                    speed = open(d).read()
                except:
                    logging.warning(" Cannot acess speed for device {}: {}".format(device, node))

            __numaInfo[node]["net_devices"][device]['speed'] = int(speed)

    # check cuda devices:
    if __haspynvml:

        nGpus = pynvml.nvmlDeviceGetCount()

        for i in range(nGpus):
            handle = pynvml.nvmlDeviceGetHandleByIndex(i)
            pciInfo = pynvml.nvmlDeviceGetPciInfo(handle)

            d = '/sys/bus/pci/devices/' + pciInfo.busId.decode('ascii') + "/numa_node"
            node = open(d).read().strip()
            if node not in __numaInfo:
                logging.debug("Device on node {}, but node not in list of nodes. Possible node was deacitvated.".format(node))
                continue
            __numaInfo[node]['gpus'].append(str(i))


def getInfo():
    """
    Returns dict with info on numa configuration. For every numa node the dict
    contains a dict with the associated ressources.
    """
    global __numaInfo
    if not __numaInfo:
        updateInfo()
    return __numaInfo


def getFastestNic(numa_node=None):
    """
    Returns (name, description) of the fastest nic (on given numa_node)
    """
    if numa_node is not None:
        nics = getInfo()[numa_node]["net_devices"]
        if nics:
            fastest_nic = max(nics.keys(), key=lambda k: nics[k]['speed'])
            return fastest_nic, nics[fastest_nic]
        else:
            logging.warning("Using dummy nic - are you in debug mode?")
            return "dummy_nic", {"ip":"127.0.0.1","speed":12345}
    else:
        f = None
        d = None
        for node in getInfo():
           fn, fnd =  getFastestNic(node)
           if f is not None:
               if fnd['speed'] < d['speed']:
                   continue
           f = fn
           d = fnd
           d['node'] = node
        if not f:
            logging.warning("Using dummy nic - are you in debug mode?")
            return "dummy_nic", {"ip":"127.0.0.1","speed":12345}

        return f, d


if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    print(getInfo())
    for node, res in getInfo().items():
        print("NUMA Node: {}".format(node))
        print("  CPU Cores: {}".format(", ".join(res['cores'])))
        print("  Isolated CPU Cores: {}".format(", ".join(res['isolated_cores'])))
        print("  GPUs: {}".format(", ".join(map(str, res['gpus']))))
        print("  Network interfaces:")
        for nic, info in res['net_devices'].items():
            print("     {nic}: ip = {ip}, speed = {speed} Mbit/s ".format(nic=nic, **info))

        nics = res['net_devices']
        if len(nics) > 0:
            fastest_nic = max(nics.keys(), key=lambda k: nics[k]['speed'])
            print('   -> Fastest interface: {}'.format(fastest_nic))

    print("Fastest nic over all: {}".format(getFastestNic()))
