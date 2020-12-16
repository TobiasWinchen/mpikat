#Copyright (c) 2018 Ewan Barr <ebarr@mpifr-bonn.mpg.de>
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.
"""
The Master Controller provides a single interface to all pipeline components in the EDD. State
changes are transmitted in parallel to the individual pipeline components. On
configuration, their dependencies are taken into account.

Configuration Settings
----------------------

The configuration dict contains the configurations of the individual product
pipelines, using their id as key.

In addition, the connection to the data store can be configured here with
the entry ``"data_store" = {"ip": "aaa.bbb.ccc.ddd", port:6347}``

"""

from __future__ import print_function, unicode_literals, division

import logging
import coloredlogs
import json
import os

import git
import tornado
import signal
import yaml
import tempfile
import networkx as nx

from tornado.gen import Return, coroutine, sleep
from katcp import Sensor, AsyncDeviceServer, AsyncReply, FailReply
from katcp.kattypes import request, return_reply, Str, Int

from mpikat.effelsberg.edd.edd_roach2_product_controller import ( EddRoach2ProductController)
from mpikat.effelsberg.edd.edd_digpack_client import DigitiserPacketiserClient
from mpikat.effelsberg.edd.edd_server_product_controller import EddServerProductController

from mpikat.utils.process_tools import ManagedProcess, command_watcher
from mpikat.effelsberg.edd.pipeline.EDDPipeline import EDDPipeline, value_list, getArgumentParser, setup_logger, launchPipelineServer
import mpikat.effelsberg.edd.EDDDataStore as EDDDataStore

log = logging.getLogger("mpikat.effelsberg.edd.EddMasterController")





class EddMasterController(EDDPipeline):
    """
    The main KATCP interface to the EDD backend.
    """

    def __init__(self, ip, port, redis_ip, redis_port, edd_ansible_git_repository_folder, inventory):
        """
        Args:

          ip:           The IP address on which the server should listen
          port:         The port that the server should bind to
          redis_ip:     IP for conenction to the EDD Datastore
          redis_port:   Port for the comnenctioon to the edd data store
          edd_ansible_git_repository_folder:
                        Directory of a (checked out) edd_ansible git repository
                        to be used for provisioning inventory to use for ansible
        """
        EDDPipeline.__init__(self, ip, port, {"data_store": dict(ip=redis_ip, port=redis_port), "id":"There can be only one."})

        self.__controller = {}
        self.__eddDataStore = EDDDataStore.EDDDataStore(redis_ip, redis_port)
        self.__edd_ansible_git_repository_folder = edd_ansible_git_repository_folder
        self.__inventory = inventory
        if not os.path.isdir(self.__edd_ansible_git_repository_folder):
            log.warning("{} is not a readable directory".format(self.__edd_ansible_git_repository_folder))

        self.__provisioned = None
        self.__controller = {}


    def setup_sensors(self):
        """
        Setup katcp monitoring sensors.
        """
        EDDPipeline.setup_sensors(self)

        self._configuration_graph = Sensor.string(
            "configuration_graph",
            description="Graph of configuration",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._configuration_graph)
        self._provision_sensor = Sensor.string(
            "provision",
            description="Current provision configuration",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._provision_sensor)


    @coroutine
    def set(self, config_json):
        log.debug('Received setting:\n' + config_json)
        try:
            cfg = json.loads(config_json)
        except:
            log.error("Error parsing json")
            raise FailReply("Cannot handle config string {} - Not valid json!".format(config_json))
        try:
            fixed_cfg = dict(packetizers={}, products={})
            log.debug('Current config:\n' + json.dumps(self._config, indent=4))

            for item, value in cfg.items():
                if item in self._config['packetizers']:
                    fixed_cfg['packetizers'][item] = value
                elif item in self._config['products']:
                    fixed_cfg['products'][item] = value
                else:
                    log.error("Item {} neither in products nor packetizers!".format(item))
                    raise FailReply("Item {} neither in products nor packetizers!".format(item))

            log.debug("Updated set:\n" + json.dumps(fixed_cfg, indent=4))
        except Exception as E:
            log.error("Error processing setting")
            log.exception(E)
            raise FailReply("Error processing setting {}".format(E))
        try:
            EDDPipeline.set(self, fixed_cfg)
        except Exception as E:
            log.error("Error processing setting in pipeline")
            log.exception(E)
            raise FailReply("Error processing setting in pipeline {}".format(E))


    @coroutine
    def configure(self, config_json):
        """
        Configure the EDD backend

        Args:
            config_json:    A JSON dictionary object containing configuration information

        """
        log.info("Configuring EDD backend for processing")

        #log.info("Resetting data streams")
        #TODo: INterface? Decide if this is always done
        #self.__eddDataStore._dataStreams.flushdb()
        log.debug("Received configuration string: '{}'".format(config_json))

        try:
            cfg = json.loads(config_json)
        except:
            log.error("Error parsing json")
            raise FailReply("Cannot handle config string {} - Not valid json!".format(config_json))

        if not self.__provisioned:
            log.debug("Not provisioned. Using full config.")
            # Do not use set here, as there might not be a basic config from
            # provisioning
            cfg = self.__sanitizeConfig(cfg)
            self._config = cfg
        else:
            EDDPipeline.set(self, cfg)

        yield self._installController(self._config['products'])

        cfs = json.dumps(self._config, indent=4)
        log.debug("Starting configuration:\n" + cfs)


        # Data streams are only filled in on final configure as they may
        # require data from the configure command of previous products. As example, the packetizer
        # data stream has a sync time that is propagated to other components
        # The components are thus configured following the dependency tree,
        # which is a directed acyclic graph (DAG)
        log.debug("Build DAG from config")
        dag = nx.DiGraph()
        for product, product_config in self._config['products'].items():
            log.debug("Adding node: {}".format(product))
            dag.add_node(product)
            if "input_data_streams" in product_config:
                for stream in value_list(product_config["input_data_streams"]):
                    if not stream["source"]:
                        log.warning("Ignoring stream without source for DAG from {}".format(product))
                        continue
                    source_product = stream["source"].split(":")[0]
                    if source_product not in self._config['products']:
                        raise FailReply("{} requires data stream of unknown product {}".format(product, stream["source"]))
                    log.debug("Connecting: {} -> {}".format(source_product, product))
                    dag.add_edge(source_product, product)

        log.debug("Checking for loops in graph")
        try:
            cycle = nx.find_cycle(dag)
            FailReply("Cycle detected in dependency graph: {}".format(cycle))
        except nx.NetworkXNoCycle:
            log.debug("No loop on graph found")
            pass
        graph = "\n".join(["  {} --> {}".format(k[0], k[1]) for k in dag.edges()])
        log.info("Dependency graph of products:\n{}".format(graph))
        self._configuration_graph.set_value(graph)

        configure_results = {}
        configure_futures = []

        @coroutine
        def __process_node(node):
            """
            Wrapper to parallelize configuration of nodes. Any Node will wait for its predecessors to be done.
            """
            #Wait for all predecessors to be finished
            log.debug("DAG Processing {}: Waiting for {} predecessors".format(node, len(list(dag.predecessors(node)))))
            for pre in dag.predecessors(node):
                log.debug('DAG Processing {}: waiting for {}'.format(node, pre))
                while not pre in configure_results:
                    # python3 asyncio coroutines would not run until awaited,
                    # so we could build the graph up front and then execute it
                    # without waiting
                    yield tornado.gen.sleep(0.5)
                log.debug('DAG Processing {}: Predecessor {} done.'.format(node, pre))
                if not configure_results[pre]:
                    log.error('DAG Processing {}: fails due to error in predecessor {}'.format(node, pre))
                    configure_results[node] = False
                    raise Return
                log.debug('DAG Processing {}: Predecessor {} was successfull.'.format(node, pre))

            log.debug("DAG Processing {}: All predecessors done.".format(node))
            try:
                log.debug("DAG Processing {}: Checking input data streams for updates.".format(node))
                if "input_data_streams" in self._config['products'][node]:
                    log.debug('DAG Processing {}: Update input streams'.format(node))
                    for stream in value_list(self._config['products'][node]["input_data_streams"]):
                        product_name, stream_name = stream["source"].split(":")
                        stream.update(self._config['products'][product_name]["output_data_streams"][stream_name])

                log.debug('DAG Processing {}: Set Final config'.format(node))
                yield self.__controller[node].set(self._config['products'][node])
                log.debug('DAG Processing {}: Staring configuration'.format(node))
                yield self.__controller[node].configure()
                log.debug("DAG Processing {}: Getting updated config".format(node))
                cfg = yield self.__controller[node].getConfig()
                log.debug("Got: {}".format(json.dumps(cfg, indent=4)))
                self._config["products"][node] = cfg

            except Exception as E:
                log.error('DAG Processing: {} Exception cought during configuration:\n {}:{}'.format(node, type(E).__name__, E))
                configure_results[node] = False
            else:
                log.debug('DAG Processing: {} Successfully finished configuration'.format(node))
                configure_results[node] = True

        log.debug("Creating processing futures")
        configure_futures = [__process_node(node) for node in dag.nodes()]
        yield configure_futures
        self._configUpdated()
        log.debug("Final configuration:\n '{}'".format(json.dumps(self._config, indent=2)))
        failed_prcts = [k for k in configure_results if not configure_results[k]]
        if failed_prcts:
            raise FailReply("Failed products: {}".format(",".join(failed_prcts)))
        log.info("Updating data streams in database")
        for product in self._config["products"]:
            if "output_data_streams" in product:
                for stream in value_list(product["output_data_streams"]):
                    key = "{}:{}".format(product, stream)
                    self.__eddDataStore.addDataStream(key, ofs)

        log.info("Successfully configured EDD")
        raise Return("Successfully configured EDD")


    @coroutine
    def deconfigure(self):
        """
        Implementation of the deconfiguration request.
        """
        log.info("Deconfiguring all products:")
        log.debug("Sending deconfigure to {} products: {}".format(len(self.__controller.keys()), "\n - ".join(self.__controller.keys()) ))
        futures = []
        for cid, controller in self.__controller.items():
            futures.append(controller.deconfigure())
        yield futures
        self._configuration_graph.set_value("")


    @coroutine
    def capture_start(self):
        """
        Implementation of the capture-start.
        """
        log.debug("Sending capture_start to {} products: {}".format(len(self.__controller.keys()), "\n - ".join(self.__controller.keys()) ))
        futures = []
        for cid, controller in self.__controller.items():
            futures.append(controller.capture_start())
        yield futures


    @coroutine
    def capture_stop(self):
        """
        Stop the EDD backend processing
        """
        log.debug("Sending capture_stop to {} products: {}".format(len(self.__controller.keys()), "\n - ".join(self.__controller.keys()) ))
        futures = []
        for cid, controller in self.__controller.items():
            futures.append(controller.capture_stop())
        yield futures


    @coroutine
    def measurement_prepare(self, config_json=""):
        """
        """
        log.debug("Received measurement prepare ... ")
        try:
            cfg = json.loads(config_json)
        except:
            log.error("Error parsing json")
            raise FailReply("Cannot handle config string {} - Not valid json!".format(config_json))

        log.debug("Sending measurement_prepare to {} products: {}".format(len(self.__controller.keys()), "\n - ".join(self.__controller.keys()) ))
        futures = []
        for cid, controller in self.__controller.items():
            futures.append(controller.measurement_prepare(cfg))
        yield futures


    @coroutine
    def measurement_start(self):
        """
        """
        log.debug("Sending measurement_start to {} products: {}".format(len(self.__controller.keys()), "\n - ".join(self.__controller.keys()) ))
        futures = []
        for cid, controller in self.__controller.items():
            futures.append(controller.measurement_start())
        yield futures


    @coroutine
    def measurement_stop(self):
        """
        """
        log.debug("Sending measurement_stop to {} products: {}".format(len(self.__controller.keys()), "\n - ".join(self.__controller.keys()) ))
        futures = []
        for cid, controller in self.__controller.items():
            futures.append(controller.measurement_stop())
        yield futures


    @request(Str())
    @return_reply()
    def request_provision(self, req, name):
        """
        Loads a provision configuration and dispatch it to ansible, sets and
        connects the data streams for all products and load their initial
        configuration.
        """
        log.info("Provision request received")
        @coroutine
        def wrapper():
            try:
                yield self.provision(name)
            except FailReply as fr:
                log.error(str(fr))
                req.reply("fail", str(fr))
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(wrapper)
        raise AsyncReply


    @request()
    @return_reply()
    def request_list_provisions(self, req):
        """
        List all provision descriptions available in the
        edd_ansible_git_repository_folder.
        """
        @coroutine
        def wrapper():
            try:
                all_files = os.listdir(os.path.join(self.__edd_ansible_git_repository_folder, "provison_descriptions"))
                yml_files = [f for f in all_files if f.endswith('.yml')]
                l = [" - {}".format(l[:-4]) for l in yml_files if l[:-4] + '.json' in all_files]
                req.reply("ok", "\nAvailable provision descriptions:\n" +"\n".join(l))

            except FailReply as fr:
                log.error(str(fr))
                req.reply("fail", str(fr))
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(wrapper)


    @coroutine
    def __ansible_subplay_executioner(self, play, additional_args=""):
        """
        Uses ansible-playbook to execute thegiven play by writing it in a tempfile.
        Args:
            play                The play to be executed
            additional_args     Additional args added to the ansible execution, e.g. --tags=stop
        """
        playfile = tempfile.NamedTemporaryFile(delete=False)
        yaml.dump([play], playfile)
        playfile.close()

        try:
            yield command_watcher("ansible-playbook -i {} {} {}".format(self.__inventory, playfile.name, additional_args), env={"ANSIBLE_ROLES_PATH":os.path.join(self.__edd_ansible_git_repository_folder, "roles")}, timeout=240)
        except Exception as E:
            playfile.unlink()
            raise RuntimeError("Error {} processing play:\n {}".format(E, yaml.dump(play)))




    @coroutine
    def provision(self, description):
        """
        Provision the EDD using the provided provision description.

        Args:
            description: description of the provision. This has to be a string of format

                - ::`NAME`                 to load NAME.json and NAME.yml, or
                - ::`NAME1.yml;NAME2.json` to load different yml / json configs
        """
        os.chdir(self.__edd_ansible_git_repository_folder)
        log.debug("Provision description {} from directory {}".format(description, os.getcwd()))
        if description.startswith('"'):
            description = description.lstrip('"')
            description = description.rstrip('"')

        descr_subfolder = "provison_descriptions"
        if ";" in description:
            description = description.split(';')
            if description[0].endswith("yml"):
                playbook_file = os.path.join(descr_subfolder, description[0])
                basic_config_file = os.path.join(descr_subfolder, description[1])
            else:
                playbook_file = os.path.join(descr_subfolder, description[1])
                basic_config_file = os.path.join(descr_subfolder, description[0])
        else:
            playbook_file = os.path.join(descr_subfolder, description + ".yml")
            basic_config_file = os.path.join(descr_subfolder, description + ".json")

        log.debug("Loading provision description files: {} and {}".format(playbook_file, basic_config_file))
        if not os.path.isfile(playbook_file):
            raise FailReply("cannot find playbook file {}".format(playbook_file))
        if not os.path.isfile(basic_config_file):
            raise FailReply("cannot find config file {}".format(basic_config_file))

        try:
            provision_playbook = yaml.load(open(playbook_file,'r'))
        except Exception as E:
            log.error(E)
            raise FailReply("Error in provisioning, cannot load file: {}".format(E))

        try:
            subplay_futures = []
            log.debug("Executing playbook as {} seperate subplays in parallel".format(len(provision_playbook)))

            self.__provisioned = playbook_file
            self._provision_sensor.set_value(playbook_file)
            for play in provision_playbook:
                subplay_futures.append(self.__ansible_subplay_executioner(play))

            yield subplay_futures
        except Exception as E:
            raise FailReply("Error in provisioning thrown by ansible {}".format(E))

        yield self._loadBasicConfig(basic_config_file)



    @request(Str())
    @return_reply()
    def request_load_basic_config(self, req, name):
        """
        Loads a provision configuration and dispatch it to ansible and sets the
        data streams for all products

        Note:
            The provision request already loads the basic configuration so that
            this request does NOT need to be send - it is intended as an
            development tool only.
        """
        log.info("load-basic-config request received")
        @coroutine
        def wrapper():
            try:
                descr_subfolder = os.path.join(self.__edd_ansible_git_repository_folder, "provison_descriptions")
                basic_config_file = os.path.join(descr_subfolder, name)

                yield self._loadBasicConfig(basic_config_file)
            except FailReply as fr:
                log.error(str(fr))
                req.reply("fail", str(fr))
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(wrapper)
        raise AsyncReply


    @coroutine
    def _loadBasicConfig(self, basic_config_file):
        """
        Actually loads the basic configuration, called by provision and load
        basic configuarion requests.
        """
        try:
            with open(basic_config_file) as cfg:
                basic_config = json.load(cfg)
        except Exception as E:
            raise FailReply("Error reading config {}".format(E))
        log.debug("Read basic config: {}".format(json.dumps(basic_config, indent=4)))

        basic_config = self.__sanitizeConfig(basic_config)

        yield self._installController(basic_config['products'])

        # Retrieve default configs from products and merge with basic config to
        # have full config locally.
        self._config = self._default_config.copy()

        self._config["products"] = {}

        for product in basic_config['products'].values():
            log.debug("Retrieve basic config for {}".format(product["id"]))
            controller = self.__controller[product["id"]]

            log.debug("Checking basic config {}".format(json.dumps(product, indent=4)))
            yield controller.set(product)
            cfg = yield controller.getConfig()
            log.debug("Got: {}".format(json.dumps(cfg, indent=4)))

            cfg['data_store'] = self._default_config['data_store']
            self._config["products"][cfg['id']] = cfg

        self._configUpdated()


    def __sanitizeConfig(self, config):
        """
        Ensures config products are a dict with product['id'] as key.
        """
        log.debug("Sanitze config")
        if not 'products' in config:
            config["products"] = {}
        elif isinstance(config['products'], list):
            d = {p['id']:p for p in config['products']}
            config['products'] = d
        return config


    @coroutine
    def _installController(self, config = {}):
        """
        Ensures that a controller for all products known to redis exists. In
        case a config is provided it is also ensured that controller exists for
        all products in the config.

        Note:
            If a config contains a product unknonw to the data store, the
            configuration must provide **ip** and **port** of the product.
        """
        log.debug("Installing controller for {} registered products.".format(len(self.__eddDataStore.products)))
        for product in self.__eddDataStore.products:
            if product['id'] == self._config["id"]:
                log.debug("Ignoring self for control: {}".format(product['id']))
                continue
            if product['id'] in self.__controller:
                log.debug("Controller for {} already installed.".format(product['id']))
                controller = self.__controller[product['id']]
                if (product['ip'] == controller.ip) and (product['port'] == controller.port):
                    # No action needed as identical product
                    log.debug("Ip and port matching, doing nothing.")
                    continue
                else:
                    log.debug("Ip and port not matching, Checking controller health.")
                    ping = yield controller.ping()
                    if ping:
                        log.warning("Controller for {} already present at {}:{} and reachable. Not updating with new controller for product at {}:{}".format(product,controller.ip, controller.port, product['ip'], product['port']))
                        continue
                    else:
                        log.warning("Controller for {} already present but not reachable at {}:{}. Replacing with new controller for product at {}:{}".format(product,controller.ip, controller.port, product['ip'], product['port']))
            try:
                controller = EddServerProductController(product['id'], product["ip"], product["port"])
            except:
                log.error("Cannot create controller for {} at {}:{}. Removing product from redis.".format(product['id'], product["ip"], product["port"]))
                self.__eddDataStore.removeProduct(product)
            else:
                ping = yield controller.ping()
                if ping:
                    log.debug("Reached product {} at {}:{}.".format(product['id'], product["ip"], product["port"]))
                    self.__controller[product['id']] = controller
                else:
                    log.debug("Cannot reach product {} at {}:{}. Removing product from redis.".format(product['id'], product["ip"], product["port"]))
                    self.__eddDataStore.removeProduct(product)


        if config: log.debug("Checking product for config")
        for product in config.values():
            if product['id'] not in self.__controller:
                log.warning('Product {} found in configuration, but no product registered. Manually adding controller for product.'.format(product['id']))
                if ('ip' not in product) or ('port' not in product):
                    raise RuntimeError("No controller for {} could be installed automatically. Manual config requires IP and PORT, which were not provided!".format(product['id']))
                log.warning('Manually installed product {} at {}:{}'.format(product['id'], product['ip'], product['port']))
                self.__controller[product['id']] = EddServerProductController(product['id'], product["ip"], product["port"])

            ping = yield self.__controller[product['id']].ping()
            if not ping:
                raise RuntimeError('Product {} required by config but not reachable.'.format(product['id']))




    @request()
    @return_reply()
    def request_deprovision(self, req):
        """
        Deprovision EDD - stop all ansible containers launched in recent
        provision cycle and removes all controllers  from the data base.
        """
        log.info("Deprovision request received")
        @coroutine
        def wrapper():
            try:
                yield self.deprovision()
            except FailReply as fr:
                log.error(str(fr))
                req.reply("fail", str(fr))
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(wrapper)
        raise AsyncReply


    @coroutine
    def deprovision(self):
        log.debug("Deprovision {}".format(self.__provisioned))
        if self.__provisioned:
            try:
                provision_playbook = yaml.load(open(self.__provisioned,'r'))
            except Exception as E:
                log.error(E)
                raise FailReply("Error in deprovisioning, cannot load file: {}".format(E))

            try:
                subplay_futures = []
                log.debug("Executing playbook as {} seperate subplays in parallel".format(len(provision_playbook)))
                for play in provision_playbook:
                    subplay_futures.append(self.__ansible_subplay_executioner(play, "--tags=stop"))

                yield subplay_futures
            except Exception as E:
                raise FailReply("Error in deprovisioning thrown by ansible {}".format(E))

        self.__controller = {}
        self.__eddDataStore._products.flushdb()
        self.__provisioned = None
        self._provision_sensor.set_value("Unprovisioned")
        self._configuration_graph.set_value("")
        self._config = self._default_config.copy()


    @request(Str())
    @return_reply()
    def request_provision_update(self, req, repository=""):
        """
        Clones or pulls updates for the git repository

        """
        @coroutine
        def wrapper():
            try:
                yield self.provision_update(name)
            except FailReply as fr:
                log.error(str(fr))
                req.reply("fail", str(fr))
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(wrapper)
        raise AsyncReply


    @coroutine
    def provision_update(self, repository=""):
        """
        Clones or pulls updates for the git repository
        """

        if not os.path.isdir(self.__edd_ansible_git_repository_folder):
            raise RuntimeError("Directory {} does not exist!".format(self.__edd_ansible_git_repository_folder))
            #log.debug("Directory {} does not exist, cloning from {}".format(self.__edd_ansible_git_repository_folder, repository))
            #repo = git.Repo.clone_from(repo_source, target_dir)
        else:
            repo = git.Repo(self.__edd_ansible_git_repository_folder)

        if repo.is_dirty():
            raise RuntimeError("Trying to update dirty repository. Please fix manually!")

        comm = repo.remote().pull()[0].commit
        log.info("Updated to latest commit: {}, {}\n    {}\n\n    {}".format(comm.hexsha, comm.authored_datetime.ctime(), comm.author, comm.message))




if __name__ == "__main__":
    parser = getArgumentParser()

    parser.add_argument('--edd_ansible_repository', dest='edd_ansible_git_repository_folder', type=str, default=os.path.join(os.getenv("HOME"), "edd_ansible"), help='The path to a git repository for the provisioning data')

    parser.add_argument('--edd_ansible_inventory', dest='inventory', type=str,
            default="effelsberg", help='The inventory to use with the ansible setup')
    args = parser.parse_args()
    setup_logger(args)

    server = EddMasterController(
        args.host, args.port,
        args.redis_ip, args.redis_port, args.edd_ansible_git_repository_folder, args.inventory)
    launchPipelineServer(server, args)
