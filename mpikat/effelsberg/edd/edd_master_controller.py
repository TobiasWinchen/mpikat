from __future__ import print_function, unicode_literals, division
"""
Copyright (c) 2018 Ewan Barr <ebarr@mpifr-bonn.mpg.de>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import logging
import coloredlogs
import json
import os

import git
import tornado
import signal

from tornado.gen import Return, coroutine
from katcp import Sensor, AsyncDeviceServer, AsyncReply, FailReply
from katcp.kattypes import request, return_reply, Str, Int

from mpikat.effelsberg.edd.edd_roach2_product_controller import ( EddRoach2ProductController)
from mpikat.effelsberg.edd.edd_digpack_client import DigitiserPacketiserClient
from mpikat.effelsberg.edd.edd_server_product_controller import EddServerProductController

from mpikat.utils.process_tools import ManagedProcess, command_watcher
import mpikat.effelsberg.edd.pipeline.EDDPipeline as EDDPipeline
import mpikat.effelsberg.edd.EDDDataStore as EDDDataStore

log = logging.getLogger("mpikat.effelsberg.edd.EddMAsterController")

class EddMasterController(EDDPipeline.EDDPipeline):
    """
    The main KATCP interface for the EDD backend
    """
    VERSION_INFO = ("mpikat-edd-api", 0, 2)
    BUILD_INFO = ("mpikat-edd-implementation", 0, 2, "rc1")

    def __init__(self, ip, port, redis_ip, redis_port, edd_ansible_git_repository_folder, inventory):
        """
        @brief       Construct new EddMasterController instance

        @params  ip           The IP address on which the server should listen
        @params  port         The port that the server should bind to
        @params  redis_ip     IP for conenction to the EDD Datastore
        @params  redis_port   Port for the comnenctioon to the edd data store
        @params  edd_ansible_git_repository_folder
                              Directory of a (checked out) edd_ansible git
                              repository to be used for provisioning
        @params inventory to use for ansible
        """
        EDDPipeline.EDDPipeline.__init__(self, ip, port, {"data_store": dict(ip=redis_ip, port=redis_port), "skip_packetizer_config":False})

        self.__controller = {}
        self.__eddDataStore = EDDDataStore.EDDDataStore(redis_ip, redis_port)
        self.__edd_ansible_git_repository_folder = edd_ansible_git_repository_folder
        self.__inventory = inventory
        if not os.path.isdir(self.__edd_ansible_git_repository_folder):
            log.warning("{} is not a readable directory".format(self.__edd_ansible_git_repository_folder))

        self.__provisioned = None


    @request()
    @return_reply(Int())
    def request_reset_edd_layout(self, req):
        """
        @brief   Reset the edd layout - after a change of the edd layout via ansible, the iformation about the available products is updated

        @detail  Reread the layout of the edd setup from the ansible data set.
        @return  katcp reply object [[[ !product-list ok | (fail [error description]) <number of configured producers> ]]
        """
        log.info("reset-edd-layout requested")
        self.__eddDataStore.updateProducts()
        self.__controller = {}
        # add a control handle for each product
        #for productid in self.__eddDataStore.products:
        #    product = self.__eddDataStore.getProduct(productid)
        #    # should query the product tog et the right type of controller

        #    self.__controller[productid] = EddServerProductController(self, productid, (product["address"], product["port"]) )

        return ("ok", len(self.__eddDataStore.products))


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

            for item, value in cfg.iteritems():
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
            EDDPipeline.EDDPipeline.set(self, fixed_cfg)
        except Exception as E:
            log.error("Error processing setting in pipeline")
            log.exception(E)
            raise FailReply("Error processing setting in pipeline {}".format(E))


    @coroutine
    def configure(self, config_json):
        """
        @brief   Configure the EDD backend

        @param   config_json    A JSON dictionary object containing configuration information

        @param   The following global configuration option can be set in the configuration:
            "data_store" = {"ip": "aaa.bbb.ccc.ddd", port:6347}

        """
        log.info("Configuring EDD backend for processing")

        self.__eddDataStore.updateProducts()
        log.info("Resetting data streams")
        #TODo: INterface? Decide if this is always done
        self.__eddDataStore._dataStreams.flushdb()
        log.debug("Received configuration string: '{}'".format(config_json))

        try:
            cfg = json.loads(config_json)
        except:
            log.error("Error parsing json")
            raise FailReply("Cannot handle config string {} - Not valid json!".format(config_json))

        if not self.__provisioned:
            # Do not use set here, as there might not be a basic config from
            # provisioning
            cfg = self.__sanitizeConfig(cfg)
            self._config = cfg
        else:
            EDDPipeline.EDDPipeline.set(self, cfg)

        cfs = json.dumps(self._config, indent=4)
        log.info("Final configuration:\n" + cfs)

        # ToDo: Check if provisioned
        if not self.__provisioned:
            self._installController(self._config)


        # Data streams are only filled in on final configure as they may
        # require data from the configure command. As example,t he packetizer
        # dat atream has a sync time that is propagated to other components

        # Get output streams from packetizer and configure packetizer
        log.info("Configuring digitisers/packetisers")
        for packetizer in self._config['packetizers'].itervalues():
            if self._config["skip_packetizer_config"]:
                log.warning("Skipping packetizer configuration as requested in config!")
            else:
                yield self.__controller[packetizer["id"]].configure(packetizer)

            ofs = dict(format="MPIFR_EDD_Packetizer",
                        sample_rate=float(packetizer["sampling_rate"]) / int(packetizer["predecimation_factor"]),
                        bit_depth=packetizer["bit_width"])
            ofs["sync_time"] = yield self.__controller[packetizer["id"]].get_sync_time()
            log.info("Sync Time for {}: {}".format(packetizer["id"], ofs["sync_time"]))

            key = packetizer["id"] + ":" + "v_polarization"
            ofs["ip"] = packetizer["v_destinations"].split(':')[0]
            ofs["port"] = packetizer["v_destinations"].split(':')[1]
            self.__eddDataStore.addDataStream(key, ofs)

            key = packetizer["id"] + ":" + "h_polarization"
            ofs["ip"] = packetizer["h_destinations"].split(':')[0]
            ofs["port"] = packetizer["h_destinations"].split(':')[1]
            self.__eddDataStore.addDataStream(key, ofs)
            yield self.__controller[packetizer["id"]].populate_data_store(self.__eddDataStore.host, self.__eddDataStore.port)

        log.debug("Identify additional output streams")
        # Get output streams from products
        for product in self._config['products'].itervalues():

            if not "output_data_streams" in product:
                continue

            for k, i in product["output_data_streams"].iteritems():
                # look up data stream in storage
                dataStream = self.__eddDataStore.getDataFormatDefinition(i['format'])
                dataStream.update(i)
                key = "{}:{}".format(product['id'], k)
                if 'ip' in i:
                    pass
                # ToDo: mark multicast adress as used xyz.mark_used(i['ip'])
                else:
                # ToDo: get MC address automatically if not set
                    raise NotImplementedError("Missing ip statement! Automatic assignment of IPs not implemented yet!")
                self.__eddDataStore.addDataStream(key, i)

        log.debug("Connect data streams with high level description")
        for product in self._config['products'].itervalues():
            log.debug("checking product: {}".format(product["id"]))
            if not "input_data_streams" in product:
                log.warning("Product: {} without input data streams".format(product['id']))
                continue
            counter = 0
            for k in product["input_data_streams"]:
                if isinstance(product["input_data_streams"], dict):
                    log.debug("input stream of type dict, k = {}".format(k))
                    inputStream = product["input_data_streams"][k]
                elif isinstance(product["input_data_streams"], list):
                    inputStream = k
                    k = counter
                    counter += 1
                    log.debug("input stream of type list, k = {}".format(k))
                else:
                    raise RuntimeError("Input streams has to be dict or list, got: {}!".format(type(product["input_data_streams"])))

                datastream = self.__eddDataStore.getDataFormatDefinition(inputStream['format'])
                datastream.update(inputStream)
                if not "source" in inputStream:
                    log.debug("Source not definied for input stream {} of {} - no lookup but assuming manual definition!".format(k, product['id']))
                    continue
                s = inputStream["source"]

                if not self.__eddDataStore.hasDataStream(s):
                    log.error("Unknown data stream {}. Corresponding input stream: {}".format(s, inputStream))
                    raise RuntimeError("Unknown data stream {} !".format(s))

                log.debug("Updating {} of {} - with {}".format(k, product['id'], s))
                datastream.update(self.__eddDataStore.getDataStream(s))
                product["input_data_streams"][k] = datastream

        log.debug("Updated configuration:\n '{}'".format(json.dumps(self._config, indent=2)))
        log.info("Configuring products")
        log.debug("Sending configure to {} products: {}".format(len(self.__controller.keys()), "\n - ".join(self.__controller.keys()) ))
        futures = []
        for product in self._config["products"].itervalues():
            #inject global data store values into product configuration
            product["data_store"] = self._config["data_store"]
            futures.append(self.__controller[product['id']].configure(product))
        yield futures

        self._edd_config_sensor.set_value(json.dumps(self._config))
        log.info("Successfully configured EDD")
        raise Return("Successfully configured EDD")


    @coroutine
    def deconfigure(self):
        """
        @brief      Deconfigure the EDD backend.
        """
        log.info("Deconfiguring all products:")
        log.debug("Sending deconfigure to {} products: {}".format(len(self.__controller.keys()), "\n - ".join(self.__controller.keys()) ))
        futures = []
        for cid, controller in self.__controller.iteritems():
            futures.append(controller.deconfigure())
        yield futures
        #self._update_products_sensor()


    @coroutine
    def capture_start(self):
        """
        @brief      Start the EDD backend processing

        @detail     Not all processing components will respond to a capture_start request.
                    For example the packetisers and roach2 boards will in general constantly
                    stream once configured. Processing components (such as the FITS interfaces)
                    which must be cognisant of scan boundaries should respond to this request.

        @note       This method may be updated in future to pass a 'scan configuration' containing
                    source and position information necessary for the population of output file
                    headers.
        """
        log.debug("Sending capture_start to {} products: {}".format(len(self.__controller.keys()), "\n - ".join(self.__controller.keys()) ))
        futures = []
        for cid, controller in self.__controller.iteritems():
            futures.append(controller.capture_start())
        yield futures


    @coroutine
    def capture_stop(self):
        """
        @brief      Stop the EDD backend processing

        @detail     Not all processing components will respond to a capture_stop request.
                    For example the packetisers and roach2 boards will in general constantly
                    stream once configured. Processing components (such as the FITS interfaces)
                    which must be cognisant of scan boundaries should respond to this request.
        """
        log.debug("Sending capture_stop to {} products: {}".format(len(self.__controller.keys()), "\n - ".join(self.__controller.keys()) ))
        futures = []
        for cid, controller in self.__controller.iteritems():
            futures.append(controller.capture_stop())
        yield futures


    @coroutine
    def measurement_prepare(self, config_json=""):
        """"""
        log.debug("Received measurement prepare ... ")
        try:
            cfg = json.loads(config_json)
        except:
            log.error("Error parsing json")
            raise FailReply("Cannot handle config string {} - Not valid json!".format(config_json))

        log.debug("Sending measurement_prepare to {} products: {}".format(len(self.__controller.keys()), "\n - ".join(self.__controller.keys()) ))
        futures = []
        for cid, controller in self.__controller.iteritems():
            futures.append(controller.measurement_prepare(cfg))
        yield futures


    @coroutine
    def measurement_start(self):
        """
        """
        log.debug("Sending measurement_start to {} products: {}".format(len(self.__controller.keys()), "\n - ".join(self.__controller.keys()) ))
        futures = []
        for cid, controller in self.__controller.iteritems():
            futures.append(controller.measurement_start())
        yield futures


    @coroutine
    def measurement_stop(self):
        """
        """
        log.debug("Sending measurement_stop to {} products: {}".format(len(self.__controller.keys()), "\n - ".join(self.__controller.keys()) ))
        futures = []
        for cid, controller in self.__controller.iteritems():
            futures.append(controller.measurement_stop())
        yield futures


    def reset(self):
        """
        Resets the EDD, i.e. flusing all data bases. Note that runing containers are not stopped.
        """
        self.__eddDataStore._dataStreams.flushdb()
        self.__provisioned = None


    @request(Str())
    @return_reply()
    def request_provision(self, req, name):
        """
        @brief   Loads a provision configuration and dispatch it to ansible and sets the data streams for all products

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
        @brief List all availbale provision descriptions 

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
    def provision(self, description):
        """
        @brief provision the edd with provided provision description.

        @params description fo the provision. This has to be a string of format
                NAME                 to load NAME.json and NAME.yml
                NAME1.yml;NAME2.json to load different yml / json configs
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
            yield command_watcher("ansible-playbook -i {} {}".format(self.__inventory, playbook_file),
                    env={"ANSIBLE_ROLES_PATH":os.path.join(self.__edd_ansible_git_repository_folder,
                        "roles")}, timeout=240)
        except Exception as E:
            raise FailReply("Error in provisioning thrown by ansible {}".format(E))
        self.__provisioned = playbook_file

        try:
            with open(basic_config_file) as cfg:
                basic_config = json.load(cfg)
        except Exception as E:
            raise FailReply("Error reading config {}".format(E))
        log.debug("Read basic config: {}".format(json.dumps(basic_config, indent=4)))

        self.__eddDataStore.updateProducts()
        basic_config = self.__sanitizeConfig(basic_config)
        self._installController(basic_config)

        # Retrieve default configs from products and merge with basic config to
        # have full config locally.
        self._config = self._default_config.copy()
        self._config["products"] = {}
        self._config["packetizers"] = basic_config['packetizers']

        for product in basic_config['products'].itervalues():
            log.debug("Retrieve basic config for {}".format(product["id"]))
            controller = self.__controller[product["id"]]
            if not isinstance(controller, EddServerProductController):
                # ToDo: unfify interface of DigitiserPacketiserClient and EDD Pipeline,
                # they should be identical. Here the DigitiserPacketiserClient
                # should mimick the getConfig and translate it as the actual
                # request is not there
                continue

            log.debug("Checking basic config {}".format(json.dumps(product, indent=4)))
            yield controller.set(product)
            cfg = yield controller.getConfig()
            log.debug("Got: {}".format(json.dumps(cfg, indent=4)))

            #cfg = EDDPipeline.updateConfig(cfg, product)
            self._config["products"][cfg['id']] = cfg

        self._configUpdated()


    def __sanitizeConfig(self, config):
        """
        Ensures config has products and packaetizers
        """
        log.debug("Sanitze config")
        if 'packetisers' in config:
            config['packetizers'] = config.pop('packetisers')
        elif "packetizers" not in config:
            log.warning("No packetizers in config!")
            config["packetizers"] = {}

        if isinstance(config['packetizers'], list):
            d = {p['id']:p for p in config['packetizers']}
            config["packetizers"] = d

        if not 'products' in config:
            config["products"] = {}
        elif isinstance(config['products'], list):
            d = {p['id']:p for p in config['products']}
            config['products'] = d
        return config


    def _installController(self, config):
        """
        Ensure a controller exists for all components in a configuration
        """
        log.debug("Installing controller for products.")
        config = self.__sanitizeConfig(config)

        for packetizer in config['packetizers'].itervalues():
            if packetizer["id"] in self.__controller:
                log.warning("Controller for {} already there, not replacing with new controller!".format(packetizer["id"]))
            else:
                log.debug("Adding new controller for {}".format(packetizer["id"]))
                self.__controller[packetizer["id"]] = DigitiserPacketiserClient(*packetizer["address"])
                self.__controller[packetizer["id"]].populate_data_store(self.__eddDataStore.host, self.__eddDataStore.port)

        for product in config["products"].itervalues():
            if product['id'] in self.__controller:
                log.warning("Controller for {} already there".format(product['id']))
            else:
                log.debug("Adding new controller for {}".format(product["id"]))
            if "type" in product and product["type"] == "roach2":
                    self.__controller[product['id']] = EddRoach2ProductController(self, product['id'],
                                                                            (self._r2rm_host, self._r2rm_port))
            elif product['id']:
                if product['id'] in self.__eddDataStore.products:
                    cfg = self.__eddDataStore.getProduct(product['id'])
                    cfg.update(product)
                    self.__controller[product['id']] = EddServerProductController(cfg['id'], cfg["address"], cfg["port"])
                else:
                    log.warning("Manual setup of product {} - require address and port properties".format(product['id']))
                    self.__controller[product['id']] = EddServerProductController(product['id'], product["address"], product["port"])


    @request()
    @return_reply()
    def request_deprovision(self, req):
        """
        @brief   Deprovision EDD - stop all ansible containers launched in recent provision cycle.
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
                yield command_watcher("ansible-playbook -i {} {} --tags=stop".format(self.__inventory, self.__provisioned),
                        env={"ANSIBLE_ROLES_PATH":os.path.join(self.__edd_ansible_git_repository_folder,
                            "roles")}, timeout=240)
            except Exception as E:
                raise FailReply("Error in provisioning {}".format(E))
            self.__eddDataStore.updateProducts()
        self.__eddDataStore.flush()
        self.__provisioned = None


    @request(Str())
    @return_reply()
    def request_provision_update(self, req, repository=""):
        """
        @brief   Clones or pulls updates for the git repository 

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
        @brief   Clones or pulls updates for the git repository 
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
    parser = EDDPipeline.getArgumentParser()
    parser.add_argument('--redis-ip', dest='redis_ip', type=str, default="localhost",
                      help='The ip for the redis server')
    parser.add_argument('--redis-port', dest='redis_port', type=int, default=6379,
                      help='The port number for the redis server')

    parser.add_argument('--edd_ansible_repository', dest='edd_ansible_git_repository_folder', type=str, default=os.path.join(os.getenv("HOME"), "edd_ansible"), help='The path to a git repository for the provisioning data')

    parser.add_argument('--edd_ansible_inventory', dest='inventory', type=str,
            default="effelsberg", help='The inventory to use with the ansible setup')
    args = parser.parse_args()
    EDDPipeline.setup_logger(args)

    server = EddMasterController(
        args.host, args.port,
        args.redis_ip, args.redis_port, args.edd_ansible_git_repository_folder, args.inventory)
    EDDPipeline.launchPipelineServer(server, args)
