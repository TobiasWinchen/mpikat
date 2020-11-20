#Copyright (c) 2019 Tobias Winchen <twinchen@mpifr-bonn.mpg.de>
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

from __future__ import print_function, division, unicode_literals
from mpikat.utils.process_tools import ManagedProcess, command_watcher
from mpikat.utils.process_monitor import SubprocessMonitor
from mpikat.utils.sensor_watchdog import SensorWatchdog
from mpikat.utils.db_monitor import DbMonitor
from mpikat.utils.mkrecv_stdout_parser import MkrecvSensors
import mpikat.utils.numa as numa

import mpikat.effelsberg.edd.EDDDataStore as EDDDataStore

from katcp import Sensor, AsyncDeviceServer, AsyncReply, FailReply
from katcp.kattypes import request, return_reply, Int, Str

import tornado
from tornado.gen import coroutine, Return, with_timeout, sleep

import os
import datetime
import logging
import signal
from argparse import ArgumentParser
import coloredlogs
import json
import tempfile
import threading
import types
import functools
import socket

import sys
if sys.version_info[0] >=3:
    unicode_type = str
else:
    unicode_type = unicode


log = logging.getLogger("mpikat.effelsberg.edd.pipeline.EDDPipeline")


def updateConfig(oldo, new):
    """
    Merge retrieved config [new] into [old] via recursive dict merge

    Example::

        >>> old = {'a:': 0, 'b': {'ba':0, 'bb': 0}}
        >>> new = {'a:': 1, 'b': {'ba':0}}

        >>> print(updateConfig(old, new))
        {'a:': 1, 'b': {'ba': 2, 'bb': 0}}

    """
    old = oldo.copy()
    for k in new:
        if isinstance(old[k], dict):
            log.debug("update sub dict for key: {}".format(k))
            old[k] = updateConfig(old[k], new[k])
        else:
            if type(old[k]) != type(new[k]):
                log.warning("Update option {} with different type! Old value(type) {}({}), new {}({}) ".format(k, old[k], type(old[k]), new[k], type(new[k])))
            old[k] = new[k]
    return old


def value_list(d):
    
    if isinstance(d, dict):
        return d.values()
    else:
        return d





class StateChange(Exception):
    """
    Special exception that causes a state change to a state other than the
    defined error states.
    """
    pass



class EDDPipeline(AsyncDeviceServer):
    """
    Abstract interface for EDD Pipelines

    Pipelines can implement functions to act within the following
    sequence of commands with associated state changes. After provisioning the
    pipeline is in state idle.

        ?set "partial config"
            Updates the current configuration with the provided partial config.
            After set, it remains in state idle as only the config dictionary
            may have changed. A wrong config is rejected without changing state
            as state remains valid. Multiple set commands can bes end to the
            pipeline.
        ?configure "partial config"
            state change from idle to configuring and configured (on success)
            or error (on fail)
        ?capture_start
            state change from configured to streaming or ready (on success) or
            error (on fail).  Streaming indicates that no further changes to
            the state are expected and data is injected into the EDD.
        ?measurement_prepare "data"
            state change from ready to set or error
        ?measurement_start
            state change from set to running or error
        ?measurement_stop
            state change from running to set or error
        ?capture_stop
            return to state configured or idle
        ?deconfigure
            restore state idle

    * configure - optionally does a final update of the current config and
                  prepares the pipeline. Configuring the pipeline may take time,
                  so all lengthy preparations should be done here.
    * capture start - The pipeline should send data (into the EDD) after this command.
    * measurement prepare - receive optional configuration before each measurement.
                            The pipeline must not stop streaming on update.
    * measurement start - Start of an individual measurement. Should be quasi
                          instantaneous. E.g. a recorder should be already connected
                          to the data stream and just start writing to disk.
    * measurement stop -  Stop the measurement

    Pipelines can also implement:
        * populate_data_store to send data to the store. The address and port for a data store is received along the request.
    """
    DEVICE_STATUSES = ["ok", "degraded", "fail"]


    PIPELINE_STATES = ["idle", "configuring", "configured",
            "capture_starting", "streaming", "ready",
            "measurement_preparing", "set",
            "measurement_starting", "measuring", "running",
            "measurement_stopping",
            "capture_stopping", "deconfiguring", "error", "panic"]

    def __init__(self, ip, port, default_config={}):
        """
        Initialize the pipeline. Subclasses are required to provide their
        default config dict and specify the data formats definied by the class,
        if any.

        Args:
            ip:             ip to accept connections from
            port:           port to listen
            default_config: default config of the pipeline
        """
        self._state = "idle"
        self.previous_state = "unprovisioned"
        self._sensors = []
        # inject data store dat into all default configs.
        default_config.setdefault("data_store", dict(ip="localhost", port=6379))
        default_config.setdefault("id", "Unspecified")
        default_config.setdefault("type", self.__class__.__name__)
        default_config.setdefault("input_data_streams", [])
        default_config.setdefault("output_data_streams", [])

        default_config["ip"] = socket.gethostname()
        default_config["port"] = port

        for stream in value_list(default_config['input_data_streams']):
            stream.setdefault("source", "")
            if not stream['format']:
                log.warning("Input stream without format definition!")
                continue
            for key, value in EDDDataStore.data_formats[stream['format']].items():
                stream.setdefault(key, value)
        for stream in value_list(default_config['output_data_streams']):
            if not stream['format']:
                log.warning("Output stream without format definition!")
                continue
            for key, value in EDDDataStore.data_formats[stream['format']].items():
                stream.setdefault(key, value)

        self.__config = default_config.copy()
        self._default_config = default_config
        self._subprocesses = []
        self._subprocessMonitor = None
        AsyncDeviceServer.__init__(self, ip, port)

        #update the docstrings for the requests by their subclass implementation
        for r,s in [(self.request_configure, self.configure),
                (self.request_set, self.set),
                (self.request_capture_start, self.capture_start),
                (self.request_capture_stop, self.capture_stop),
                (self.request_measurement_start, self.measurement_start),
                (self.request_measurement_stop, self.measurement_stop)]:
            r.__func__.__doc__ = s.__doc__


    @property
    def _config(self):
        """
        The current configuration of the pipeline, i.e. the default
        after all updates received via set and configure commands. This value
        should then be used in the _configure method.
        """
        return self.__config


    @_config.setter
    def _config(self, value):
        if not isinstance(value, dict):
            raise RuntimeError("_config has to be a dict!")

        if value == self.__config:
            log.debug("No changes in config, not updating sensor")
        else:
            self.__config = value
            self._configUpdated()


    def _configUpdated(self):
        """
        Signals that the config dict has been updated. Seperate method as
        direct updates of _config items without writing a full dict to _config
        will noy trigger the _config.setter and have to call this method
        manually.
        """
        self._edd_config_sensor.set_value(json.dumps(self.__config, indent=4))




    def setup_sensors(self):
        """
        Setup monitoring sensors.

        The EDDPipeline base provides default sensors. Should be called by a subclass.

        """
        self._pipeline_sensor_status = Sensor.discrete(
            "pipeline-status",
            description="Status of the pipeline",
            params=self.PIPELINE_STATES,
            default="idle",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._pipeline_sensor_status)

        self._edd_config_sensor = Sensor.string(
            "current-config",
            description="The current configuration for the EDD backend",
            default=json.dumps(self._config, indent=4),
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._edd_config_sensor)

        self._log_level = Sensor.string(
            "log-level",
            description="Log level",
            default=logging.getLevelName(log.level),
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._log_level)


    @request(Str())
    @return_reply()
    def request_set_log_level(self, req, level):
        """
        Sets the log level

        Return:
            katcp reply object [[[ !configure ok | (fail [error description]) ]]]
        """
        @coroutine
        def wrapper():
            try:
                log.info("Setting log level to: {}".format(level.upper()))
                logger = logging.getLogger('mpikat')
                logger.setLevel(level.upper())
                self._log_level.set_value(level.upper())
                log.debug("Successfully set log-level")
            except FailReply as fr:
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
    def request_whoami(self, req):
        """
        Returns the name of the controlled pipeline

        Return:
            katcp reply object
        """

        @coroutine
        def wrapper():
            req.reply(self.__class__.__name__)
#            req.reply("".format(self._config['type'], self._config['id']))
        self.ioloop.add_callback(wrapper)
        raise AsyncReply



    @property
    def sensors(self):
        return self._sensors


    @property
    def state(self):
        """
        State of the pipeline.
        """
        return self._state


    @state.setter
    def state(self, value):
        log.info("Changing state: {} -> {}".format(self.previous_state, value))
        self.previous_state = self._state
        self._state = value
        self._pipeline_sensor_status.set_value(self._state)
        self.notify()


    def start(self):
        """
        Start the server
        """
        AsyncDeviceServer.start(self)


    def stop(self):
        """
        Stop the server
        """
        AsyncDeviceServer.stop(self)


    def _decode_capture_stdout(self, stdout, callback):
        """
        @ToDo: Potentially obsolete ??
        """
        log.debug('{}'.format(str(stdout)))


    def _handle_execution_stderr(self, stderr, callback):
        """
        @ToDo: Potentially obsolete ??
        """
        log.info(stderr)
        log.debug('{}'.format(str(stderr)))


    def _subprocess_error(self, proc):
        """
        Sets the error state because proc has ended.
        """
        pass
        log.error("Errror handle called because subprocess {} ended with return code {}".format(proc.pid, proc.returncode))
        self._subprocessMonitor.stop()
        self.state =  "error"


    @request(Str())
    @return_reply()
    def request_configure(self, req, config_json):
        """
        Configure EDD to receive and process data

        Returns:
            katcp reply object [[[ !configure ok | (fail [error description]) ]]]
        """
        @coroutine
        def configure_wrapper():
            try:
                yield self.configure(config_json)
            except FailReply as fr:
                req.reply("fail", str(fr))
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(configure_wrapper)
        raise AsyncReply


    @coroutine
    def configure(self, config_json=""):
        """
        Default method for configuration.
        """
        pass


    @request()
    @return_reply()
    def request_set_default_config(self, req):
        """
        (Re-)set the config to the default.

        Returns:
            katcp reply object [[[ !reconfigure ok | (fail [error description]) ]]]
        """

        logging.info("Setting default configuration")
        self._config = self._default_config.copy()
        req.reply("ok")
        raise AsyncReply


    @request(Str())
    @return_reply()
    def request_set(self, req, config_json):
        """
        Add the config_json to the current config

        Returns:
            katcp reply object [[[ !configure ok | (fail [error description]) ]]]
        """

        @coroutine
        def wrapper():
            try:
                yield self.set(config_json)
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
    def _cfgjson2dict(self, config_json):
        """
        Returns the provided config as dict if a json object or returns the object if it already is a dict.
        """
        if isinstance(config_json, str) or isinstance(config_json, unicode_type):
            log.debug("Received config as string:\n  {}".format(config_json))
            if (not config_json.strip()) or config_json.strip() == '""':
                log.debug("String empty, returning empty dict.")
                raise Return({})
            try:
                cfg = json.loads(config_json)
            except:
                log.error("Error parsing json")
                raise FailReply("Cannot handle config string {} - Not valid json!".format(config_json))
        elif isinstance(config_json, dict):
            log.debug("Received config as dict")
            cfg = config_json
        else:
            raise FailReply("Cannot handle config type {}. Config has to bei either json formatted string or dict!".format(type(config_json)))
        log.debug("Got cfg: {}, {}".format(cfg, type(cfg)))
        raise Return(cfg)


    @coroutine
    def set(self, config_json):
        """
        Add the config_json to the current config. Input / output data streams
        will be filled with default values if not provided.

        The configuration will be rejected if no corresponding value is present
        in the default config. A warnign is emitted on type changes.

        The final configuration is stored in self._config for access in derived classes.

        Returns:
            katcp reply object [[[ !configure ok | (fail [error description]) ]]]
        """

        log.debug("Updating configuration: '{}'".format(config_json))
        cfg = yield self._cfgjson2dict(config_json)
        try:
            newcfg = updateConfig(self._config, cfg)
#            yield self.check_config(newcfg)
            self._config = newcfg
            log.debug("Updated config: '{}'".format(self._config))
        except FailReply as E:
            log.error("Check config failed!")
            raise E
        except KeyError as error:
            raise FailReply("Unknown configuration option: {}".format(str(error)))
        except Exception as error:
            raise FailReply("Unknown ERROR: {}".format(str(error)))

#    @coroutine
#    def check_config(self, cfg):
#        """
#        Checks a config dictionary for validity. to be implemented in child class. Raise FailReply on invalid setting.
#        """
#        pass

    @request()
    @return_reply()
    def request_capture_start(self, req):
        """
        Start the EDD backend processing

        Note:
            This is the KATCP wrapper for the capture_start command

        Returns:
            katcp reply object [[[ !capture_start ok | (fail [error description]) ]]]
        """

        @coroutine
        def start_wrapper():
            try:
                yield self.capture_start()
            except FailReply as fr:
                log.error(str(fr))
                req.reply("fail", str(fr))
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(start_wrapper)
        raise AsyncReply


    @coroutine
    def request_halt(self, req, msg):
        """
        Halts the process. Reimplemnetation of base class halt without timeout as this crash
        """
        if self.state == "running":
            yield self.capture_stop()
        yield self.deconfigure()
        self.ioloop.stop()
        req.reply("Server has stopepd - ByeBye!")
        raise AsyncReply


    def watchdog_error(self):
        """
        Set error mode requested by watchdog.
        """
        log.error("Error state requested by watchdog!")
        self.state = "error"


    @coroutine
    def capture_start(self):
        """
        Default method - no effect
        """
        pass


    @request()
    @return_reply()
    def request_capture_stop(self, req):
        """
        Stop the EDD backend processing

        Note:
            This is the KATCP wrapper for the capture_stop command

        Return:
            katcp reply object [[[ !capture_stop ok | (fail [error description]) ]]]
        """

        @coroutine
        def stop_wrapper():
            try:
                yield self.capture_stop()
            except FailReply as fr:
                log.error(str(fr))
                req.reply("fail", str(fr))
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(stop_wrapper)
        raise AsyncReply


    @coroutine
    def capture_stop(self):
        """Default method - no effect"""
        pass


    @request(Str())
    @return_reply()
    def request_measurement_prepare(self, req, config_json):
        """
        Prepare measurement request

        Return:
            katcp reply object [[[ !measurement_prepare ok | (fail [error description]) ]]]
        """

        @coroutine
        def wrapper():
            try:
                yield self.measurement_prepare(config_json)
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
    def measurement_prepare(self, config_json=""):
        """Default method - no effect"""
        pass



    @request()
    @return_reply()
    def request_measurement_start(self, req):
        """
        Start emasurement.

        Note:
            This is the KATCP wrapper for the measurement_start command

        Return:
            katcp reply object [[[ !measurement_start ok | (fail [error description]) ]]]
        """

        @coroutine
        def wrapper():
            try:
                yield self.measurement_start()
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
    def measurement_start(self):
        """Default method - no effect"""
        pass


    @request()
    @return_reply()
    def request_measurement_stop(self, req):
        """
        Stop  measurement

        Note:
            This is the KATCP wrapper for the measurement_stop command

        Return:
            katcp reply object [[[ !measurement_start ok | (fail [error description]) ]]]
        """

        @coroutine
        def wrapper():
            try:
                yield self.measurement_stop()
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
    def measurement_stop(self):
        """Default method - no effect"""
        pass

    @request()
    @return_reply()
    def request_deconfigure(self, req):
        """
        Deconfigure the pipeline.

        Note:
            This is the KATCP wrapper for the deconfigure command

        Return:
            katcp reply object [[[ !deconfigure ok | (fail [error description]) ]]]
        """

        @coroutine
        def deconfigure_wrapper():
            try:
                yield self.deconfigure()
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(deconfigure_wrapper)
        raise AsyncReply


    @coroutine
    def deconfigure(self):
        """Default method - no effect"""
        pass

    @request(include_msg=True)
    @return_reply()
    def request_register(self, req, msg):
        """
        Register the pipeline in the datastore. Optionally the data store can be specified as "ip:port". If not specified the value in the configuration will be used.

        katcp reply object [[[ !configure ok | (fail [error description]) ]]]
        """
        log.debug("regsiter request")
        @coroutine
        def wrapper():
            try:
                if msg.arguments:
                    host, port = msg.argument.split(':')
                    port = int(port)
                else:
                    host = self._config['data_store']['ip']
                    port = self._config['data_store']['port']

                yield self.register(host, port)
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
    def register(self, host=None, port=None):
        """
        Registers the pipeline in the data store.

        Args:
            host, port: Ip and port of the data store.

        If no host and port ar eprovided, values from the internal config are used.
        """
        if host == None:
            log.debug("No host provided. Use value from current config.")
            host = self.config["data_store"]["ip"]
        if port == None:
            log.debug("No portprovided. Use value from current config.")
            port = self.config["data_store"]["port"]
        log.debug("Register pipeline in data store @ {}:{}".format(host, port))
        dataStore = EDDDataStore.EDDDataStore(host, port)
        dataStore.updateProduct(self._config)




def state_change(target, allowed=EDDPipeline.PIPELINE_STATES, waitfor=None, abortwaitfor=['deconfiguring', 'error', 'panic'], intermediate=None, error='error', timeout=120):
    """
    Decorator to perform a state change in a method.

    Args:
       target (str):          Target state.
       allowed (list):        Allowed source states.
       intermediate (str):    Intermediate state assumed while executing.
       error (str):           State assumed if an exception reached the decorator.
       waitfor (str):         Wait with the state changes until the current state set.
       abortwaitfor (list):   States that result in aborting the wait (with Failure).
       timeout (int):         If state change is not completed after [timeout] seconds, error state is assumed. Timeout can be None to wait indefinitely.

    Example:

       State transition from idle->configure (or error) via configuring.::

         @state_change(target="configured", allowed=["idle"], intermediate="configuring")
         @coroutine
         def configure(self, config_json):
             pass


    Note:
        If more than one valid target or error state is possible, the final
        state has to be indicated by throwing a StateChange exception.
    """
    def decorator_state_change(func):
        @functools.wraps(func)
        @coroutine
        def wrapper(self, *args, **kwargs):
            log.debug("Decorator managed state change {} -> {}".format(self.state, target))
            if self.state not in allowed:
                log.warning("State change to {} requested, but state {} not in allowed states! Doing nothing.".format(target, self.state))
                return
            if waitfor:
                waiting_since = 0
                while (self.state != waitfor):
                    log.warning("Waiting since {}s for state {} to start state change to {}, current state {}. Timeout: {}s".format(waiting_since, waitfor, target, self.state, timeout))
                    if waiting_since > timeout:
                        raise RuntimeError("Waiting since {}s to assume state {} in preparation to change to {}. Aborting.".format(waiting_since, waitfor, target))
                    yield sleep(1)
                    waiting_since += 1

                    if self.state in abortwaitfor:
                        raise FailReply("Aborting waiting for state: {} due to state: {}".format(waitfor, self.state))

            if intermediate:
                self.state = intermediate
            try:
                if timeout:
                    yield with_timeout(datetime.timedelta(seconds=timeout), func(self, *args, **kwargs))
                else:
                    yield func(self, *args, **kwargs)
            except StateChange as E:
                self.state = str(E)
            except Exception as E:
                self.state = error
                raise E
            else:
                self.state = target
        return wrapper
    return decorator_state_change






@coroutine
def on_shutdown(ioloop, server):
    """
    Shut down the server and stop the ioloop.
    """
    log.info("Shutting down server")
    yield server.stop()
    ioloop.stop()


def getArgumentParser(description = "", include_register_command=True):
    """
    Creates an argument parser with standard arguments for all pipelines. By
    this all pipeliens have a standard set of commandline options for pipelien
    start and stop.

    Returns:
        ArgumentParser
    """
    parser = ArgumentParser(description=description)
    parser.add_argument('-H', '--host', dest='host', type=str, default='localhost',
                      help='Host interface to bind to')
    parser.add_argument('-p', '--port', dest='port', type=int, default=1235,
                      help='Port number to bind to')
    parser.add_argument('--log-level', dest='log_level', type=str,
                      help='Port number of status server instance', default="INFO")
    parser.add_argument('--register-id', dest='register_id', type=str,
                      help='The default pipeline to datastore.')
    parser.add_argument('--redis-ip', dest='redis_ip', type=str, default="localhost",
                      help='The ip for the redis server')
    parser.add_argument('--redis-port', dest='redis_port', type=int, default=6379,
                      help='The port number for the redis server')
    return parser


def setup_logger(args):
    """
    Setup log level from value provided py argument parser
    """
    logging.getLogger().addHandler(logging.NullHandler())
    logger = logging.getLogger('mpikat')
    coloredlogs.install(
        fmt=("[ %(levelname)s - %(asctime)s - %(name)s "
             "- %(filename)s:%(lineno)s] %(message)s"),
        level=1,            # We manage the log lvel via the logger, not the handler
        logger=logger)
    logger.setLevel(args.log_level.upper())



def launchPipelineServer(Pipeline, args=None):
    """
    Launch a pipeline server and install signal-handler to kill server on Ctrl-C.

    Args:
        Pipeline: Instance of a pipeline or ServerClass definition to launch.
        args: ArgumentParser args to use for launch of the pipeline.

    Example:
        Start a pipeline using a class definition::

            class MyPipeline(EDDPipeline):
                pass

            launchPipelineServer(MyPipeline)


        Starts a pipeline using an instance::

            class MyPipeline(EDDPipeline):
                pass

            server = MyPipeline()

            launchPipelineServer(server)




    """
    if not args:
        parser = getArgumentParser()
        args = parser.parse_args()

    setup_logger(args)

    if (type(Pipeline) == types.ClassType) or isinstance(Pipeline, type):
        log.info("Created Pipeline instance")
        server = Pipeline(
            args.host, args.port
            )
    else:
        server = Pipeline

    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting Pipeline instance")
    log.info("Accepting connections from: {}:{}".format(args.host, args.port))
    signal.signal(
        signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
            on_shutdown, ioloop, server))

    if args.register_id:
        server.set({"id": args.register_id})
        server.register(args.redis_ip, args.redis_port)

    def start_and_display():
        log.info("Starting Pipeline server")
        server.start()
        log.debug("Started Pipeline server")
        log.info("Ctrl-C to terminate server")

    ioloop.add_callback(start_and_display)
    ioloop.start()
