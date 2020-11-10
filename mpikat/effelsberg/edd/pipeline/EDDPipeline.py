from __future__ import print_function, division, unicode_literals
"""
Copyright (c) 2019 Tobias Winchen <twinchen@mpifr-bonn.mpg.de>

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

log = logging.getLogger("mpikat.effelsberg.edd.pipeline.EDDPipeline")

def updateConfig(oldo, new):
    """
    @brief Merge retrieved config [new] into [old] via recursive dict merge
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
        # Ducktyping
        return d





class StateChange(Exception):
    """
    Special exception that causes a state change to a state other than the defined error states.
    """
    pass



class EDDPipeline(AsyncDeviceServer):
    """
    @brief Abstract interface for EDD Pipelines

    @detail Pipelines can implement functions to act within the following
    sequence of commands with associated state changes:

                                               After provisioning the pipeline is in state idle.
            * ?set "partial config"            After set, it remains in state idle as only the
                                               config dictionary may have changed. A wrong config
                                               is rejected without changing state as state remains
            valid.
            * ?set "partial config"
            * ?set "partial config"
            * ?configure "partial config"      state change from idle to configuring
                                               and configured (on success) or error (on fail)
            * ?capture_start                   state change from configured to streaming or ready
                                               (on success) or error (on fail).
                                               Streaming indicates that no
                                               further changes to the state are
                                               expected and data is injected
                                               into the EDD.
            * ?measurement_prepare "data"      state change from ready to set or error
            * ?measurement_start               state change from set to running or error
            * ?measurement_stop                state change from running to set or error
            * ?measurement_prepare "data"
            * ?measurement_start
            * ?measurement_stop
            * ?measurement_prepare "data"      state change from ready to set or error
            * ?measurement_start               state change from set to running
            * ?measurement_stop                return to state ready
            * ?capture_stop                    return to state configured or idle
            * ?deconfigure                     restore state idle

                                               starting and stopping
                                               may be used as
                                               intermediate states for
                                               capture_start /
                                               capture_stop /
                                               measurement_start,
                                               measurement_stop

    * set - updates the current configuration with the provided partial config. This
            is handled entirely within the parent class which updates the member
            attribute _config.
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
        @brief Initialize the pipeline. Subclasses are required to provide their default config dict and specify
                the data formats definied by the class, if any.
        """
        self.callbacks = set()
        self._state = "idle"
        self.previous_state = "unprovisioned"
        self._sensors = []
        # inject data store dat into all default configs.
        default_config.setdefault("data_store", dict(ip="localhost", port=6379))
        default_config.setdefault("id", "Unspecified")
        default_config.setdefault("type", "Unspecified")
        default_config.setdefault("input_data_streams", [])
        default_config.setdefault("output_data_streams", [])
        default_config["ip"] = ip
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
        @brief The current configuration of the pipeline, i.e. the default
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
        Signals that the config dict has been updated. Seperate method as direct updates of _config items without writing a full dict to _config will noy trigger the _config.setter and have to call this method manually.
        """
        self._edd_config_sensor.set_value(json.dumps(self.__config, indent=4))




    def setup_sensors(self):
        """
        @brief Setup monitoring sensors.

        @detail The EDDPipeline base provides default sensors. Should be called by a subclass.

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
        @brief     Sets the log level

        @return     katcp reply object [[[ !configure ok | (fail [error description]) ]]]
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
        @brief      Returns the name of the controlled pipeline

        @return     katcp reply object
        """

        @coroutine
        def wrapper():
            req.reply(self.__class__.__name__)
        self.ioloop.add_callback(wrapper)
        raise AsyncReply



    @property
    def sensors(self):
        return self._sensors


    def notify(self):
        """@brief callback function."""
        for callback in self.callbacks:
            callback(self._state, self)


    @property
    def state(self):
        """@brief property of the pipeline state."""
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
        @brief    Start the server
        """
        AsyncDeviceServer.start(self)


    def stop(self):
        """
        @brief    Stop the server
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
        log.error("Errror handle called because subprocess {} ended with return code {}".format(proc.pid, proc.returncode))
        self._subprocessMonitor.stop()
        self.state =  "error"


    @request(Str())
    @return_reply()
    def request_configure(self, req, config_json):
        """
        @brief      Configure EDD to receive and process data

        @return     katcp reply object [[[ !configure ok | (fail [error description]) ]]]
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
        """@brief Default method - no effect"""
        pass


    @request()
    @return_reply()
    def request_set_default_config(self, req):
        """
        @brief      Set the current config to the default config

        @return     katcp reply object [[[ !reconfigure ok | (fail [error description]) ]]]
        """

        logging.info("Setting default configuration")
        self._config = self._default_config.copy()
        req.reply("ok")
        raise AsyncReply


    @request(Str())
    @return_reply()
    def request_set(self, req, config_json):
        """
        @brief      Add the config_json to the current config

        @return     katcp reply object [[[ !configure ok | (fail [error description]) ]]]
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
        @brief  Returns the config as dict.
        """
        if isinstance(config_json, str) or isinstance(config_json, unicode):
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
        @brief      Add the config_json to the current config. Input / output data streams will be filled with default values if not provided.

        @return     katcp reply object [[[ !configure ok | (fail [error description]) ]]]
        """

        log.debug("Updating configuration: '{}'".format(config_json))
        cfg = yield self._cfgjson2dict(config_json)
        try:
            newcfg = updateConfig(self._config, cfg)
            yield self.check_config(newcfg)
            self._config = newcfg
            log.debug("Updated config: '{}'".format(self._config))
        except FailReply as E:
            log.error("Check config failed!")
            raise E
        except KeyError as error:
            raise FailReply("Unknown configuration option: {}".format(str(error)))
        except Exception as error:
            raise FailReply("Unknown ERROR: {}".format(str(error)))

    @coroutine
    def check_config(self, cfg):
        """
        Checks a config dictionary for validity. to be implemented in child class. Raise FailReply on invalid setting.
        """
        pass

    @request()
    @return_reply()
    def request_capture_start(self, req):
        """
        @brief      Start the EDD backend processing

        @note       This method may be updated in future to pass a 'scan configuration' containing
                    source and position information necessary for the population of output file
                    headers.

        @note       This is the KATCP wrapper for the capture_start command

        @return     katcp reply object [[[ !capture_start ok | (fail [error description]) ]]]
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
        @brief Set error mode requested by watchdog.
        """
        log.error("Error state requested by watchdog!")
        self.state = "error"


    @coroutine
    def capture_start(self):
        """@brief Default method - no effect"""
        pass


    @request()
    @return_reply()
    def request_capture_stop(self, req):
        """
        @brief      Stop the EDD backend processing

        @note       This is the KATCP wrapper for the capture_stop command

        @return     katcp reply object [[[ !capture_stop ok | (fail [error description]) ]]]
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
        """@brief Default method - no effect"""
        pass


    @request(Str())
    @return_reply()
    def request_measurement_prepare(self, req, config_json):
        """
        @brief      Prepare measurement request

        @return     katcp reply object [[[ !measurement_prepare ok | (fail [error description]) ]]]
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
        """@brief Default method - no effect"""
        pass



    @request()
    @return_reply()
    def request_measurement_start(self, req):
        """
        @brief      Start

        @note       This is the KATCP wrapper for the measurement_start command

        @return     katcp reply object [[[ !measurement_start ok | (fail [error description]) ]]]
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
        """@brief Default method - no effect"""
        pass


    @request()
    @return_reply()
    def request_measurement_stop(self, req):
        """
        @brief      Start

        @note       This is the KATCP wrapper for the measurement_stop command

        @return     katcp reply object [[[ !measurement_start ok | (fail [error description]) ]]]
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
        """@brief Default method - no effect"""
        pass

    @request()
    @return_reply()
    def request_deconfigure(self, req):
        """
        @brief      Deconfigure the pipeline.

        @note       This is the KATCP wrapper for the deconfigure command

        @return     katcp reply object [[[ !deconfigure ok | (fail [error description]) ]]]
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
        """@brief Default method - no effect"""
        pass

    @request(include_msg=True)
    @return_reply()
    def request_register(self, req, msg):
        """
        @brief Register the pipeline in the datastore. Optionally the data store can be specified as "ip:port". If not specified the value in the configuration will be used.

        @return     katcp reply object [[[ !configure ok | (fail [error description]) ]]]
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
        """@brief Populate the data store"""
        if host == None:
            log.debug("No host provided. Use value from current config.")
            host = self.config["data_store"]["ip"]
        if port == None:
            log.debug("No portprovided. Use value from current config.")
            port = self.config["data_store"]["port"]
        log.debug("Register pipeline in data store @ {}:{}".format(host, port))
        dataStore = EDDDataStore.EDDDataStore(host, port)
        dataStore.updateProduct(self._config)





        pass


def state_change(target, allowed=EDDPipeline.PIPELINE_STATES, waitfor=None, abortwaitfor=['deconfiguring', 'error', 'panic'], intermediate=None, error='error', timeout=120):
    """
    @brief decorator to perform a state change in a method

    @param        target: target state
    @param       allowed: Allowed source states
    @param  intermediate: Intermediate state to assume while executing
    @param         error: State to assume on error
    @param       waitfor: Wait with the state changes until the current state set
    @param  abortwaitfor: States that result in aborting the wait (with Failure)
    @param       timeout: If state change is not completed after [timeout] seconds, error state is assumed. Timeout can be None to wait indefinitely
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
    log.info("Shutting down server")
    yield server.stop()
    ioloop.stop()


def getArgumentParser(description = ""):
    """
    @brief Provide a arguemnt parser with standard arguments for all pipelines.
    """
    parser = ArgumentParser(description=description)
    parser.add_argument('-H', '--host', dest='host', type=str, default='localhost',
                      help='Host interface to bind to')
    parser.add_argument('-p', '--port', dest='port', type=int, default=1235,
                      help='Port number to bind to')
    parser.add_argument('--log-level', dest='log_level', type=str,
                      help='Port number of status server instance', default="INFO")

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
    @brief Launch a Pipeline server.

    @param Pipeline Instance or ServerClass to launch.
    @param ArgumentParser args to use for launch.
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

    def start_and_display():
        log.info("Starting Pipeline server")
        server.start()
        log.debug("Started Pipeline server")
        log.info("Ctrl-C to terminate server")

    ioloop.add_callback(start_and_display)
    ioloop.start()
