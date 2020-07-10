"""
Copyright (c) 2020 Tobias Winchen <twinchen@mpifr-bonn.mpg.de>

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
from mpikat.effelsberg.edd.pipeline.EDDPipeline import EDDPipeline, launchPipelineServer, updateConfig, state_change
from mpikat.effelsberg.edd.EDDDataStore import EDDDataStore
import mpikat.utils.numa as numa

from tornado.gen import coroutine, sleep
from katcp import Sensor, AsyncReply, FailReply

import os
import time
import logging
import signal
from optparse import OptionParser
import coloredlogs
import json
import tempfile

log = logging.getLogger("mpikat.effelsberg.edd.pipeline.SkarabPipeline")
log.setLevel('DEBUG')

DEFAULT_CONFIG = {
        "id": "SkarabPipeline",                          # default cfgs for master controler. Needs to get a unique ID -- TODO, from ansible
        "type": "SkarabPipeline",
        "supported_input_formats": {"MPIFR_EDD_Packetizer": [1]},      # supproted input formats name:version
        "input_data_streams":
        {
            "polarization_0" :
            {
                "source": "",                               # name of the source for automatic setting of paramters
                "description": "",
                "format": "MPIFR_EDD_Packetizer:1",         # Format has version seperated via colon
                "ip": "225.0.0.152+3",
                "port": "7148",
                "bit_depth" : 12,
                "sample_rate" : 2600000000,
                "sync_time" : 1581164788.0,
                "samples_per_heap": 4096,                     # this needs to be consistent with the mkrecv configuration
            },
             "polarization_1" :
            {
                "source": "",                               # name of the source for automatic setting of paramters, e.g.: "packetizer1:h_polarization
                "description": "",
                "format": "MPIFR_EDD_Packetizer:1",
                "ip": "225.0.0.156+3",
                "port": "7148",
                "bit_depth" : 12,
                "sample_rate" : 2600000000,
                "sync_time" : 1581164788.0,
                "samples_per_heap": 4096,                           # this needs to be consistent with the mkrecv configuration
            }
        },
        "output_data_streams":
        {
            "Output1" :
            {
                "format": "Skarab:1",
                "ip": "225.0.0.172",
                "port": "7152",
            },
            "Output2" :
            {
                "format": "Skarab:1",
                "ip": "225.0.0.173",
                "port": "7152",
            }        },

        "log_level": "debug",
        "firmware": "s_ubb_64ch_codd_2020-06-30_1403.fpg"
    }

NON_EXPERT_KEYS = []



class SkarabPipeline(EDDPipeline):
    """@brief gated spectrometer pipeline
    """
    VERSION_INFO = ("mpikat-edd-api", 0, 1)
    BUILD_INFO = ("mpikat-edd-implementation", 0, 1, "rc1")

    def __init__(self, ip, port, device):
        """@brief initialize the pipeline.
           @param device is the control ip of the board
        """
        EDDPipeline.__init__(self, ip, port, DEFAULT_CONFIG)
        log.info('Connecting to skarab @ {}'.format(device))
        try:
            self._client = casperfpga.CasperFpga(device, logger=log)
        except Exception as E:
            log.error('Cannot connect')
            log.exception(E)
            raise E
        log.info('Connected to: ' + json.dumps(self._client.transport.get_skarab_version_info(), indent=4)[2:-2]

        IOLoop.current().spawn_callback(self.periodic_check_fpga_clock)

    @coroutine
    def periodic_check_fpga_clock(self):
        while True:
            yield clk = self._client.estimate_fpga_clock()
            self._fpga_clock.set_value(clk)
            yield sleep(1)

            

    def setup_sensors(self):
        """
        @brief Setup monitoring sensors
        """
        EDDPipeline.setup_sensors(self)
        self._fpga_clock = Sensor.float(
            "fpga-clock",
            description="FPGA Clock estimate",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._fpga_clock)



    @state_change(target="configured", allowed=["idle"], intermediate="configuring")
    @coroutine
    def configure(self, config_json):
        """
        @brief   Configure the Skarab PFb Pipeline 

        @param   config_json    A JSON dictionary object containing configuration information

        @detail  The configuration dictionary is highly flexible - settings relevant for non experts are:
                 @code
                     {
                     }
                 @endcode
        """
        log.info("Configuring EDD backend for processing")
        log.debug("Configuration string: '{}'".format(config_json))

        yield self.set(config_json)

        cfs = json.dumps(self._config, indent=4)
        log.info("Final configuration:\n" + cfs)


    @state_change(target="streaming", allowed=["configured"], intermediate="capture_starting")
    @coroutine
    def capture_start(self, config_json=""):
        """
        @brief start streaming spectrometer output
        """
        log.info("Starting EDD backend")


    @state_change(target="idle", allowed=["streaming"], intermediate="capture_stopping")
    @coroutine
    def capture_stop(self):
        """
        @brief Stop streaming of data
        """
        log.info("Stoping EDD backend")


    @state_change(target="idle", intermediate="deconfiguring", error='panic')
    @coroutine
    def deconfigure(self):
        """
        @brief deconfigure the gated spectrometer pipeline.
        """
        log.info("Deconfiguring EDD backend")

    @coroutine
    def populate_data_store(self, host, port):
        """@brief Populate the data store"""
        log.debug("Populate data store @ {}:{}".format(host, port))
        dataStore =  EDDDataStore(host, port)
        log.debug("Adding output formats to known data formats")

        descr = {"description":"Self descriped spead stream of sepctrometer data with noise diode on/off",
                "ip": None,
                "port": None,
                }
        dataStore.addDataFormatDefinition("Skarab:1", descr)



if __name__ == "__main__":
    launchPipelineServer(SkarabPipeline)
