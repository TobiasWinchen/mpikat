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
from __future__ import print_function, division, unicode_literals

from mpikat.effelsberg.edd.pipeline.EDDPipeline import EDDPipeline, launchPipelineServer, updateConfig, state_change, getArgumentParser, setup_logger
from mpikat.effelsberg.edd.EDDDataStore import EDDDataStore
from mpikat.effelsberg.edd.edd_skarab_client import SkarabChannelizerClient
import mpikat.utils.ip_utils as ip_utils

from tornado.gen import coroutine, sleep, Return
from tornado.ioloop import IOLoop, PeriodicCallback
from katcp import Sensor, FailReply

import logging
import json
import os

log = logging.getLogger("mpikat.effelsberg.edd.pipeline.SkarabPipeline")

DEFAULT_CONFIG = {
        "id": "SkarabPipeline",                          # default name for master controler. Needs to get a unique ID -- TODO, from ansible
        "type": "SkarabPipeline",
        "supported_input_formats": {"MPIFR_EDD_Packetizer": [1]},      # supported input formats name:version
        "input_data_streams":
        {
            "polarization_0" :
            {
                "format": "MPIFR_EDD_Packetizer:1",         # Format has version seperated via colon
                "ip": "225.0.0.140+3",
                "port": "7148",
                "bit_depth" : 12,
            },
             "polarization_1" :
            {
                "format": "MPIFR_EDD_Packetizer:1",
                "ip": "225.0.0.144+3",
                "port": "7148",
                "bit_depth" : 12,
            }
        },
        "output_data_streams":                              # Filled programatically, see below
        {                                                   # The output can be split into an arbitrary sequence of streams. The board streams to the lowest specified stream + 8 groups

        },

        "log_level": "debug",
        "force_program": False,                 # Force reprogramming of with new firmware version
        'skip_device_config': False,            # Skips the skarab config ALWAYS. Overrides force
        "firmware_directory": os.path.join(os.path.dirname(os.path.realpath(__file__)), "skarab_firmware"),
        "firmware": "s_ubb_64ch_coddh_2020-09-21_1540.fpg",
        "channels_per_group": 8,                # Channels per multicast group in the fpga output
        "board_id": 23,                         # Id to add to the spead headers of the FPGA output
        "initial_quantization_factor": 0xff,       # initial value for the quantization factor. Can be changed per measurement
        "initial_fft_shift": 0xff,                 # initial value for the fft shift. Can be changed per measurement

    }

#for i in range(8):
#    ip = "239.0.0.{}".format(120+i)
#    DEFAULT_CONFIG["output_data_streams"]['Output_{}'.format(i)] = {"format": "Skarab:1", "ip": ip, "port": "7152"}
#DEFAULT_CONFIG["output_data_streams"] = { "lower_subband": { "format": "Skarab:1", "ip": "239.0.0.120+3", "port": "7152", "central_freq":None, "sync_time": None, "sample_rate":None, "predecimation_factor": None}, "upper_subband": { "format": "Skarab:1", "ip": "239.0.0.124+3", "port": "7152", "central_freq":None, "sync_time": None, "sample_rate":None, "predecimation_factor": None } }


class SkarabPipeline(EDDPipeline):
    """@brief gated spectrometer pipeline
    """
    VERSION_INFO = ("mpikat-edd-api", 0, 1)
    BUILD_INFO = ("mpikat-edd-implementation", 0, 1, "rc1")

    def __init__(self, ip, port, device_ip, device_port=7147):
        """@brief initialize the pipeline.
           @param device is the control ip of the board
        """
        EDDPipeline.__init__(self, ip, port, DEFAULT_CONFIG)
        log.info('Connecting to skarab @ {}:{}'.format(device_ip, device_port))
        self._client = SkarabChannelizerClient(device_ip, device_port)
        self.__periodic_callback = PeriodicCallback(self._check_fpga_sensors, 1000)
        self.__periodic_callback.start()


    @coroutine
    def _check_fpga_sensors(self):
        log.debug(" Check FPGA Sensors")
        if self._client.is_connected():
            clk = yield self._client.get_fpga_clock()
            self._fpga_clock.set_value(clk)


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


    @coroutine
    def set(self, config_json):
        cfg = yield self._cfgjson2dict(config_json)
        if 'output_data_streams' in cfg:
            log.debug("Stripping outputs from cfg before check")
            # Do not check output data streams, as the only relevant thing is here
            # that they are consecutive
            outputs = cfg.pop('output_data_streams')
            log.debug("Pipeline set")
            yield EDDPipeline.set(self, cfg)
            log.debug("Re-adding outputs")
            self._config['output_data_streams'] = outputs
            self._configUpdated()
        else:
            EDDPipeline.set(self, cfg)



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

        # Convert arbitrary output parts to input list
        iplist = []
        for l in self._config["output_data_streams"].values():
            iplist.extend(ip_utils.ipstring_to_list(l["ip"]))

        output_string = ip_utils.ipstring_from_list(iplist)
        output_ip, Noutput_streams, port = ip_utils.split_ipstring(output_string)

        port = set([l["port"] for l in self._config["output_data_streams"].values()])
        if len(port) != 1:
            raise FailReply("Output data streams have to stream to same port")

        # update sync tim based on input
        for l in self._config["output_data_streams"].values():
            l["sync_time"] = self._config["input_data_streams"]["polarization_0"]["sync_time"]
        self._configUpdated()

        cfs = json.dumps(self._config, indent=4)
        log.info("Final configuration:\n" + cfs)

        if self._config["skip_device_config"]:
            log.warning("Skipping device configuration because debug mode is active!")
            raise Return


        log.debug("Setting firmware string")
        self._client.setFirmware(os.path.join(self._config["firmware_directory"], self._config['firmware']))
        log.debug("Connecting to client")
        self._client.connect()

        if self._config['force_program']:
            log.debug("Forcing reprogramming")
            yield self._client.program()

        yield self._client.initialize()

        yield self._client.configure_inputs(self._config["input_data_streams"]["polarization_0"]["ip"], self._config["input_data_streams"]["polarization_1"]["ip"], int(self._config["input_data_streams"]["polarization_0"]["port"]))

        yield self._client.configure_output(output_ip, int(port.pop()), Noutput_streams, self._config["channels_per_group"], self._config["board_id"] )

        yield   self._client.configure_quantization_factor(self._config["initial_quantization_factor"])
        yield   self._client.configure_fft_shift(self._config["initial_fft_shift"])


    @state_change(target="streaming", allowed=["configured"], intermediate="capture_starting")
    @coroutine
    def capture_start(self, config_json=""):
        """
        @brief start streaming spectrometer output
        """
        log.info("Starting EDD backend")
        yield self._client.capture_start()


    @coroutine
    def measurement_prepare(self, config_json=""):
        """@brief Set quantization factor and fft_shift parameter"""
        cfg = json.loads(config_json)
        if "fft_shift" in cfg:
            yield self._client.configure_fft_shift(cfs["fft_shift"])
        if "quantization_factor" in cfg:
            yield self._client.configure_quantization_factor(cfg["quantization_factor"])


    @state_change(target="idle", allowed=["streaming"], intermediate="capture_stopping")
    @coroutine
    def capture_stop(self):
        """
        @brief Stop streaming of data
        """
        log.info("Stoping EDD backend")
        yield self._client.capture_stop()


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

        descr = {"description":"Channelized complex voltage ouptut.",
                "ip": None,
                "port": None,
                "sample_rate":None,
                "central_freq":None,
                "sync_time": None,
                "predecimation_factor": None
                }
        dataStore.addDataFormatDefinition("Skarab:1", descr)



if __name__ == "__main__":

    parser = getArgumentParser()
    parser.add_argument('--skarab-ip', dest='skarab_ip', type=str, help='The control ip of the skarab board')
    parser.add_argument('--skarab-port', dest='skarab_port', type=int, default=7147, help='The port number to control the skarab board')

    args = parser.parse_args()
    setup_logger(args)

    pipeline = SkarabPipeline(
        args.host, args.port,
        args.skarab_ip, args.skarab_port)

    launchPipelineServer(pipeline, args)
