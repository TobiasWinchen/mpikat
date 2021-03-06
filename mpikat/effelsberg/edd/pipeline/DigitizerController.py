#Copyright (c) 2020 Tobias Winchen <twinchen@mpifr-bonn.mpg.de>
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
The DigitizerController pipeline controls MPIfR digitizer/packetizer via KATCP.

Consecutive configure commands are send to the packetizer only if the
parameters have changed. This behavior can be controlled using the
**force_reconfigure** and **max_sync_age** options.

Note:
    The digitizer state is not queried, but only previous configs received by
    the pipeline are remembered. Thus, the first  configuration is always send,
    and interacting with the packetizer by other tools in parallel will result
    in possibly wrong configuration options.


Configuration Settings
----------------------

 bit_depth (int)
    Number of bits of information in each sample. Possible values are 8 and 12.

 sampling_rate
    Sampling rate of the packetizer in Hz. Note that the effective sampling
    rate also depends on the **predecimation_factor**.

 predecimation_factor (int)
    Only 1 out of **predecimation_factor** samples is used by the ADC. A
    sampling_rate setting of 2600000000 and a **predecimation_factor** of 2
    corresponds to an effective sampling_rate of 1.3 GHz. Possible values are:
    1,2,4,8.

 noise_diode_pattern (float, float) (percentage [0-1],  )
    Noise diode i
    Frequency of the noise diode. The noise diode is 50% on and 50% off with a
    cycle duration of 2x **noise_diode_frequency**.  A negative value indicates
    always off. A value of 0 indicates always on.

 flip_spectrum (bool)
    Flips the spectrum, e.g. to correct for Nyquist zone.


Output Data Streams
-------------------
    polarization_0, polarization_1
        One output data stream per polarization in MPIFR_EDD_Packetizer format.
        polarization_0 maps to capture_destination "v" of the packetizer,
        polarization_1 maps to capture_destination "h" of the packetizer.


Expert/Debugging Settings
-------------------------
 sync_time (int)
    Provide time stamp (Unix time) at which the packetizer will be synced. The
    timestamp has to be in the future.  If the value is 0 (default), the
    current time is used.

 force_reconfigure (bool)
    Force a reconfiguration even if parameters are the same.

 max_sync_age (int)
    Maximum time since the last synchronisation [s] before a packetizer is
    reconfigured even if no settings have changed.  Default: 82800 s

 skip_packetizer_config (bool)
    If true, ALWAYS skips the packetizer configuration - Overrides force and
    max sync age. Useful for pipeline development.

 dummy_configure (bool)
    Update output streams with dummy configuration. Useful for provision
    development.

"""

from __future__ import print_function, division, unicode_literals

from mpikat.effelsberg.edd.pipeline.EDDPipeline import EDDPipeline, launchPipelineServer, updateConfig, state_change, getArgumentParser, setup_logger
from mpikat.effelsberg.edd.EDDDataStore import EDDDataStore
from mpikat.effelsberg.edd.edd_digpack_client import DigitiserPacketiserClient
import mpikat.utils.ip_utils as ip_utils

from tornado.gen import coroutine, sleep, Return
from tornado.ioloop import IOLoop, PeriodicCallback
from katcp import Sensor, FailReply

import time
import logging
import json
import os

log = logging.getLogger("mpikat.effelsberg.edd.pipeline DigitizerController")

_DEFAULT_CONFIG = {
        "id": "DigitizerController",
        "type": "DigitizerController",

        "bit_depth" : 12,
        "sampling_rate" : 2600000000,
        "predecimation_factor" : 1,
        "flip_spectrum": False,
        'sync_time': 0,
        'noise_diode_pattern': {"percentage":0, "period": 0},                  # Note: noise diode pattern can also be set in measurement prepare
        'force_reconfigure': False,
        'skip_packetizer_config': False,
        'dummy_configure': False,
        'max_sync_age': 82800,
        'interface_addresses': [],                               # if set, the digitizer nics are assigned an ip manually during configure
        "output_data_streams":
        {
            "polarization_0" :                          # polarization_0 maps to v in packetizer nomenclatura
            {                                           # Dual info on polarization, as stream nameing is arbitrary string, but pol field carries the nfromaton
                "format": "MPIFR_EDD_Packetizer:1",
                "ip": "225.0.0.140+3",
                "port": "7148",
                "polarization": 0,
            },
             "polarization_1" :
            {
                "format": "MPIFR_EDD_Packetizer:1",
                "ip": "225.0.0.144+3",
                "port": "7148",
                "polarization": 1,
            }
        }
    }


class DigitizerControllerPipeline(EDDPipeline):
    """@brief gated spectrometer pipeline
    """
    VERSION_INFO = ("mpikat-edd-api", 0, 1)
    BUILD_INFO = ("mpikat-edd-implementation", 0, 1, "rc1")

    def __init__(self, ip, port, device_ip, device_port=7147):
        """@brief initialize the pipeline.
           @param device is the control ip of the board
        """
        EDDPipeline.__init__(self, ip, port, _DEFAULT_CONFIG)
        log.info('Connecting to packetizer @ {}:{}'.format(device_ip, device_port))
        self._client = DigitiserPacketiserClient(device_ip, device_port)

        # We do not know the initial state of the packetizr before we take
        # control, thus we will config on first try
        self.__previous_config = None


    def setup_sensors(self):
        """
        @brief Setup monitoring sensors
        """
        EDDPipeline.setup_sensors(self)

#    def check_config(self, cfg):
#        errors = []
#        if not ip_utils.is_valid_multicast_range(*ip_utils.split_ipstring(cfg["output_data_streams"]["polarization_0"]["ip"])):
#            errors.append("Ip strings {} nvalid.\n".format(cfg["output_data_streams"]["polarization_0"]["ip"]) + "\n".join(ip_utils.is_valid_multicast_range.__doc__.split('\n')[1:]))
#        if not ip_utils.is_valid_multicast_range(*ip_utils.split_ipstring(cfg["output_data_streams"]["polarization_1"]["ip"])):
#            errors.append("Ip strings {} nvalid.\n".format(cfg["output_data_streams"]["polarization_0"]["ip"]) + "\n".join(ip_utils.is_valid_multicast_range.__doc__.split('\n')[1:]))
#        if not cfg["bit_depth"] in [8, 10, 12]:
#            errors.append("Unsupported bit-depth.")
#        if not cfg["sampling_rate"] in self._client._sampling_modes:
#            errors.append("Invalid sampling mode")
#
#        if errors:
#            raise FailReply("\n * ".join(["Errors in configuration:"] + errors))


    @state_change(target="configured", allowed=["idle"], intermediate="configuring")
    @coroutine
    def configure(self, config_json):
        """
        @brief   Configure the Packetizer
        """

        if self._config["dummy_configure"]:
            log.warning("DUMMY CONFIGURE ENABELD!")
            for pol in ["polarization_0", "polarization_1"]:
                self._config["output_data_streams"][pol]["sync_time"] = 23
                self._config["output_data_streams"][pol]["bit_depth"] = self._config["bit_depth"]
                self._config["output_data_streams"][pol]["sample_rate"] = self._config["sampling_rate"] / self._config['predecimation_factor']
            self._configUpdated()
            raise Return

        # Do not use configure from packetizer client, as we know the previous
        # config and may thus send this only once.
        log.info("Configuring packetizer")
        log.debug("Configuration string: '{}'".format(config_json))
        yield self.set(config_json)

        sync_age = time.time() - (yield self._client.get_sync_time())

        if self._config['force_reconfigure'] or self.__previous_config != self._config or sync_age > self._config["max_sync_age"]:
            log.debug("Reconfiguring packetizer - Config changed: {}; Sync_age : {}; Forced: {}".format(self.__previous_config != self._config, sync_age, self._config['force_reconfigure']))


            vips = "{}:{}".format(self._config["output_data_streams"]["polarization_0"]["ip"], self._config["output_data_streams"]["polarization_0"]["port"])
            hips = "{}:{}".format(self._config["output_data_streams"]["polarization_1"]["ip"], self._config["output_data_streams"]["polarization_1"]["port"])

            if self._config["skip_packetizer_config"]:
                log.warning('Packetizer configuration manually skipped')
            else:
                yield self._client.capture_stop()
                yield self._client.set_sampling_rate(self._config["sampling_rate"])
                yield self._client.set_predecimation(self._config["predecimation_factor"])

                yield self._client.flip_spectrum(self._config["flip_spectrum"])
                yield self._client.set_bit_width(self._config["bit_depth"])

                for i, ip_address in enumerate(self._config["interface_addresses"]):
                    yield self._client.set_interface_address(i, ip_address)
                yield self._client.set_destinations(vips, hips)
                if self._config["sync_time"] > 0:
                    yield self._client.synchronize(self._config["sync_time"])
                else:
                    yield self._client.synchronize()

            log.debug("Update output data streams")
            sync_time = yield self._client.get_sync_time()

            for pol in ["polarization_0", "polarization_1"]:
                self._config["output_data_streams"][pol]["sync_time"] = sync_time
                self._config["output_data_streams"][pol]["bit_depth"] = self._config["bit_depth"]
                self._config["output_data_streams"][pol]["sample_rate"] = self._config["sampling_rate"] / self._config['predecimation_factor']
            self._configUpdated()
            self.__previous_config = self._config
        else:
            log.debug("Configuration of packetizer skipped as not changed.")

        yield self._client.set_noise_diode_firing_pattern(**self._config["noise_diode_pattern"])



    @state_change(target="streaming", allowed=["configured"], intermediate="capture_starting")
    @coroutine
    def capture_start(self, config_json=""):
        """
        @brief start streaming spectrometer output
        """
        log.info("Starting streaming")
        yield self._client.capture_start()


    @coroutine
    def measurement_prepare(self, config_json=""):
        """@brief Set quantization factor and fft_shift parameter"""
        cfg = json.loads(config_json)
        yield self._client.measurement_prepare(cfg)



    @state_change(target="idle", allowed=["streaming"], intermediate="capture_stopping")
    @coroutine
    def capture_stop(self):
        """
        @brief Stop streaming of data
        """
        yield self._client.capture_stop()


    @state_change(target="idle", intermediate="deconfiguring", error='panic')
    @coroutine
    def deconfigure(self):
        """
        @brief deconfigure the gated spectrometer pipeline.
        """
        log.info("Deconfiguring EDD backend")
        yield self._client.capture_stop()


#    @coroutine
#    def populate_data_store(self, host, port):
#        """@brief Populate the data store"""
#        log.debug("Populate data store @ {}:{}".format(host, port))
#        dataStore =  EDDDataStore(host, port)
#        log.debug("Adding output formats to known data formats")
#
#        descr = {"description":"Channelized complex voltage ouptut.",
#                "ip": None,
#                "port": None,
#                "sample_rate":None,
#                "central_freq":None,
#                "sync_time": None,
#                "predecimation_factor": None
#                }
#        dataStore.addDataFormatDefinition("Skarab:1", descr)



if __name__ == "__main__":

    parser = getArgumentParser()
    parser.add_argument('--packetizer-ip', dest='packetizer_ip', type=str, help='The control ip of the packetizer')
    parser.add_argument('--packetizer-port', dest='packetizer_port', type=int, default=7147, help='The port number to control the packetizer')

    args = parser.parse_args()
    setup_logger(args)

    pipeline = DigitizerControllerPipeline(
        args.host, args.port,
        args.packetizer_ip, args.packetizer_port)

    launchPipelineServer(pipeline, args)
