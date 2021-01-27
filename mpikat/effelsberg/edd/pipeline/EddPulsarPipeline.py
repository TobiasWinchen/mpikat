#Copyright (c) 2019 Jason Wu <jwu@mpifr-bonn.mpg.de>
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

from __future__ import print_function, unicode_literals, division

from mpikat.utils.process_tools import ManagedProcess, command_watcher
from mpikat.utils.process_monitor import SubprocessMonitor
import mpikat.utils.numa as numa
from mpikat.utils.core_manager import CoreManager

from mpikat.effelsberg.edd.pipeline.EDDPipeline import EDDPipeline, launchPipelineServer, updateConfig, state_change, StateChange
from mpikat.effelsberg.edd.EDDDataStore import EDDDataStore
from mpikat.effelsberg.edd.pipeline.dada_rnt import render_dada_header, make_dada_key_string
from mpikat.effelsberg.edd.pipeline.EddPulsarPipeline_blank_image import BLANK_IMAGE

import logging
import shlex
import shutil
import os
import re
import base64
from subprocess import Popen, PIPE
import tempfile
import json

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, LoggingEventHandler

from astropy.time import Time
import astropy.units as u
from astropy.coordinates import SkyCoord

from katcp import Sensor
from katcp.kattypes import request, return_reply, Int, Str

import time

import tornado
from tornado.gen import coroutine, sleep

log = logging.getLogger("mpikat.effelsberg.edd.pipeline.pipeline")

DEFAULT_CONFIG = {
    "id": "PulsarPipeline",
    "type": "PulsarPipeline",
    "mode": "Timing",                        # Timing, Searching, Baseband, Leap_baseband
    "cod": 0,                                # dm for coherent filterbanking, tested up to 3000, this will overwrite the DM got from par file if it is non-zero
    "npol": 4,                               # For search mode product, output 1 (Intensity) or 4 (Coherence) products
    "decimation": 8,                         # Decimation in frequency for filterbank output
    "filterbank_nchannels": 8192,            # Number of filterbank channels before decimation in digifits
    "zaplist": "800:1230",                   # Frequncy zap list
    "epta_directory": "epta",                # Data will be read from /mnt/epta_directory
    "nchannels": 1024,                       # Number of channels in pulsar archive, only used in timing mode
    "nbins": 1024,                           # Number of time bins in pulsar archive, only used in timing mode
    "tempo2_telescope_name": "Effelsberg",   # Effelsberg, SKA-MPG
    "merge_application": "edd_merge",        # edd_merge, edd_merge_10to8, edd_roach_merge, edd_roach_merge_leap
    "npart": 2,
    "merge_threads": 4,
    "dspsr_threads": 1,
    "fft_length": 8192,                      # 2048 for searching
    "input_data_streams":
    [
        {
            "format": "MPIFR_EDD_Packetizer:1",
            "ip": "225.0.0.180+3",
            "port": "7148",
            "bit_depth": 8,
            "sync_time": 0,
        },
        {
            "format": "MPIFR_EDD_Packetizer:1",
            "ip": "225.0.0.184+3",
            "port": "7148",
            "bit_depth": 8,
            "sync_time": 1599749491.0,
        }
    ],
    "dada_header_params":
    {
        "filesize": 4096000000,
        "instrument": "EDD",
        "receiver_name": "P217",
        "mode": "PSR",
        "nbit": 8,
        "ndim": 1,
        "npol": 2,
        "nchan": 1,
        "bandwidth": 800,
        "frequency_mhz": 1200,
        "resolution": 1,
        "tsamp": 0.000625,
        "dsb": 1,
        "heaps_nbytes": 4096,
        "nindices": 2,
        "idx1_step": 4096,
        "idx2_item": 2,
        "idx2_list": "0,1",
        "idx2_mask": "0x1",
        "slots_skip": 32,
        "dada_nslots": 3,
        },
    "dspsr_params":
    {
        "args": "-L 10 -r -minram 1024"
    },
    "dada_params":
    {
        "size": 409600000,
        "number": 32
    },
    "dadc_params":
    {
        "size": 409600000,
        "number": 32
    }
}


def is_accessible(path, mode='r'):
    """
    Check if the file or directory at `path` can
    be accessed by the program using `mode` open flags.
    """
    try:
        f = open(path, mode)
        f.close()
    except IOError:
        return False
    return True


def parse_tag(source_name):
    split = source_name.split("_")
    if len(split) == 1:
        return "default"
    else:
        return split[-1]


class ArchiveAdder(FileSystemEventHandler):
    def __init__(self, output_dir, zaplist):
        super(ArchiveAdder, self).__init__()
        self.first_file = True
        self.freq_zap_list = ""
        self.time_zap_list = ""
        self.update_freq_zaplist(zaplist)

    def _syscall(self, cmd):
        log.info("Calling: {}".format(cmd))
        proc = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
        proc.wait()
        if proc.returncode != 0:
            log.error(proc.stderr.read())
        else:
            log.debug("Call success")

    def fscrunch(self, fname):
        # frequency scrunch done here all fscrunch archive
        self._syscall("paz {} -e zapped {}".format(self.freq_zap_list, fname))
        self._syscall(
            "pam -F -e fscrunch {}".format(fname.replace(".ar", ".zapped")))
        return fname.replace(".ar", ".fscrunch")

    def first_tscrunch(self, fname):
        self._syscall("paz {} -e first {}".format(self.freq_zap_list, fname))

    def update_freq_zaplist(self, zaplist):
        for item in range(len(zaplist.split(","))):
            self.freq_zap_list = str(
                self.freq_zap_list) + " -F '{}' ".format(zaplist.split(",")[item])

        self.freq_zap_list = self.freq_zap_list.replace(":", " ")
        log.info("Latest frequency zaplist {}".format(self.freq_zap_list))

    def update_time_zaplist(self, zaplist):
        self.time_zap_list = ""
        for item in range(len(zaplist.split(":"))):
            self.time_zap_list = str(
                self.time_zap_list) + " {}".format(zaplist.split(":")[item])

        log.info("Latest time zaplist {}".format(self.time_zap_list))

    def process(self, fname):
        fscrunch_fname = self.fscrunch(fname)
        if self.first_file:
            log.info("First file in set. Copying to sum.?scrunch.")
            shutil.copy2(fscrunch_fname, "sum.fscrunch")
            self.first_tscrunch(fname)
            shutil.copy2(fname.replace(".ar", ".first"), "sum.tscrunch")
            os.remove(fname.replace(".ar", ".first"))
            self.first_file = False
        else:
            self._syscall("psradd -T -inplace sum.tscrunch {}".format(fname))
            # update fscrunch here with the latest list, cannot go backward
            # (i.e. cannot redo zap)
            self._syscall("paz {} -m sum.tscrunch".format(self.freq_zap_list))
            self._syscall(
                "psradd -inplace sum.fscrunch {}".format(fscrunch_fname))
            #self._syscall(
            #    "paz -w '{}' -m sum.fscrunch".format(self.time_zap_list)) #disabled at the moment as this is taking up a lot of CPU time
            self._syscall(
                "psrplot -p freq+ -jDp -D ../combined_data/tscrunch.png/png sum.tscrunch")
            self._syscall(
                "pav -DFTp sum.fscrunch  -g ../combined_data/profile.png/png")
            self._syscall(
                "pav -FYp sum.fscrunch  -g ../combined_data/fscrunch.png/png")
            log.info("removing {}".format(fscrunch_fname))
        os.remove(fscrunch_fname)
        os.remove(fscrunch_fname.replace(".fscrunch", ".zapped"))
        log.info("Accessing archive PNG files")

    def on_created(self, event):
        log.info("New file created: {}".format(event.src_path))
        try:
            fname = event.src_path
            log.info(fname.find('.ar.') != -1)
            if fname.find('.ar.') != -1:
                log.info(
                    "Passing archive file {} for processing".format(fname[0:-9]))
                time.sleep(1)
                self.process(fname[0:-9])
        except Exception as error:
            log.error(error)


class EddPulsarPipelineKeyError(Exception):
    pass


class EddPulsarPipelineError(Exception):
    pass


class EddPulsarPipeline(EDDPipeline):
    """
    @brief Interface object which accepts KATCP commands

    """
    VERSION_INFO = ("mpikat-edd-api", 0, 1)
    BUILD_INFO = ("mpikat-edd-implementation", 0, 1, "rc1")

    def __init__(self, ip, port):
        """@brief initialize the pipeline."""
        EDDPipeline.__init__(self, ip, port, DEFAULT_CONFIG)
        self.mkrec_cmd = []
        self._dada_buffers = ["dada", "dadc"]
        self._dspsr = None
        self._mkrecv_ingest_proc = None
        self._archive_directory_monitor = None

        # Pick first available numa node. Disable non-available nodes via
        # EDD_ALLOWED_NUMA_NODES environment variable
        self.numa_number = numa.getInfo().keys()[0]


    def setup_sensors(self):
        """
        @brief Setup monitoring sensors
        """
        EDDPipeline.setup_sensors(self)
        self._tscrunch = Sensor.string(
            "tscrunch_PNG",
            description="tscrunch png",
            default=BLANK_IMAGE,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._tscrunch)

        self._fscrunch = Sensor.string(
            "fscrunch_PNG",
            description="fscrunch png",
            default=BLANK_IMAGE,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._fscrunch)

        self._profile = Sensor.string(
            "profile_PNG",
            description="pulse profile png",
            default=BLANK_IMAGE,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._profile)

        self._central_freq = Sensor.string(
            "_central_freq",
            description="_central_freq",
            default="N/A",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._central_freq)

        self._source_name_sensor = Sensor.string(
            "target_name",
            description="target name",
            default="N/A",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._source_name_sensor)

        self._nchannels = Sensor.string(
            "_nchannels",
            description="_nchannels",
            default="N/A",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._nchannels)

        self._nbins = Sensor.string(
            "_nbins",
            description="_nbins",
            default="N/A",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._nbins)

        self._time_processed = Sensor.string(
            "_time_processed",
            description="_time_processed",
            default="N/A",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._time_processed)

        self._dm_sensor = Sensor.string(
            "_source_dm",
            description="_source_dm",
            default="N/A",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._dm_sensor)

        self._par_dict_sensor = Sensor.string(
            "_par_dict_sensor",
            description="_par_dict_sensor",
            default="N/A",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._par_dict_sensor)
        self._directory_size_sensor = Sensor.string(
            "_directory_size_sensor",
            description="_directory_size_sensor",
            default="N/A",
            initial_status=Sensor.UNKNOWN)

        self.add_sensor(self._par_dict_sensor)

    def _decode_capture_stdout(self, stdout, callback):
        log.debug('{}'.format(str(stdout)))


    def _error_treatment(self, callback):
        self.stop_pipeline_with_mkrecv_crashed()


    def _handle_execution_returncode(self, returncode, callback):
        log.debug(returncode)


    def _handle_execution_stderr(self, stderr, callback):
        if bool(stderr[:8] == "Finished") & bool("." not in stderr):
            self._time_processed.set_value(stderr)
            log.debug(stderr)
        if bool(stderr[:8] != "Finished"):
            log.info(stderr)


    def _handle_eddpolnmerge_stderr(self, stderr, callback):
        log.debug(stderr)


    @coroutine
    def _png_monitor(self):
        try:
            processed_seconds = int(
                os.popen("ls {}/*ar | wc -l".format(self.in_path)).read())
            self._time_processed.set_value(
                "{} s".format(processed_seconds * 10))
            log.info("processed {}s".format(processed_seconds * 10))
        except Exception as error:
            log.debug(error)
        log.info("reading png from : {}".format(self.out_path))
        try:
            log.info("reading {}/fscrunch.png".format(self.out_path))
            with open("{}/fscrunch.png".format(self.out_path), "rb") as imageFile:
                image_fscrunch = base64.b64encode(imageFile.read())
                self._fscrunch.set_value(image_fscrunch)
        except Exception as error:
            log.debug(error)
        try:
            log.info("reading {}/tscrunch.png".format(self.out_path))
            with open("{}/tscrunch.png".format(self.out_path), "rb") as imageFile:
                image_tscrunch = base64.b64encode(imageFile.read())
                self._tscrunch.set_value(image_tscrunch)
        except Exception as error:
            log.debug(error)
        try:
            log.info("reading {}/profile.png".format(self.out_path))
            with open("{}/profile.png".format(self.out_path), "rb") as imageFile:
                image_profile = base64.b64encode(imageFile.read())
                self._profile.set_value(image_profile)
        except Exception as error:
            log.debug(error)
        return

    @coroutine
    def _folder_size_monitor(self):
        try:
            total_size = 0
            for root, dirs, files in os.walk("{}".format(self.in_path)):
                for f in files:
                    total_size += os.path.getsize(os.path.join(root, f))
            total_size_in_gb = total_size / 1024 / 1024 / 1024.
            self._directory_size_sensor.set_value(
                "{} GB".format(total_size_in_gb))
            log.info("Directory size = {} GB".format(total_size_in_gb))
        except Exception as error:
            log.debug(error)
        return


    @coroutine
    def _create_ring_buffer(self, bufferSize, blocks, key, numa_node):
        """
        @brief Create a ring buffer of given size with given key on specified numa node.
               Adds and register an appropriate sensor to thw list
        """
        # always clear buffer first. Allow fail here
        yield command_watcher("dada_db -d -k {key}".format(key=key), allow_fail=True)

        cmd = "numactl --cpubind={numa_node} --membind={numa_node} dada_db -k {key} -n {blocks} -b {bufferSize} -p -l".format(
            key=key, blocks=blocks, bufferSize=bufferSize, numa_node=numa_node)
        log.debug("Running command: {0}".format(cmd))
        yield command_watcher(cmd)


    @coroutine
    def _reset_ring_buffer(self, key, numa_node):
        """
        @brief Create a ring buffer of given size with given key on specified numa node.
               Adds and register an appropriate sensor to thw list
        """
        # always clear buffer first. Allow fail here
        cmd = "numactl --cpubind={numa_node} --membind={numa_node} dbreset -k {key} --log_level debug".format(
            numa_node=numa_node, key=key)
        log.debug("Running command: {0}".format(cmd))
        yield command_watcher(cmd, allow_fail=True)


    def _buffer_status_handle(self, status):
        """
        @brief Process a change in the buffer status
        """
        pass


    @state_change(target="configured", allowed=["idle"], intermediate="configuring")
    @coroutine
    def configure(self, config_json):
        log.info("Configuring EDD backend for processing")
        log.debug("Configuration string: '{}'".format(config_json))
        yield self.set(config_json)

        if isinstance(self._config['input_data_streams'], dict):
            log.warning("CHANGING INPUT DATA STREAM TYPE FROM DICT TO LIST - THIS IS A HACKY HACK AND BE DONE PROPERLY!")
            l = [i for i in self._config['input_data_streams'].values()]
            self._config['input_data_streams'] = l
            log.debug(self._config)

        cfs = json.dumps(self._config, indent=4)
        log.info("Final configuration:\n" + cfs)

        self.__coreManager = CoreManager(self.numa_number)
        self.__coreManager.add_task("mkrecv", 9, prefere_isolated=True)
        self.__coreManager.add_task("tempo", 1)
        self.__coreManager.add_task("dspsr", self._config["dspsr_threads"])
        self.__coreManager.add_task("merge", self._config["merge_threads"])
        # The master contoller provides the data store IP as default gloal
        # config to all pipelines
        self.__eddDataStore = EDDDataStore(self._config["data_store"]["ip"], self._config["data_store"]["port"])

        self.sync_epoch = self._config['input_data_streams'][0]['sync_time']
        log.info("sync_epoch = {}".format(self.sync_epoch))

        yield self._create_ring_buffer(self._config["dada_params"]["size"], self._config["dada_params"]["number"], "dada", self.numa_number)
        yield self._create_ring_buffer(self._config["dadc_params"]["size"], self._config["dadc_params"]["number"], "dadc", self.numa_number)

        self.epta_dir = os.path.join("/mnt/", self._config["epta_directory"])
        if not os.path.isdir(self.epta_dir):
            log.error("Not a directory {} !".format(self.epta_dir))
            raise RuntimeError("Epta directory is no directory: {}".format(self.epta_dir))
        if os.path.isfile("/tmp/t2pred.dat"):
                os.remove("/tmp/t2pred.dat")



    @state_change(target="ready", allowed=["configured"], intermediate="capture_starting")
    def capture_start(self):
        log.debug('Received capture start, doing nothing.')


    @state_change(target="set", allowed=["ready", "measurement_starting", "configured", "streaming"], intermediate="measurement_preparing")
    @coroutine
    def measurement_prepare(self, config_json):
        self._subprocessMonitor = SubprocessMonitor()

        self._source_name = self.__eddDataStore.getTelescopeDataItem("source-name")
        ra = self.__eddDataStore.getTelescopeDataItem("ra")
        decl = self.__eddDataStore.getTelescopeDataItem("dec")
        scannum = self.__eddDataStore.getTelescopeDataItem("scannum")
        subscannum = self.__eddDataStore.getTelescopeDataItem("subscannum")
        log.debug("Retrieved data from telescope:\n   Source name: {}\n   RA = {},  decl = {}".format(self._source_name, ra, decl))

        if self._config["mode"] == "Timing":
            epta_file = os.path.join(self.epta_dir, '{}.par'.format(self._source_name.split("_")[0][1:]))
            log.debug("Checking epta file {}".format(epta_file))
            self.pulsar_flag = is_accessible(epta_file)
            if (parse_tag(self._source_name) != "R") and (not self.pulsar_flag):
                log.warning("source {} is neither pulsar nor calibrator. Will not react until next measurement prepare".format(self._source_name))
                raise StateChange("streaming")

        self._timer = Time.now()
        log.debug("Setting blank image")
        self._fscrunch.set_value(BLANK_IMAGE)
        self._tscrunch.set_value(BLANK_IMAGE)
        self._profile.set_value(BLANK_IMAGE)

        log.debug("writing mkrecv header")
        self.cuda_number = numa.getInfo()[self.numa_number]['gpus'][0]
        log.debug("  - Running on cuda core: {}".format(self.cuda_number))
        header = self._config["dada_header_params"]
        central_freq = header["frequency_mhz"]
        self._central_freq.set_value(str(header["frequency_mhz"]))
        self._source_name_sensor.set_value(self._source_name)
        self._nchannels.set_value(self._config["nchannels"])
        self._nbins.set_value(self._config["nbins"])
        header["telescope"] = self._config["tempo2_telescope_name"]
        log.debug("  - Tempo2 telescope name: {}".format(header['telescope']))

        c = SkyCoord("{} {}".format(ra, decl), unit=(u.deg, u.deg))
        header["ra"] = c.to_string("hmsdms").split(" ")[0].replace(
            "h", ":").replace("m", ":").replace("s", "")
        header["dec"] = c.to_string("hmsdms").split(" ")[1].replace(
            "d", ":").replace("m", ":").replace("s", "")
        header["key"] = self._dada_buffers[0]
        log.debug("  - Dada key: {}".format(header['key']))
        if header["instrument"] == "SKARAB":
            for i in self._config['input_data_streams']:
                header["mc_source"] += i["ip"] + ","
            header["mc_source"] = header["mc_source"][:-1]
        else:
            header["mc_source"] = self._config['input_data_streams'][0][
                "ip"] + "," + self._config['input_data_streams'][1]["ip"]
        log.debug("  - mc source: {}".format(header['mc_source']))
        header["mc_streaming_port"] = self._config[
            'input_data_streams'][0]["port"]
        log.debug("  - mc streaming port: {}".format(header['mc_streaming_port']))
        header["interface"] = numa.getFastestNic(self.numa_number)[1]['ip']
        log.debug("  - mc interface: {}".format(header['interface']))
        header["sync_time"] = self.sync_epoch
        log.debug("  - sync time: {}".format(header['sync_time']))
        header["sample_clock"] = float(self._config['input_data_streams'][0]["sample_rate"]) # adjsutment for the predecimation factor is done in the amster controller
        log.debug("  - sample_clock: {}".format(header['sample_clock']))
        header["source_name"] = self._source_name
        header["obs_id"] = "{0}_{1}".format(scannum, subscannum)
        header["filesize"] = int(float(self._config["dada_params"]["size"]) /8.0)
        log.debug("  - obs_id: {}".format(header['obs_id']))
        tstr = Time.now().isot.replace(":", "-")
        tdate = tstr.split("T")[0]


        log.debug("Setting up the input and scrunch data directories")
        if self._config["mode"] == "Timing":
            try:
                self.in_path = os.path.join("/mnt/dspsr_output/",
                                            tdate, self._source_name, str(central_freq), tstr, "raw_data")
                self.out_path = os.path.join(
                    "/mnt/dspsr_output/", tdate, self._source_name, str(central_freq), tstr, "combined_data")
                log.debug("Creating directories")
                log.debug("in path {}".format(self.in_path))
                log.debug("out path {}".format(self.out_path))
                if not os.path.isdir(self.in_path):
                    os.makedirs(self.in_path)
                if not os.path.isdir(self.out_path):
                    os.makedirs(self.out_path)
                os.chdir(self.in_path)
                log.debug("Change to workdir: {}".format(os.getcwd()))
                log.debug("Current working directory: {}".format(os.getcwd()))
            except Exception as error:
                raise EddPulsarPipelineError(str(error))
        elif self._config["mode"] == "Searching":
            try:
                self.in_path = os.path.join("/mnt/filterbank_output/",
                    tdate, self._source_name, str(central_freq), tstr)
                log.debug("Creating directories")
                log.debug("in path {}".format(self.in_path))
                if not os.path.isdir(self.in_path):
                    os.makedirs(self.in_path)
                os.chdir(self.in_path)
                log.debug("Change to workdir: {}".format(os.getcwd()))
                log.debug("Current working directory: {}".format(os.getcwd()))
            except Exception as error:
                raise EddPulsarPipelineError(str(error))
        elif self._config["mode"] == "Baseband":
            try:
                self.in_path = os.path.join("/mnt/baseband_output/",
                    tdate, self._source_name, str(central_freq), tstr)
                log.debug("Creating directories")
                log.debug("in path {}".format(self.in_path))
                if not os.path.isdir(self.in_path):
                    os.makedirs(self.in_path)
                os.chdir(self.in_path)
                log.debug("Change to workdir: {}".format(os.getcwd()))
                log.debug("Current working directory: {}".format(os.getcwd()))
            except Exception as error:
                raise EddPulsarPipelineError(str(error))
        elif self._config["mode"] == "Leap_baseband":
            try:
                self.in_path = os.path.join("/mnt/Leap_baseband_output/",
                    tdate, self._source_name, str(central_freq), tstr)
                log.debug("Creating directories")
                log.debug("in path {}".format(self.in_path))
                if not os.path.isdir(self.in_path):
                    os.makedirs(self.in_path)
                os.chdir(self.in_path)
                log.debug("Change to workdir: {}".format(os.getcwd()))
                log.debug("Current working directory: {}".format(os.getcwd()))
            except Exception as error:
                raise EddPulsarPipelineError(str(error))

        os.chdir("/tmp/")
        log.debug("Creating the predictor with tempo2")
        if self._config["mode"] == "Timing":
            if (parse_tag(self._source_name) != "R") & is_accessible(epta_file):
                cmd = 'numactl -m {} taskset -c {} tempo2 -f {} -pred'.format(
                    self.numa_number, self.__coreManager.get_coresstr('tempo'),
                    epta_file).split()

                cmd.append("{} {} {} {} {} 24 2 3599.999999999".format(self._config["tempo2_telescope_name"], Time.now().mjd - 1, Time.now().mjd + 1, float(central_freq) - 200, float(central_freq) + 200))
                log.debug("Command to run: {}".format(cmd))
                yield command_watcher(cmd, allow_fail=True)
                attempts = 0
                retries = 5
                while True:
                    if attempts >= retries:
                        error = "Could not read t2pred.dat"
                        log.warning("{}. Will not react until next measurement prepare".format(error))
                        raise StateChange("streaming")
                    else:
                        yield sleep(1)
                        if is_accessible('{}/t2pred.dat'.format(os.getcwd())):
                            log.debug('found {}/t2pred.dat'.format(os.getcwd()))
                            break
                        else:
                            attempts += 1

        self.dada_header_file = tempfile.NamedTemporaryFile(
            mode="w",
            prefix="edd_dada_header_",
            suffix=".txt",
            dir="/tmp/",
            delete=False)
        log.debug(
            "Writing dada header file to {0}".format(
                self.dada_header_file.name))
        header_string = render_dada_header(header)
        self.dada_header_file.write(header_string)
        self.dada_key_file = tempfile.NamedTemporaryFile(
            mode="w",
            prefix="dada_keyfile_",
            suffix=".key",
            dir="/tmp/",
            delete=False)
        log.debug("Writing dada key file to {0}".format(
            self.dada_key_file.name))
        key_string = make_dada_key_string(self._dada_buffers[1])
        self.dada_key_file.write(make_dada_key_string(self._dada_buffers[1]))
        log.debug("Dada key file contains:\n{0}".format(key_string))
        self.dada_header_file.close()
        self.dada_key_file.close()

        attempts = 0
        retries = 5
        while True:
            if attempts >= retries:
                error = "could not read dada_key_file"
                raise EddPulsarPipelineError(error)
            else:
                yield sleep(1)
                if is_accessible('{}'.format(self.dada_key_file.name)):
                    log.debug('found {}'.format(self.dada_key_file.name))
                    break
                else:
                    attempts += 1
        ## Setting DM value for filterbank recording
        self.par_dict = {}
        self.dm = 0
        try:
            with open(os.path.join(self.epta_dir, '{}.par'.format(self._source_name.split("_")[0][1:]))) as fh:
                for line in fh:
                    #par file can have 2-4 columns, this is a stupid way to do it
                    if len(line.strip().split()) == 2:
                        key, value = line.strip().split()
                    elif len(line.strip().split()) == 3:
                        key, value, error = line.strip().split()
                    elif len(line.strip().split()) == 4:
                        key, value, lock, error = line.strip().split()
                    self.par_dict[key] = value.strip()
        except IOError as error:
            log.info(error)
        try:
            self.dm = float(self.par_dict["DM"])
        except KeyError as error:
            log.info("Key {} not found".format(error))
        if self._config["cod"] != 0:
            log.info("Overriding filterbank cod DM value from config from {} to {}".format(self.dm, self._config["cod"]))
            self.dm = self._config["cod"]
        self._dm_sensor.set_value(self.dm)
        self._par_dict_sensor.set_value(json.dumps(self.par_dict))


    @state_change(target="measuring", allowed=["set", "ready", "measurement_preparing"], waitfor="set", intermediate="measurement_starting")
    @coroutine
    def measurement_start(self):
        ####################################################
        #STARTING DSPSR                                    #
        ####################################################
        if self.previous_state == "set":
            os.chdir(self.in_path)
            log.debug("source_name = {}".format(
                self._source_name))
            if self._config["mode"] == "Timing":
                epta_file = os.path.join(self.epta_dir, '{}.par'.format(self._source_name.split("_")[0][1:]))
                if (parse_tag(self._source_name) != "R") and self.pulsar_flag:
                    cmd = "numactl -m {numa} dspsr {args} {nchan} {nbin} -fft-bench -x {fft_length} -cpu {cpus} -cuda {cuda_number} -P {predictor} -N {name} -E {parfile} {keyfile}".format(
                        numa=self.numa_number,
                        fft_length=self._config["fft_length"],
                        args=self._config["dspsr_params"]["args"],
                        nchan="-F {}:D".format(self._config["nchannels"]),
                        nbin="-b {}".format(self._config["nbins"]),
                        name=self._source_name.split("_")[0],
                        predictor="/tmp/t2pred.dat",
                        parfile=epta_file,
                        cpus=self.__coreManager.get_coresstr('dspsr'),
                        cuda_number=self.cuda_number,
                        keyfile=self.dada_key_file.name)

                elif parse_tag(self._source_name) == "R":
                    cmd = "numactl -m {numa} dspsr -L 10 -c 1.0 -D 0.0001 -r -minram 1024 -fft-bench -x {fft_length} {nchan} -cpu {cpus} -N {name} -cuda {cuda_number} {keyfile}".format(
                        numa=self.numa_number,
                        args=self._config["dspsr_params"]["args"],
                        fft_length=self._config["fft_length"],
                        nchan="-F {}:D".format(self._config["nchannels"]),
                        name=self._source_name.split("_")[0],
                        cpus=self.__coreManager.get_coresstr('dspsr'),
                        cuda_number=self.cuda_number,
                        keyfile=self.dada_key_file.name)
                else:
                    error = "source is unknown"
                    raise EddPulsarPipelineError(error)

            if self._config["mode"] == "Searching":
                cmd = "numactl -m {numa} digifits -b 8 -F {nchan}:D -D {DM} -p {npol} -f {decimation} -do_dedisp -x {fft_length} -cpu {cpus} -cuda {cuda_number} -o {name}_{DM}_{npol}.fits {keyfile}".format(
                    numa=self.numa_number, npol=self._config["npol"],
                    DM=self.dm,
                    nchan=self._config["filterbank_nchannels"],
                    fft_length=self._config["fft_length"],
                    decimation=self._config["decimation"],
                    name=self._source_name,
                    cpus=self.__coreManager.get_coresstr('dspsr'),
                    cuda_number=self.cuda_number,
                    keyfile=self.dada_key_file.name)

            if self._config["mode"] == "Baseband":
                cmd = "numactl -m {numa} taskset -c {cpus} dada_dbdisk -D {in_path} -b {cpus} -o -k dadc".format(
                    numa=self.numa_number,
                    in_path=self.in_path,
                    cpus=self.__coreManager.get_coresstr('dspsr'))

            if self._config["mode"] == "Leap_baseband":
                cmd = "numactl -m {numa} taskset -c {cpus} dbdiskleap -n 8".format(
                    numa=self.numa_number,
                    cpus=self.__coreManager.get_coresstr('dspsr'))

            log.debug("Running command: {0}".format(cmd))
            if self._config["mode"] == "Timing":
                log.info("Staring dspsr")
            if self._config["mode"] == "Searching":
                log.info("Staring digifits")
            if self._config["mode"] == "Baseband":
                log.info("Staring dada_dbdisk")
            if self._config["mode"] == "Leap_baseband":
                log.info("Staring dbdiskleap")
            self._dspsr = ManagedProcess(cmd)
            self._subprocessMonitor.add(self._dspsr, self._subprocess_error)

            ####################################################
            #STARTING merging code                         #
            ####################################################
            if self._config["mode"] not in "Leap_baseband":
                cmd = "numactl -m {numa} taskset -c {cpu} {merge_application} -p {npart} -n {nthreads} --log_level=info".format(
                    numa=self.numa_number,
                    cpu=self.__coreManager.get_coresstr('merge'),
                    nthreads=self._config["merge_threads"],
                    merge_application=self._config["merge_application"],
                    npart=self._config["npart"])
                log.debug("Running command: {0}".format(cmd))
                log.info("Staring EDDPolnMerge")
                self._polnmerge_proc = ManagedProcess(cmd)
                self._subprocessMonitor.add(
                    self._polnmerge_proc, self._subprocess_error)

            ####################################################
            #STARTING MKRECV                                   #
            ####################################################
            cmd = "numactl -m {numa} taskset -c {cpu} mkrecv_v4 --header {dada_header} --lst --quiet".format(
                numa=self.numa_number, cpu=self.__coreManager.get_coresstr('mkrecv'), dada_header=self.dada_header_file.name)
            log.debug("Running command: {0}".format(cmd))
            log.info("Staring MKRECV")
            self._mkrecv_ingest_proc = ManagedProcess(cmd)
            self._subprocessMonitor.add(
                self._mkrecv_ingest_proc, self._subprocess_error)

            ####################################################
            #STARTING ARCHIVE MONITOR                          #
            ####################################################
            if self._config["mode"] == "Timing":
                log.info("Staring archive monitor")
                self.archive_observer = Observer()
                self.archive_observer.daemon = False
                log.info("Input directory: {}".format(self.in_path))
                log.info("Output directory: {}".format(self.out_path))
                log.info("Setting up ArchiveAdder handler")
                self.handler = ArchiveAdder(self.out_path, self._config["zaplist"])
                self.archive_observer.schedule(self.handler, str(self.in_path), recursive=False)
                log.info("Starting directory monitor")
                self.archive_observer.start()
                self._png_monitor_callback = tornado.ioloop.PeriodicCallback(
                    self._png_monitor, 5000)
                self._png_monitor_callback.start()
            else:
                self._folder_size_monitor_callback = tornado.ioloop.PeriodicCallback(
                    self._folder_size_monitor, 5000)
                self._folder_size_monitor_callback.start()
            self._subprocessMonitor.start()
            self._timer = Time.now() - self._timer
            log.debug("Took {} s to start".format(self._timer * 86400))
        elif self.previous_state == "streaming":
            log.debug("previous state = streaming, do nothing, will wait for the next measurement prepare")


    @state_change(target="ready", intermediate="measurement_stopping")
    @coroutine
    def measurement_stop(self):
        """@brief stop mkrecv merging application and dspsr instances."""
        if self.previous_state == "measuring":
            if self._subprocessMonitor is not None:
                self._subprocessMonitor.stop()
            if self._config["mode"] == "Timing":
                self._png_monitor_callback.stop()
            else:
                self._folder_size_monitor_callback.stop()
            if self._config["mode"] not in "Leap_baseband":
                process = [self._mkrecv_ingest_proc,
                           self._polnmerge_proc]
                for proc in process:
                    proc.terminate(timeout=1)
            else:
                self._mkrecv_ingest_proc.terminate(timeout=1)
            if os.path.isfile("/tmp/t2pred.dat"):
                os.remove("/tmp/t2pred.dat")
            log.info("reset DADA buffer")
            yield self._create_ring_buffer(self._config["dada_params"]["size"], self._config["dada_params"]["number"], "dada", self.numa_number)
            yield self._create_ring_buffer(self._config["dadc_params"]["size"], self._config["dadc_params"]["number"], "dadc", self.numa_number)
            del self._subprocessMonitor
        else:
            raise StateChange("streaming")



    @state_change(target="idle", intermediate="deconfiguring", error='panic')
    @coroutine
    def deconfigure(self):
        """@brief deconfigure the pipeline."""
        log.debug("Destroying dada buffers")

        for k in self._dada_buffers:
            cmd = "dada_db -d -k {0}".format(k)
            log.debug("Running command: {0}".format(cmd))
            yield command_watcher(cmd, allow_fail=True)


    @request(Str())
    @return_reply()
    def request_freq_zaplist(self, req, zaplist):
        """
        @brief      Add freq zaplist

        """
        @coroutine
        def zaplist_wrapper():
            try:
                yield self.freq_zaplist(zaplist)
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(zaplist_wrapper)
        raise AsyncReply


    def freq_zaplist(self, zaplist):
        """
        @brief     Add zap list to Katcp sensor
        """
        self._freq_zaplist_sensor.set_value(zaplist)
        try:
            self.handler.update_freq_zaplist(zaplist)
        except:
            pass
        return


    @request(Str())
    @return_reply()
    def request_time_zaplist(self, req, zaplist):
        """
        @brief      Add freq zaplist

        """
        @coroutine
        def zaplist_wrapper():
            try:
                yield self.time_zaplist(zaplist)
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(zaplist_wrapper)
        raise AsyncReply


    def time_zaplist(self, zaplist):
        """
        @brief     Add zap list to Katcp sensor
        """
        self._time_zaplist_sensor.set_value(zaplist)
        try:
            self.handler.update_time_zaplist(zaplist)
        except:
            pass
        return


if __name__ == "__main__":
    launchPipelineServer(EddPulsarPipeline)

