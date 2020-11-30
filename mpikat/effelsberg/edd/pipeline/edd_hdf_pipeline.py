"""
Pipeline to record EDD data.

Currently only gates spectrometer dta format is understood.
"""

from __future__ import print_function, division, unicode_literals
from tornado.gen import coroutine, sleep
from tornado.ioloop import PeriodicCallback
import logging
import time
import errno
import numpy as np
import json
from datetime import datetime
from threading import Thread, Event

import sys
if sys.version_info[0] >=3:
    import queue
else:
    import Queue as queue       # In python 3 this will be queue

from multiprocessing import Process, Pipe

import spead2
import spead2.recv

from katcp import Sensor, ProtocolFlags
from katcp.kattypes import (request, return_reply)

from mpikat.effelsberg.edd.pipeline.EDDPipeline import EDDPipeline, launchPipelineServer, state_change, value_list
from mpikat.effelsberg.edd.edd_HDF5_writer import EDDHDFFileWriter

import mpikat.utils.numa as numa

_log = logging.getLogger("mpikat.hdf5pipeline")



def conditional_update(sensor, value, status=1, timestamp=None):
    """
    Update a sensor if the value has changed
    """
    if sensor.value() != value:
        sensor.set_value(value, status, timestamp=timestamp)



class EDDHDF5WriterPipeline(EDDPipeline):
    """
    Write EDD Data Streams to HDF5 Files.

    Configuration
    -------------

    """
    def __init__(self, ip, port):
        """
        Args:
            ip:   IP accepting katcp control connections from.
            port: Port number to serve on
        """
        EDDPipeline.__init__(self, ip, port, dict(output_directory="/mnt",
            input_data_streams={
            "Stokes_I_0" :
            {
                "format": "GatedSpectrometer:1",
                "ip": "225.0.1.172",
                "port": "7152",
            },
            "Stokes_I_1" :
            {
                "format": "GatedSpectrometer:1",
                "ip": "225.0.1.173",
                "port": "7152",
            },
            "Stokes_Q_0" :
            {
                "format": "GatedSpectrometer:1",
                "ip": "225.0.1.174",
                "port": "7152",
            },
            "Stokes_Q_1" :
            {
                "format": "GatedSpectrometer:1",
                "ip": "225.0.1.175",
                "port": "7152",
            },
            "Stokes_U_0" :
            {
                "format": "GatedSpectrometer:1",
                "ip": "225.0.1.176",
                "port": "7152",
            },
            "Stokes_U_1" :
            {
                "format": "GatedSpectrometer:1",
                "ip": "225.0.1.177",
                "port": "7152",
            },
            "Stokes_V_0" :
            {
                "format": "GatedSpectrometer:1",
                "ip": "225.0.1.178",
                "port": "7152",
            },
            "Stokes_V_1" :
            {
                "format": "GatedSpectrometer:1",
                "ip": "225.0.1.179",
                "port": "7152",
            }
            },
            id="hdf5_writer", type="hdf5_writer"))
        self.mc_interface = []

        self.__periodic_callback = PeriodicCallback(self.periodic_sensor_update, 1000)
        self.__periodic_callback.start()

        self.__output_file = None
        self.__measuring = False


    def setup_sensors(self):
        """
        Setup monitoring sensors
        """
        EDDPipeline.setup_sensors(self)

        self._spectra_written= Sensor.integer(
            "written-packages",
            description="Number of spectra written to file.",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._spectra_written)

        self._incomplete_heaps = Sensor.integer(
            "incomplete-heaps",
            description="Incomplete heaps received.",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._incomplete_heaps)

        self._complete_heaps = Sensor.integer(
            "complete-heaps",
            description="Complete heaps received.",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._complete_heaps)




    @state_change(target="idle", allowed=["streaming", "deconfiguring"], intermediate="capture_stopping")
    @coroutine
    def capture_stop(self):
        if self._capture_thread:
            _log.debug("Cleaning up capture thread")
            self._capture_thread.stop()
            self._capture_thread.join()
            self._capture_thread = None
            _log.debug("Capture thread cleaned")


    @state_change(target="idle", intermediate="deconfiguring", error='panic')
    @coroutine
    def deconfigure(self):
        pass


    @state_change(target="configured", allowed=["idle"], intermediate="configuring")
    @coroutine
    def configure(self, config_json):
        """
        """
        _log.info("Configuring HDF5 Writer")
        _log.debug("Configuration string: '{}'".format(config_json))

        yield self.set(config_json)

        cfs = json.dumps(self._config, indent=4)
        _log.info("Final configuration:\n" + cfs)

        #ToDo: allow streams with multiple multicast groups and multiple ports
        self.mc_interface = []
        self.mc_port = None
        for stream_description in value_list(self._config['input_data_streams']):
            self.mc_interface.append(stream_description['ip'])
            if self.mc_port is None:
                self.mc_port = stream_description['port']
            else:
                if self.mc_port != stream_description['port']:
                    raise RuntimeError("All input streams have to use the same port!!!")



    def _package_writer(self, (section, data)):
        if self._state == "measuring":
            _log.info('Writing data to section: {}'.format(section))
            self.__output_file.addData(section, data)
        else:
            _log.debug("Not measuring, Dropping package")




    @state_change(target="ready", allowed=["configured"], intermediate="capture_starting")
    @coroutine
    def capture_start(self):
        """
        """
        _log.info("Starting FITS interface capture")
        nic_name, nic_description = numa.getFastestNic()
        self._capture_interface = nic_description['ip']
        _log.info("Capturing on interface {}, ip: {}, speed: {} Mbit/s".format(nic_name, nic_description['ip'], nic_description['speed']))
        affinity = numa.getInfo()[nic_description['node']]['cores']

        spead_handler = GatedSpectrometerSpeadHandler()
        self._capture_thread = SpeadCapture(self.mc_interface,
                                           self.mc_port,
                                           self._capture_interface,
                                           spead_handler, self._package_writer, affinity)
        self._capture_thread.start()


    @coroutine
    def periodic_sensor_update(self):
        """
        Updates the katcp sensors.

        This is a peiodic update as the connection are managed using threads and not coroutines.
        """
        timestamp = time.time()
        if self._capture_thread:
            conditional_update(self._incomplete_heaps, self._capture_thread._incomplete_heaps, timestamp=timestamp)
            conditional_update(self._complete_heaps, self._capture_thread._complete_heaps, timestamp=timestamp)



    @state_change(target="set", allowed=["ready", "measurement_starting", "configured", "streaming"], intermediate="measurement_preparing")
    def measurement_prepare(self, config={}):
        try:
            config = json.loads(config)
        except:
            _log.error("Cannot parse json:\n{}".format(config))
            raise RuntimeError("Cannot parse json.")



        if ("new_file" in config and config["new_file"]) or (not self.__output_file):
            _log.debug("Creating new file")
            self.__output_file = EDDHDFFileWriter(path=self._config["output_directory"])

        if ("override_newsubscan"  in config and config["override_newsubscan"]):
            _log.debug("Overriding new subscan creation")
        else:
            _log.debug("Creating new subscan")
            self.__output_file.newSubscan()



    @state_change(target="measuring", allowed=["set", "ready", "measurement_preparing"], waitfor="set", intermediate="measurement_starting")
    @coroutine
    def measurement_start(self):
        _log.info("Starting file output")
        # The state measuring isw aht is checked,. This is enough, as also
        # error handling is done correctly


    @state_change(target="ready", intermediate="measurement_stopping")
    @coroutine
    def measurement_stop(self):
        _log.info("Stopping FITS interface data transfer")
        # ToDo: There probably should be an output queue so that time ordering
        # is not becoming an issue



    @coroutine
    def stop(self):
        """
        Handle server stop. Stop all threads
        """
        try:
            if self._capture_thread:
                self._capture_thread.stop()
                self._capture_thread.join(3.0)
        except Exception as E:
            _log.error("Exception during stop! {}".format(E))
        super(EDDHDF5WriterPipeline, self).stop()



class SpeadCapture(Thread):
    """
    Captures heaps from one or more streams that are transmitted in SPEAD
    format and call handler on completed heaps.
    """
    def __init__(self, mc_ip, mc_port, capture_ip, spead_handler, package_handler, numa_affinity = []):
        """
        Args:
            mc_ip:              Array of multicast group IPs
            mc_port:            Port number of multicast streams
            capture_ip:         Interface to capture streams from MCG  on
            handler:            Object that handles data in the stream
        """
        Thread.__init__(self, name=self.__class__.__name__)
        self._mc_ip = mc_ip
        self._mc_port = mc_port
        self._capture_ip = capture_ip
        self._stop_event = Event()
        self._spead_handler = spead_handler
        self._package_handler = package_handler

        #ToDo: fix magic numbers for parameters in spead stream
        thread_pool = spead2.ThreadPool(threads=8, affinity=[int(k) for k in numa_affinity])
        self.stream = spead2.recv.Stream(thread_pool, spead2.BUG_COMPAT_PYSPEAD_0_5_2, max_heaps=64, ring_heaps=64, contiguous_only = False)
        pool = spead2.MemoryPool(16384, ((32*4*1024**2)+1024), max_free=64, initial=64)
        self.stream.set_memory_allocator(pool)

        ##ToDo: fix magic numbers for parameters in spead stream
        #thread_pool = spead2.ThreadPool(threads=4)
        #self.stream = spead2.recv.Stream(thread_pool, spead2.BUG_COMPAT_PYSPEAD_0_5_2, max_heaps=64, ring_heaps=64, contiguous_only = False)
        #pool = spead2.MemoryPool(16384, ((32*4*1024**2)+1024), max_free=64, initial=64)
        #self.stream.set_memory_allocator(pool)
        #self.rb = self.stream.ringbuffer
        self._nheaps = 0
        self._incomplete_heaps = 0
        self._complete_heaps = 0


    def stop(self):
        """
        Stop the capture thread
        """
        self._spead_handler.stop = True
        self.stream.stop()
        self._stop_event.set()


    def run(self):
        """
        Subscribe to MC groups and start processing.
        """
        _log.debug("Subscribe to multicast groups:")
        for i, mg in enumerate(self._mc_ip):
            _log.debug(" - Subs {}: ip: {}, port: {}".format(i,self._mc_ip[i], self._mc_port ))
            self.stream.add_udp_reader(mg, int(self._mc_port), max_size = 9200,
                buffer_size=1073741820, interface_address=self._capture_ip)

        _log.debug("Start processing heaps:")
        self._nheaps = 0
        self._incomplete_heaps = 0
        self._complete_heaps = 0

        for heap in self.stream:
            self._nheaps += 1
            _log.debug('Received heap {} - previously {} completed, {} incompleted'.format(self._nheaps, self._complete_heaps, self._incomplete_heaps))
            if isinstance(heap, spead2.recv.IncompleteHeap):
                _log.warning('Received incomplete heap - received only {} / {} bytes.'.format(heap.received_length, heap.heap_length))
                self._incomplete_heaps += 1
                continue
            else:
                self._complete_heaps += 1
            try:
                package = self._spead_handler(heap)
            except Exception as E:
                _log.error("Error decoding heap. Cought exception:\n{}".format(E))
                continue        # With next heap

            try:
                self._package_handler(package)
            except Exception as E:
                _log.error("Error handling decoded data package. Cought exception:\n{}".format(E))




def convert48_64(A):
    """
    Converts 48 bit to 64 bit integers.

    Args:
        A:  array of 6 bytes.

    Returns:
    int
    """
    assert (len(A) == 6)
    npd = np.array(A, dtype=np.uint64)
    return np.sum(256**np.arange(0,6, dtype=np.uint64)[::-1] * npd)



class GatedSpectrometerSpeadHandler(object):
    """
    Parse heaps of gated spectrometer output from spead stream and create data dict.

    """
    def __init__(self):

       # self.plottingQueue = queue.PriorityQueue()
        #self.__delay = delay

        #Description of heap items
        # ToDo: move to gated spectrometer or whereever the stream format is
        # defined
        self.ig = spead2.ItemGroup()
        self.ig.add_item(5632, "timestamp_count", "", (6,), dtype=">u1")
        self.ig.add_item(5633, "polarization", "", (6,), dtype=">u1")
        self.ig.add_item(5634, "noise_diode_status", "", (6,), dtype=">u1")
        self.ig.add_item(5635, "fft_length", "", (6,), dtype=">u1")
        self.ig.add_item(5636, "number_of_input_samples", "", (6,), dtype=">u1")
        self.ig.add_item(5637, "sync_time", "", (6,), dtype=">u1")
        self.ig.add_item(5638, "sampling_rate", "", (6,), dtype=">u1")
        self.ig.add_item(5639, "naccumulate", "", (6,), dtype=">u1")

    def __call__(self, heap):
        """
        handle heaps. Merge polarization heaps with matching timestamps and pass to output queue.
        """
        _log.debug("Unpacking heap")
        items = self.ig.update(heap)
        if 'data' not in items:
            # On first heap only, get number of channels
            fft_length = convert48_64(items['fft_length'].value)
            nchannels = int((fft_length/2)+1)
            _log.debug("First item - setting data length to {} channels".format(nchannels))
            self.ig.add_item(5640, "data", "", (nchannels,), dtype="<f")
            # Reprocess heap to get data
            items = self.ig.update(heap)

        _log.debug("Checking missign keys.")
        if len(items.keys()) != len(self.ig.items()):
            missing_keys = []
            for key in self.ig.items():
                if key[0] not in items:
                    missing_keys.append(key[0])
            _log.warning("Received invalid heap, containing only {} / {} keys. Missign keys:\n {}".format(len(items.keys()), len(self.ig.items()), " \n".join(missing_keys)))
            return

        _log.debug("No missing keys.")
        pol = convert48_64(items["polarization"].value)
        nds = convert48_64(items["noise_diode_status"].value)
        section_id = "S{}_ND_{}".format(pol, nds)
        _log.debug("Set section_id: {}".format(section_id))

        sampling_rate = float(convert48_64(items["sampling_rate"].value))
        fft_length = convert48_64(items["fft_length"].value)
        number_of_input_samples = convert48_64(items["number_of_input_samples"].value)
        naccumulate = convert48_64(items["naccumulate"].value)
        sync_time = convert48_64(items["sync_time"].value)
        timestamp_count = convert48_64(items["timestamp_count"].value)

        data = {}
        data['spectrum'] = items['data'].value

        # Integration period does not contain efficiency of sampling as heaps may
        # be lost respectively not in this gate
        integration_period = (naccumulate * fft_length) / sampling_rate

        # The reference time is in the center of the integration # period
        reference_time = float(sync_time) + float(timestamp_count) / sampling_rate + float(integration_period/ 2.)
        _log.debug("Set timestamp: {}".format(reference_time))

        data['timetamp'] = np.array([reference_time])

        data['integration_time'] = np.array([number_of_input_samples / sampling_rate])
        data['saturated_samples'] = np.array([-42])

        return section_id, data


if __name__ == "__main__":
    launchPipelineServer(EDDHDF5WriterPipeline)

