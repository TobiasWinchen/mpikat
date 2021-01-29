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
from threading import Thread, Event, Lock
import copy


import sys
if sys.version_info[0] >=3:
    import queue
else:
    import Queue as queue       # In python 3 this will be queue

import spead2
import spead2.recv

from katcp import Sensor, ProtocolFlags
from katcp.kattypes import (request, return_reply)

from mpikat.effelsberg.edd.pipeline.EDDPipeline import EDDPipeline, launchPipelineServer, state_change, value_list
from mpikat.effelsberg.edd.edd_HDF5_writer import EDDHDFFileWriter

import mpikat.utils.numa as numa

_log = logging.getLogger("mpikat.hdf5pipeline")

from concurrent.futures import ProcessPoolExecutor

def conditional_update(sensor, value, status=1, timestamp=None):
    """
    Update a sensor if the value has changed
    """
    if sensor.value() != value:
        sensor.set_value(value, status, timestamp=timestamp)


def plot_script(data, plotmap, ndisplay_channels=1024):
    """
    data dict: [key] [list of data sets]
    plotmap dict: [key] figure numer
    """
    print(data)
    data = {}

    import matplotlib as mpl
    mpl.use('Agg')
    import numpy as np
    import pylab as plt
    import astropy.time

    import sys
    if sys.version_info[0] >=3:
        import io
    else:
        import cStringIO as io
        io.BytesIO = io.StringIO

    import base64
    mpl.rcParams.update(mpl.rcParamsDefault)
    mpl.use('Agg')
    ########################################################################
    # Actual plot routine
    ########################################################################
    # Reorder by subplot
    ko = {}
    for k,i in plotmap.items():
        if i not in ko:
            ko[i] = []
        ko[i].append(k)

    figsize = {2: (8, 4), 4: (8, 8)}
    fig = plt.figure(figsize=figsize[len(ko)])

    timestamp = None

    # loop over suplotindices
    for si, plot_keys in ko.items():
        sub = fig.add_subplot(si)

        if not data:
            sub.text(.5, .5, "NO DATA")

        for k in sorted(plot_keys):
            S = np.zeros(ndisplay_channels)
            T = 1E-64

            if k not in data:
                #_log.warning("{} not in data!".format(k))
                #_log.debug("Data: {}".format(data))
                continue

            for d in data[k]:
                di = d['spectrum'][1:].reshape([ndisplay_channels, (d['spectrum'].size -1) // ndisplay_channels]).sum(axis=1)
                if np.isfinite(di).all():
                    S += di
                    T += d['integration_time']
                    timestamp = d['timestamp'][-1]

            sub.plot(10. * np.log10(S / T), label="{} (T = {:.2f} s)".format(k.replace('_', ' '), T))
        sub.legend()
        sub.set_xlabel('Channel')
        sub.set_ylabel('PSd [dB]')
    if timestamp:
        fig.suptitle('{}'.format(astropy.time.Time(timestamp, format='unix').isot))
    else:
        fig.suptitle('NO DATA {}'.format(astropy.time.Time.now().isot))

    fig.tight_layout(rect=[0, 0.03, 1, 0.95])

    ########################################################################
    # End of plot
    ########################################################################
    fig_buffer = io.BytesIO()
    fig.savefig(fig_buffer, format='png')
    fig_buffer.seek(0)
    b64 = base64.b64encode(fig_buffer.read())

    return b64






class EDDHDF5WriterPipeline(EDDPipeline):
    """
    Write EDD Data Streams to HDF5 Files.

    Configuration
    -------------
        default_hdf5_group_prefix

    Input Data Steams
    -----------------



    """
    def __init__(self, ip, port):
        """
        Args:
            ip:   IP accepting katcp control connections from.
            port: Port number to serve on
        """
        EDDPipeline.__init__(self, ip, port, dict(output_directory="/mnt",
                input_data_streams = [
                {
                    "source": "gated_spectrometer_0:polarization_0_0",
                    "hdf5_group_prefix": "P",
                    "format": "GatedSpectrometer:1",
                    "ip": "225.0.1.183",
                    "port": "7152"
                },
                {
                    "source": "gated_spectrometer_0:polarization_0_1",
                    "hdf5_group_prefix": "P",
                    "format": "GatedSpectrometer:1",
                    "ip": "225.0.1.182",
                    "port": "7152"
                },
                {
                    "source": "gated_spectrometer_1:polarization_1_0",
                    "hdf5_group_prefix": "P",
                    "format": "GatedSpectrometer:1",
                    "ip": "225.0.1.185",
                    "port": "7152"
                },
                {
                    "source": "gated_spectrometer_1:polarization_1_1",
                    "hdf5_group_prefix": "P",
                    "format": "GatedSpectrometer:1",
                    "ip": "225.0.1.184",
                    "port": "7152"
                }
            ],
                plot={"P0_ND_0": 121, "P0_ND_1": 121, "P1_ND_0": 122, "P1_ND_1": 122},# Dictionary of prefixes to plot  with values indicatin subplot to use, e.g.: plot": {"P0_ND_0": 0, "P0_ND_1": 0, "P1_ND_0": 1, "P1_ND_1": 1},
            nplot= 10.,       # update plot every 10 s

            default_hdf5_group_prefix="S",
            id="hdf5_writer", type="hdf5_writer"))
        self.mc_interface = []

        self.__periodic_callback = PeriodicCallback(self.periodic_sensor_update, 1000)
        self.__periodic_callback.start()

        self._output_file = None
        self.__measuring = False
        self.__plotting = False
        self.__data_snapshot = {}


        self._capture_threads = []

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

        self._current_file = Sensor.string(
            "current-file",
            description="Current filename.",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._current_file)

        self._current_file_size = Sensor.float(
            "current-file-size",
            description="Current filesize.",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._current_file_size)

        self._bandpass = Sensor.string(
            "bandpass",
            description="band-pass data (base64 encoded)",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._bandpass)




    @state_change(target="idle", allowed=["ready", "streaming", "deconfiguring"], intermediate="capture_stopping")
    @coroutine
    def capture_stop(self):
        _log.debug("Cleaning up capture threads")
        for t in self._capture_threads:
            t.stop()
        for t in self._capture_threads:
            t.join(10.0)
        self._capture_threads = []
        _log.debug("Capture threads cleaned")
        _log.debug("Stopping subprocess pool")


    @state_change(target="idle", intermediate="deconfiguring", error='panic')
    @coroutine
    def deconfigure(self):
        pass


    @state_change(target="configured", allowed=["idle"], intermediate="configuring")
    @coroutine
    def configure(self, config_json = '{}'):
        _log.info("Configuring HDF5 Writer")
        _log.debug("Configuration string: '{}'".format(config_json))

        yield self.set(config_json)

        cfs = json.dumps(self._config, indent=4)
        _log.info("Final configuration:\n" + cfs)

        #ToDo: allow streams with multiple multicast groups and multiple ports
        self.mc_subscriptions = {}

        for stream_description in value_list(self._config['input_data_streams']):
            hdf5_group = self._config["default_hdf5_group_prefix"]
            if "hdf5_group_prefix" in stream_description:
                hdf5_group = stream_description["hdf5_group_prefix"]
            if hdf5_group not in self.mc_subscriptions:
                self.mc_subscriptions[hdf5_group] = dict(groups=[], port=stream_description['port'], attributes={})
            self.mc_subscriptions[hdf5_group]['groups'].append(stream_description['ip'])
            if self.mc_subscriptions[hdf5_group]['port'] != stream_description['port']:
                raise RuntimeError("All input streams of one group have to use the same port!!!")

            for key in stream_description:
                if key in ["ip", "port"]:
                    continue
                self.mc_subscriptions[hdf5_group]['attributes'][key] = stream_description[key]
        _log.debug("Got {} subscription groups".format(len(self.mc_subscriptions)))
        self.__counter = 0
        yield self.plot()


    def _package_writer(self, data):
        if self._state == "measuring":
            _log.info('Writing data to section: {}'.format(data[0]))
            self._output_file.addData(data[0], data[1], )
        else:
            _log.debug("Not measuring, Dropping package")

        if self._config['plot']:
            if data[0] not in self.__data_snapshot:
                self.__data_snapshot[data[0]] = []

        self.__data_snapshot[data[0]].append(copy.deepcopy(data[1]))


    @coroutine
    def plot_wrapper(self):
        try:
            self.plot()
        except Excepton as E:
            _log.error("Error creating plot")
            _log.exception(E)

    @coroutine
    def plot(self):
        """

        """
        _log.error("Called plot")

        if self.__plotting:
            _log.warning("Previous plot not finished, dropping plot!")
            return
        self.__plotting = True

        _log.error("Obtain Lock ")

        with ProcessPoolExecutor(max_workers=1) as executor:
            p = executor.submit(plot_script, self.__data_snapshot, self._config['plot'])
            self.__data_snapshot = {}
            _log.debug("Waiting for subprocess to finish")

            try:
                plt = yield p
                _log.debug("Received {} bytes".format(len(plt)))
            except Exception as E:
                _log.error('Error communicating with subprocess:\n {}'.format(E))
                return
            _log.debug("Setting bandpass sensor with timestamp")
            self._bandpass.set_value(plt)
            _log.debug("Ready for next plot")

        self.__plotting = False


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


        self._capture_threads = []
        for hdf5_group_prefix, mcg in self.mc_subscriptions.items():
            spead_handler = GatedSpectrometerSpeadHandler(hdf5_group_prefix, mcg['attributes'])
            ct = SpeadCapture(mcg["groups"], mcg["port"],
                                               self._capture_interface,
                                               spead_handler, self._package_writer, affinity)
            ct.start()
            self._capture_threads.append(ct)

        _log.debug("Starting subprocess pool")
        if self._config['plot']:
            _log.debug("Starting Callback")
            self.__plot_callback = PeriodicCallback(self.plot_wrapper, self._config['nplot'] * 1000. )
            self.__plot_callback.start()

        _log.debug("Done capture starting!")



    @coroutine
    def periodic_sensor_update(self):
        """
        Updates the katcp sensors.

        This is a peiodic update as the connection are managed using threads and not coroutines.
        """
        timestamp = time.time()
        incomplete_heaps = 0
        complete_heaps = 0
        for t in self._capture_threads:
            incomplete_heaps += t._incomplete_heaps
            complete_heaps += t._complete_heaps

        conditional_update(self._incomplete_heaps, incomplete_heaps, timestamp=timestamp)
        conditional_update(self._complete_heaps, complete_heaps, timestamp=timestamp)

        if self._output_file:
            conditional_update(self._current_file_size, self._output_file.getFileSize(), timestamp=timestamp)



    @state_change(target="set", allowed=["ready", "measurement_starting", "configured", "streaming"], intermediate="measurement_preparing")
    def measurement_prepare(self, config={}):
        try:
            config = json.loads(config)
        except:
            _log.error("Cannot parse json:\n{}".format(config))
            raise RuntimeError("Cannot parse json.")

        if ("new_file" in config and config["new_file"]) or (not self._output_file):
            _log.debug("Creating new file")
            if "file_id" in config:
                file_id = config["file_id"]
            else:
                file_id = None
            self._output_file = EDDHDFFileWriter(path=self._config["output_directory"], file_id_no = file_id)
            self._current_file.set_value(self._output_file.filename)


        if ("override_newsubscan"  in config and config["override_newsubscan"]):
            _log.debug("Overriding new subscan creation")
        else:
            _log.debug("Creating new subscan")
            self._output_file.newSubscan()



    @state_change(target="measuring", allowed=["set", "ready", "measurement_preparing"], waitfor="set", intermediate="measurement_starting")
    @coroutine
    def measurement_start(self):
        _log.info("Starting file output")
        # The state measuring isw aht is checked,. This is enough, as also
        # error handling is done correctly


    @state_change(target="ready", allowed="measuring", intermediate="measurement_stopping")
    @coroutine
    def measurement_stop(self):
        _log.info("Stopping FITS interface data transfer")
        self._output_file.flush()
        # ToDo: There probably should be an output queue so that time ordering
        # is not becoming an issue





    @coroutine
    def stop(self):
        """
        Handle server stop. Stop all threads
        """
        try:
            for t in self._capture_threads:
                t.stop()
            for t in self._capture_interface:
                t.join(3.0)

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
        pool = spead2.MemoryPool(lower=16384, upper=((32*4*1024**2)+1024), max_free=64, initial=64)
        stream_config = spead2.recv.StreamConfig(bug_compat=spead2.BUG_COMPAT_PYSPEAD_0_5_2, max_heaps=64, memory_allocator=pool)
        ring_stream_config = spead2.recv.RingStreamConfig(heaps=64, contiguous_only = False)
        self.stream = spead2.recv.Stream(thread_pool, stream_config, ring_stream_config)

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

            if self._stop_event.is_set():
                _log.debug('Stop processing heaps from stream.')
                break




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
    def __init__(self, group_prefix="", attributes={}):

       # self.plottingQueue = queue.PriorityQueue()
        #self.__delay = delay
        self.__group_prefix = group_prefix
        self.__attributes = attributes

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
        section_id = self.__group_prefix + "{}_ND_{}".format(pol, nds)
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

        data['timestamp'] = np.array([reference_time])

        data['integration_time'] = np.array([number_of_input_samples / sampling_rate])
        data['saturated_samples'] = np.array([-42])

        return section_id, data, self.__attributes


if __name__ == "__main__":
    launchPipelineServer(EDDHDF5WriterPipeline)

