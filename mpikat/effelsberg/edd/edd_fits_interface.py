"""
Pipeline to send spectrometer data to the Effelsberg fits writer (APEX Fits writer).
"""

from __future__ import print_function, division, unicode_literals
from tornado.gen import coroutine, sleep
from tornado.ioloop import PeriodicCallback
import logging
import socket
import time
import errno
import numpy as np
import ctypes
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

from mpikat.effelsberg.edd.pipeline.EDDPipeline import EDDPipeline, launchPipelineServer, state_change
import mpikat.utils.numa as numa

log = logging.getLogger("mpikat.edd_fits_interface")


# Artificial time delta between noise diode on / off status
_NOISEDIODETIMEDELTA = 0.001


def conditional_update(sensor, value, status=1, timestamp=None):
    """
    Update a sensor if the value has changed
    """
    if sensor.value() != value:
        sensor.set_value(value, status, timestamp=timestamp)


class FitsWriterConnectionManager(Thread):
    """
    Manages TCP connections to the APEX FITS writer.

    This class implements a TCP/IP server that can accept connections from
    the FITS writer. Upon acceptance, the new communication socket is stored
    and packages from a local queue are streamed to the fits writer.

    Packages are delayed by (at default) 3s, as the pacakges might be out of
    time-order, and the fits writer expects to receive the packages in strict
    order.
    """
    def __init__(self, ip, port, delay=3):
        """
        Args:
            ip:    The IP address to accept connectons from.
            port:  The port to serve on
            delay: Number of seconds a package is kept in the queue before submission.
        """
        Thread.__init__(self)
        self._address = (ip, port)
        self._shutdown = Event()
        self._has_connection = Event()
        self._is_measuring = Event()

        self.__output_queue = queue.PriorityQueue()
        self.__delay = delay
        self.__latestPackageTime = 0
        self.send_items = 0
        self.dropped_items = 0

        self._transmit_socket = None
        log.debug("Creating the FITS writer TCP listening socket")
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.setsockopt(
            socket.SOL_TCP, socket.TCP_NODELAY, 1)
        self._server_socket.setblocking(False)
        log.debug("Binding to {}".format(self._address))
        self._server_socket.bind(self._address)
        self._server_socket.listen(1)


    def _accept_connection(self):
        log.debug("Accepting connections on FW server socket")
        while not self._shutdown.is_set():
            try:
                transmit_socket, addr = self._server_socket.accept()
                self._has_connection.set()
                log.info("Received connection from {}".format(addr))
                self.send_items = 0
                self.dropped_items = 0
                return transmit_socket
            except socket.error as error:
                error_id = error.args[0]
                if error_id == errno.EAGAIN or error_id == errno.EWOULDBLOCK:
                    time.sleep(1)
                    continue
                else:
                    log.exception(
                        "Unexpected error on socket accept: {}".format( str(error)))
                    raise error


    def drop_connection(self):
        """
        Drop any current FITS writer connection. The
        FitsWriterConnectionManager will be ready for new connections
        afterwards,
        """
        self._has_connection.clear()
        if not self._transmit_socket is None:
            log.debug("Dropping connection")
            try:
                self._transmit_socket.shutdown(socket.SHUT_RDWR)
            except Exception as E:
                log.debug("Error shuting down socket - just dropping ")
            try:
                self._transmit_socket.close()
            except Exception as E:
                log.error("Error closing socket.")
                log.exception(E)
            self._transmit_socket = None


    def put(self, pkg):
        """
        Put a new output pkg on the local queue if there is a fits server
        connection. Drop packages otherwise.
        """
        if self._is_measuring.is_set():
            self.__output_queue.put([pkg.timestamp, pkg, time.time()])
        else:
            log.info("Not measuring dropping package.")



    def send_data(self):
        """
        Send all packages in the queue older than delay seconds.
        """
        now = time.time()
        log.debug('Send data, Queue size: {}'.format(self.__output_queue.qsize()))

        while not self.__output_queue.empty():
            ref_time, pkg, timestamp = self.__output_queue.get(timeout=1)
            self.__output_queue.task_done()
            if self._is_measuring.is_set():
                if (now - timestamp) < self.__delay:
                    # Pkg. too young, there might be others. Put back and
                    # return.
                    # But only during measurement - if no measurement, just
                    # empty queue asap, as there cant be other packages.
                    self.__output_queue.put([ref_time, pkg, timestamp])
                    return
            else:
                log.debug("Flushing queue. Current size: {}".format(self.__output_queue.qsize()))
            if ref_time < self.__latestPackageTime:
                log.warning("Package with ref_time {} in queue, but previously send {}. Dropping package.".format(ref_time,  self.__latestPackageTime))
                self.dropped_items += 1
            else:
                log.info('Send item {} with reference time {}. Fits timestamp: {}'.format(self.send_items, time.ctime(timestamp), pkg.timestamp))
                self.__latestPackageTime = ref_time
                self._transmit_socket.send(bytearray(pkg))
                self.send_items += 1


    def queue_is_empty(self):
        return self.__output_queue.empty()


    def clear_queue(self):
        """
        Empty queue without sending
        """
        self.send_items = 0
        self.dropped_items = 0
        while not self.__output_queue.empty():
            self.__output_queue.get(timeout=1)
            self.__output_queue.task_done()


    def stop(self):
        """
        Stop the server
        """
        self._shutdown.set()


    def run(self):
        """
        main loop of the server. The server will wait for a FITS writer
        conenction and send any data as long as the connection exists. If
        connection drops, it wwill wait for a new connection.
        """
        while not self._shutdown.is_set():
            if not self._has_connection.is_set():
                self.drop_connection()  # Just in case
                try:
                    self._transmit_socket = self._accept_connection()
                except Exception as error:
                    self.drop_connection()
                    log.exception(str(error))
                    continue
            try:
                self.send_data()
            except Exception as error:
                log.error("Exception during send data. Dropping connection, emtying queue")
                log.exception(str(error))
                self.drop_connection()
                self._is_measuring.clear()
                self.clear_queue()
                continue

            time.sleep(0.1)

        self.drop_connection()
        self._server_socket.close()



class FitsInterfaceServer(EDDPipeline):
    """
    Interface of EDD pipeliens to the Effelsberg FITS writer.

    Configuration
    -------------

    fits_writer_ip: "0.0.0.0"   - IP to accept Fitw Writer Connections from.
    fits_writer_port: 5002      - Port to listen for fits writer connections.
    drop_nans: True             - If true, any spectra containing a  NaM or Inf value will be dropped.


    """
    VERSION_INFO = ("spead-edd-fi-server-api", 1, 0)
    BUILD_INFO = ("spead-edd-fi-server-implementation", 0, 1, "")

    def __init__(self, ip, port):
        """
        Args:
            ip:   IP accepting katcp control connections from.
            port: Port number to serve on
        """
        EDDPipeline.__init__(self, ip, port, dict(input_data_streams=[],
            id="fits_interface", type="fits_interface", drop_nans=True,
            fits_writer_ip="0.0.0.0", fits_writer_port=5002))
        self._fw_connection_manager = None
        self._capture_thread = None
        self._shutdown = False
        self.mc_interface = []

        self.__periodic_callback = PeriodicCallback(self.periodic_sensor_update, 1000)
        self.__periodic_callback.start()

        self.__bandpass_callback = PeriodicCallback(self.bandpassplotter , 10000)
        self.__bandpass_callback.start()
        self.__plotting = False

    def setup_sensors(self):
        """
        Setup monitoring sensors
        """
        EDDPipeline.setup_sensors(self)

        self._fw_connection_status = Sensor.discrete(
            "fits-writer-connection-status",
            description="Status of the fits writer conenction",
            params=["Unmanaged", "Connected", "Unconnected" ],
            default="Unmanaged",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._fw_connection_status)

        self._fw_packages_sent = Sensor.integer(
            "fw-sent-packages",
            description="Number of packages sent to fits writer in this measurement",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._fw_packages_sent)

        self._fw_packages_dropped = Sensor.integer(
            "fw-dropped-packages",
            description="Number of packages dropped by fits writer in this measurement",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._fw_packages_dropped)

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

        self._invalid_packages = Sensor.integer(
            "invalid-packages",
            description="Number of invalid packages dropped.",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._invalid_packages)

        self._bandpass = Sensor.string(
            "bandpass",
            description="band-pass data (base64 encoded)",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._bandpass)



    @state_change(target="idle", allowed=["streaming", "deconfiguring"], intermediate="capture_stopping")
    @coroutine
    def capture_stop(self):
        if self._capture_thread:
            log.debug("Cleaning up capture thread")
            self._capture_thread.stop()
            self._capture_thread.join()
            self._capture_thread = None
            log.debug("Capture thread cleaned")
        if self._fw_connection_manager:
            self._fw_connection_manager.stop()
            self._fw_connection_manager.join()
            self._fw_connection_manager = None
            log.debug("Connection manager thread cleaned")


    @state_change(target="idle", intermediate="deconfiguring", error='panic')
    @coroutine
    def deconfigure(self):
        pass


    @state_change(target="configured", allowed=["idle"], intermediate="configuring")
    @coroutine
    def configure(self, config_json):
        """
        """
        log.info("Configuring Fits interface")
        log.debug("Configuration string: '{}'".format(config_json))

        yield self.set(config_json)

        cfs = json.dumps(self._config, indent=4)
        log.info("Final configuration:\n" + cfs)

        #ToDo: allow streams with multiple multicast groups and multiple ports
        self.mc_interface = []
        self.mc_port = None
        for stream_description in self._config['input_data_streams']:
            self.mc_interface.append(stream_description['ip'])
            if self.mc_port is None:
                self.mc_port = stream_description['port']
            else:
                if self.mc_port != stream_description['port']:
                    raise RuntimeError("All input streams have to use the same port!!!")

        if self._fw_connection_manager is not None:
            log.warning("Replacing fw_connection manager")
            self._fw_connection_manager.stop()
            self._fw_connection_manager.join()

        self._fw_connection_manager = FitsWriterConnectionManager(self._config["fits_writer_ip"], self._config["fits_writer_port"])
        self._fw_connection_manager.start()


    @state_change(target="ready", allowed=["configured"], intermediate="capture_starting")
    @coroutine
    def capture_start(self):
        """
        """
        log.info("Starting FITS interface capture")
        nic_name, nic_description = numa.getFastestNic()
        self._capture_interface = nic_description['ip']
        log.info("Capturing on interface {}, ip: {}, speed: {} Mbit/s".format(nic_name, nic_description['ip'], nic_description['speed']))
        affinity = numa.getInfo()[nic_description['node']]['cores']

        handler = GatedSpectrometerSpeadHandler(self._fw_connection_manager, len(self._config['input_data_streams']), drop_invalid_packages=self._config['drop_nans'])
        self._capture_thread = SpeadCapture(self.mc_interface,
                                           self.mc_port,
                                           self._capture_interface,
                                           handler, affinity)
        self._capture_thread.start()

    @coroutine
    def periodic_sensor_update(self):
        """
        Updates the katcp sensors.

        This is a peiodic update as the connection are managed using threads and not coroutines.
        """
        timestamp = time.time()
        if not self._fw_connection_manager:
            conditional_update(self._fw_connection_status, "Unmanaged", timestamp=timestamp)
        elif not self._fw_connection_manager._has_connection.is_set():
            conditional_update(self._fw_connection_status, "Unconnected", timestamp=timestamp)
        else:
            conditional_update(self._fw_connection_status, "Connected", timestamp=timestamp)
            conditional_update(self._fw_packages_sent, self._fw_connection_manager.send_items, timestamp=timestamp)
            conditional_update(self._fw_packages_dropped, self._fw_connection_manager.dropped_items, timestamp=timestamp)
        if self._capture_thread:
            conditional_update(self._incomplete_heaps, self._capture_thread._incomplete_heaps, timestamp=timestamp)
            conditional_update(self._complete_heaps, self._capture_thread._complete_heaps, timestamp=timestamp)
            conditional_update(self._invalid_packages, self._capture_thread._handler.invalidPackages, timestamp=timestamp)

    @coroutine
    def bandpassplotter(self):
        log.debug("Starting periodic plot of bandpass")
        try:
            yield self.bandpassplotterwrapper()
        except Exception as E:
            logging.error(E)

    @coroutine
    def bandpassplotterwrapper(self):
        pks = []

        plottingQueue = self._capture_thread._handler.plottingQueue

        found_pair = []
        log.debug(" Checking pairs of spectra ...")

        while not plottingQueue.empty():
            ref_time, pkg = plottingQueue.get(timeout=1)
            plottingQueue.task_done()
            if found_pair:
                # continue clearing queue
                continue
            pks.append([ref_time, pkg])
            for t, pkg2 in pks:
                dt = ref_time - t
                if abs(abs(dt) - _NOISEDIODETIMEDELTA) < _NOISEDIODETIMEDELTA / 2:
                    log.debug(" Matching pair found after looking at {} packages, dt: {}".format(len(pks), dt))
                    found_pair = [pkg, pkg2, min(ref_time, t)]
                    break

        if not found_pair:
            log.warning("No matching set of pkgs found to create bandpass plot!")
            for x in pks:
                plottingQueue.put(x)
            return
        log.debug("Creating plot with matching pair")

        if self.__plotting:
            log.warning("Previous plot not finished, dropping plot!")
            return
        self.__plotting = True

        def plot_script(conn, pk1, pk2, ndisplay_channels=1024):
            """
            PLot script creating a band pass plot. To be executed using the multiprocessing module.

            Args:
                conn:     multiprocessing conenction object to which the output plot is written.
                pk1, pk2: Fits writer packages to be plotted.
                ndisplay_channels: Number of channels to be plotted.
            """
            import matplotlib as mpl
            mpl.use('Agg')
            import numpy as np
            import pylab as plt

            import sys
            if sys.version_info[0] >=3:
                import io
            else:
                import cStringIO as io

            import base64
            mpl.rcParams.update(mpl.rcParamsDefault)
            mpl.use('Agg')

            nsections = pk1.nsections
            nchannels = pk1.sections[0].nchannels

            figsize = {2: (8, 4), 4: (8, 8)}
            fig, subs = plt.subplots(nsections // 2, 2, figsize=figsize[nsections])

            labels = {2: ["PSD [dB]", 'PSd [dB]'], 4: ["I [dB]", "Q/I", "U/I", "V/I"]}
            D = np.zeros([2, nsections, ndisplay_channels])
            if nsections == 2:
                D[pk1.blank_phases-1][0] = 10 * np.log10(np.ctypeslib.as_array(pk1.sections[0].data).reshape([ndisplay_channels, nchannels // ndisplay_channels]).sum(axis=1))
                D[pk1.blank_phases-1][1] = 10 * np.log10(np.ctypeslib.as_array(pk1.sections[1].data).reshape([ndisplay_channels, nchannels // ndisplay_channels]).sum(axis=1))
                D[pk2.blank_phases-1][0] = 10 * np.log10(np.ctypeslib.as_array(pk2.sections[0].data).reshape([ndisplay_channels, nchannels // ndisplay_channels]).sum(axis=1))
                D[pk2.blank_phases-1][1] = 10 * np.log10(np.ctypeslib.as_array(pk2.sections[1].data).reshape([ndisplay_channels, nchannels // ndisplay_channels]).sum(axis=1))
            elif nsections == 4:
                D[pk1.blank_phases-1][0] = np.ctypeslib.as_array(pk1.sections[0].data).reshape([ndisplay_channels, nchannels // ndisplay_channels]).sum(axis=1)
                D[pk2.blank_phases-1][0] = np.ctypeslib.as_array(pk2.sections[0].data).reshape([ndisplay_channels, nchannels // ndisplay_channels]).sum(axis=1)
                for i in range(1,4):
                    D[pk1.blank_phases-1][i] = np.ctypeslib.as_array(pk1.sections[i].data).reshape([ndisplay_channels, nchannels // ndisplay_channels]).sum(axis=1) / D[pk1.blank_phases-1][0]
                    D[pk2.blank_phases-1][i] = np.ctypeslib.as_array(pk2.sections[i].data).reshape([ndisplay_channels, nchannels // ndisplay_channels]).sum(axis=1) / D[pk2.blank_phases-1][0]
                D[0][0] = 10 * np.log10(D[0][0])
                D[1][0] = 10 * np.log10(D[1][0])

            for i,s  in enumerate(subs.flat):
                if np.isfinite(D[0][i]).all():
                    s.plot(D[0][i], label = "BS Phase 1", c='C0')
                if np.isfinite(D[1][i]).all():
                    s.plot(D[1][i], label = "BS Phase 2", c='C1')
                s.set_xlabel('Channel')
                s.set_ylabel(labels[nsections][i])
                s.legend(fontsize='x-small', loc='upper right',  ncol =2)
            #bbox_anchor = {2: (0, -.15), 4: (0, -.15)}
            #s.legend(fontsize='x-small', loc='upper right', bbox_to_anchor=bbox_anchor[nsections], ncol =2)
            fig.suptitle('{} / {}'.format(pk1.timestamp, pk2.timestamp))
            fig.tight_layout(rect=[0, 0.03, 1, 0.95])
            fig_buffer = io.StringIO()
            fig.savefig(fig_buffer, format='png')
            fig_buffer.seek(0)
            b64 = base64.b64encode(fig_buffer.read())
            conn.send(b64)
            conn.close()


        log.debug("Create pipe")
        parent_conn, child_conn = Pipe()
        log.debug("Preparing Subprocess")
        p = Process(target=plot_script, args=(child_conn, found_pair[0].fw_pkt, found_pair[1].fw_pkt,))
        log.debug("Starting Subprocess")
        p.start()
        log.debug("Waiting for subprocess to finish")
        while p.is_alive():
            log.debug("Still waiting ...")
            yield sleep(0.5)
        #p.join()
        log.debug("Receiving data")
        if p.exitcode !=0:
            log.error('Subprocess returned with exitcode: {}'.format(p.exitcode))
        else:
            log.debug('Subprocess exitcode: {}'.format(p.exitcode))

        try:
            plt = parent_conn.recv()
            log.debug("Received {} bytes".format(len(plt)))
        except Exception as E:
            log.error('Error communicating with subprocess:\n {}'.format(E))
            return
        log.debug("Setting bandpass sensor with timestamp")
        self._bandpass.set_value(plt, timestamp=found_pair[2])
        log.debug("Ready for next plot")
        self.__plotting = False



    @state_change(target="measuring",  intermediate="measurement_starting")
    @coroutine
    def measurement_start(self):
        log.info("Starting FITS interface data transfer as soon as connection is established ...")
        self._fw_connection_manager.clear_queue()
        self._fw_connection_manager._is_measuring.set()


    @state_change(target="ready", intermediate="measurement_stopping")
    @coroutine
    def measurement_stop(self):
        log.info("Stopping FITS interface data transfer")
        yield self._fw_connection_manager._is_measuring.clear()
        counter = 0
        log.info("Waiting for emting output queue")
        while not self._fw_connection_manager.queue_is_empty():
            if counter > 150:
                log.warning("Queue not empties after flush!")
                self._fw_connection_manager.clear_queue()
                break
            yield sleep(0.1)
            counter += 1
        yield sleep(0.1)
        log.info("Dropping connection.")
        yield self._fw_connection_manager.drop_connection()


    @coroutine
    def stop(self):
        """
        Handle server stop. Stop all threads
        """
        try:
            if self._fw_connection_manager:
                self._fw_connection_manager.stop()
                self._fw_connection_manager.join(3.0)
            if self._capture_thread:
                self._capture_thread.stop()
                self._capture_thread.join(3.0)
        except Exception as E:
            log.error("Exception during stop! {}".format(E))
        super(FitsInterfaceServer, self).stop()



class SpeadCapture(Thread):
    """
    Captures heaps from one or more streams that are transmitted in SPEAD
    format and call handler on completed heaps.
    """
    def __init__(self, mc_ip, mc_port, capture_ip, speadhandler, numa_affinity = []):
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
        self._handler = speadhandler

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
        self._handler.stop = True
        self.stream.stop()
        self._stop_event.set()


    def run(self):
        """
        Subscribe to MC groups and start processing.
        """
        log.debug("Subscribe to multicast groups:")
        for i, mg in enumerate(self._mc_ip):
            log.debug(" - Subs {}: ip: {}, port: {}".format(i,self._mc_ip[i], self._mc_port ))
            self.stream.add_udp_reader(mg, int(self._mc_port), max_size = 9200,
                buffer_size=1073741820, interface_address=self._capture_ip)

        log.debug("Start processing heaps:")
        self._nheaps = 0
        self._incomplete_heaps = 0
        self._complete_heaps = 0

        for heap in self.stream:
            self._nheaps += 1
            log.debug('Received heap {} - previously {} completed, {} incompleted'.format(self._nheaps, self._complete_heaps, self._incomplete_heaps))
            if isinstance(heap, spead2.recv.IncompleteHeap):
                log.warning('Received incomplete heap - received only {} / {} bytes.'.format(heap.received_length, heap.heap_length))
                self._incomplete_heaps += 1
                continue
            else:
                self._complete_heaps += 1
            try:
                self._handler(heap)
            except Exception as E:
                log.error("Error handling heap. Cought exception:\n{}".format(E))



def fw_factory(nsections, nchannels, data_type = "EEEI", channel_data_type = "F   "):
    """
    Creates APEX fits writer compatible packages.

    Args:
      nsections (int):             Number of sections.
      nchannels (int):             Number of channels per section.
      data_type (str):             Type of data - Only 'EEEI' is supported right now.
      channel_data_type (str):     Type of channel data  - Only 'F    ' is supported right now.

    References:
    .. [HAFOK_2018] ]H. Hafok, D. Muders and M. Olberg, 2018, APEX SCPI socket command syntax and backend data stream format, APEX-MPI-ICD-0005 https://www.mpifr-bonn.mpg.de/5274578/APEX-MPI-ICD-0005-R1_1.pdf
    """
    class FWData(ctypes.LittleEndianStructure):
        _pack_ = 1
        _fields_ = [
            ('section_id', ctypes.c_uint32),
            ('nchannels', ctypes.c_uint32),
            ('data', ctypes.c_float * nchannels)
        ]

    class FWPacket(ctypes.LittleEndianStructure):
        _pack_ = 1
        _fields_ = [
            ("data_type", ctypes.c_char * 4),               #     0 -  3
            ("channel_data_type", ctypes.c_char * 4),       #     4 -  7
            ("packet_size", ctypes.c_uint32),               #     8 - 11
            ("backend_name", ctypes.c_char * 8),            #    12 - 19
            ("timestamp", ctypes.c_char * 28),              #    20 - 48
            ("integration_time", ctypes.c_uint32),          #    49 - 53
            ("blank_phases", ctypes.c_uint32),              #    54 - 57 alternate between 1 and 2 cal on or off
            ("nsections", ctypes.c_uint32),                 #    57 - 61 number of sections
            ("blocking_factor", ctypes.c_uint32),           #    61-63 spectra per section ?
            ("sections", FWData * nsections)                #    actual section data
        ]

    packet = FWPacket()

    packet.packet_size = ctypes.sizeof(packet)

    if channel_data_type != "F   ":
         raise NotADirectoryError("cannot handle backend data format {}".format(channel_data_type))
    packet.channel_data_type = channel_data_type

    if data_type != "EEEI":
         raise NotADirectoryError("cannot handle numerical encoding standard {}".format(data_type))
    packet.data_type = data_type

    packet.nsections = nsections

    # Hard coded for effelsberg ??
    packet.blocking_factor = 1
    for ii in range(nsections):
        packet.sections[ii].section_id = ii + 1
        packet.sections[ii].nchannels = nchannels
        ctypes.addressof(packet.sections[ii].data), 0, ctypes.sizeof(packet.sections[ii].data)

    return packet



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
    Parse heaps of gated spectrometer output and aggregate items with matching
    polarizations.

    Heaps above a max age will be dropped.
    Complete packages are passed to the fits interface queue
    number of input streams tro handle. half per gate.
    """
    def __init__(self, fits_interface, number_input_streams, max_age = 10, drop_invalid_packages=True):

        self.__max_age = max_age
        self.__fits_interface = fits_interface
        self.__drop_invalid_packages = drop_invalid_packages
        self.invalidPackages = 0        # count invalid packages

        self.plottingQueue = queue.PriorityQueue()

        #Description of heap items
        self.ig = spead2.ItemGroup()
        self.ig.add_item(5632, "timestamp_count", "", (6,), dtype=">u1")
        self.ig.add_item(5633, "polarization", "", (6,), dtype=">u1")
        self.ig.add_item(5634, "noise_diode_status", "", (6,), dtype=">u1")
        self.ig.add_item(5635, "fft_length", "", (6,), dtype=">u1")
        self.ig.add_item(5636, "number_of_input_samples", "", (6,), dtype=">u1")
        self.ig.add_item(5637, "sync_time", "", (6,), dtype=">u1")
        self.ig.add_item(5638, "sampling_rate", "", (6,), dtype=">u1")
        self.ig.add_item(5639, "naccumulate", "", (6,), dtype=">u1")

        # Queue for aggregated objects
        self.__packages_in_preparation = {}
        self.__number_of_input_streams = number_input_streams

        # Now is the latest checked package
        self.__now = 0

        self.__bandpass = None


    def __call__(self, heap):
        """
        handle heaps. Merge polarization heaps with matching timestamps and pass to output queue.
        """
        log.debug("Unpacking heap")
        items = self.ig.update(heap)
        if 'data' not in items:
            # On first heap only, get number of channels
            fft_length = convert48_64(items['fft_length'].value)
            self.nchannels = int((fft_length/2)+1)
            log.debug("First item - setting data length to {} channels".format(self.nchannels))
            self.ig.add_item(5640, "data", "", (self.nchannels,), dtype="<f")
            # Reprocess heap to get data
            items = self.ig.update(heap)

        if len(items.keys()) != len(self.ig.items()):
            missing_keys = []
            for key in self.ig.items():
                if key[0] not in items:
                    missing_keys.append(key[0])
            log.warning("Received invalid heap, containing only {} / {} keys. Missign keys:\n {}".format(len(items.keys()), len(self.ig.items()), " \n".join(missing_keys)))
            return


        class SpeadPacket:
            """
            Contains the items as members with conversion
            """
            def __init__(self, items):
                for item in items.values():
                    if item.name != 'data':
                        setattr(self, item.name, convert48_64(item.value))
                    else:
                        setattr(self, item.name, item.value)


        class PrepPack:
            """
            Package in preparation.
            """
            def __init__(self, number_of_spectra, nchannels):
                self._nspectra = number_of_spectra
                self.fw_pkt = fw_factory(number_of_spectra, nchannels)  # Actual package for fits writer
                self.counter = 0    # Counts spectra set in fw_pkg
                self.valid = True   # Invalid package will not be send to fits writer
                self.number_of_input_samples = 0

            def addSpectrum(self, packet):
                """
                Includes packet with spead spectrum into the fw output package
                """
                self.counter += 1
                log.debug("   This is {} / {} parts for reference_time: {:.3f}".format(pp.counter , self._nspectra, packet.reference_time))

                # Copy data and drop DC channel - Direct numpy copy behaves weired as memory alignment is expected
                # but ctypes may not be aligned
                sec_id = packet.polarization

                log.debug("   Data (first 5 ch., including DC): {}".format(packet.data[:5]))
                packet.data /= (packet.number_of_input_samples + 1E-30)
                self.number_of_input_samples = packet.number_of_input_samples
                log.debug("                After normalization: {}".format(packet.data[:5]))
                if np.isnan(packet.data).any():
                    log.debug("Invalidate package due to NaN detected")
                    self.valid = False
                #ToDo: calculation of offsets without magic numbers
                ctypes.memmove(ctypes.byref(self.fw_pkt.sections[int(sec_id)].data), packet.data.ctypes.data + 4,
                        packet.data.size * 4 - 4)

                if self.counter == self._nspectra:
                    # Fill header fields + output data
                    log.debug("Got all parts for reference_time {:.3f} - Finalizing".format(packet.reference_time))

                    # Convert timestamp to datetimestring in UTC
                    def local_to_utc(t):
                        secs = time.mktime(t)
                        return time.gmtime(secs)
                    dto = datetime.fromtimestamp(packet.reference_time)
                    timestamp = time.strftime('%Y-%m-%dT%H:%M:%S', local_to_utc(dto.timetuple() ))

                    # Blank should be at the end according to specification
                    timestamp += ".{:04d}UTC ".format(int((float(packet.reference_time) - int(packet.reference_time)) * 10000))
                    self.fw_pkt.timestamp = timestamp
                    log.debug("   Calculated timestamp for fits: {}".format(self.fw_pkt.timestamp))

                    integration_time = packet.number_of_input_samples / float(packet.sampling_rate)
                    log.debug("   Integration period: {} s".format(packet.integration_period))
                    log.debug("   Received samples in period: {}".format(packet.number_of_input_samples))
                    log.debug("   Integration time: {} s".format(integration_time))

                    self.fw_pkt.backend_name = "EDDSPEAD"
                    # As the data is normalized to the actual nyumber of samples, the
                    # integration time for the fits writer corresponds to the nominal
                    # time.
                    self.fw_pkt.integration_time = int(packet.integration_period * 1000) # in ms

                    self.fw_pkt.blank_phases = int(2 - packet.noise_diode_status)
                    log.debug("   Noise diode status: {}, Blank phase: {}".format(packet.noise_diode_status, self.fw_pkt.blank_phases))



        packet = SpeadPacket(items)

        log.debug("Aggregating data for timestamp {}, Noise diode: {}, Polarization: {}".format(packet.timestamp_count, packet.noise_diode_status, packet.polarization))

        # Integration period does not contain efficiency of sampling as heaps may
        # be lost respectively not in this gate
        packet.integration_period = (packet.naccumulate * packet.fft_length) / float(packet.sampling_rate)

        # The reference time is in the center of the integration # period
        packet.reference_time = float(packet.sync_time) + float(packet.timestamp_count) / float(packet.sampling_rate) + float(packet.integration_period/ 2.)
        # packets with noise diode on are required to arrive at different time
        # than off
        if(packet.noise_diode_status == 1):
            packet.reference_time += _NOISEDIODETIMEDELTA 

        # Update local time
        if packet.reference_time > self.__now:
            self.__now = packet.reference_time

        if packet.reference_time not in self.__packages_in_preparation:
            pp = PrepPack(self.__number_of_input_streams // 2, int(self.nchannels) - 1)
        else:
            pp = self.__packages_in_preparation.pop(packet.reference_time)

        pp.addSpectrum(packet)

        if pp.counter == self.__number_of_input_streams // 2:

            self.plottingQueue.put([packet.reference_time, pp])

            # package is done. Send or drop
            if self.__drop_invalid_packages and not pp.valid:
                log.warning("Package for reference time {} dropped because it contains NaN)!".format(packet.reference_time))
                self.invalidPackages += 1
            else:
                self.__fits_interface.put(pp.fw_pkt)
        else:
            # Package not done, (re-)add to preparation stash
            self.__packages_in_preparation[packet.reference_time] = pp

            # Cleanup old packages
            tooold_packages = []
            log.debug('Checking {} packages for age restriction'.format(len(self.__packages_in_preparation)))
            for p in self.__packages_in_preparation:
                age = self.__now - p
                #log.debug(" Package with timestamp {}: now: {} age: {}".format(p, self.__now, age) )
                if age > self.__max_age:
                    log.warning("   Age of package {} exceeded maximum age {} - Incomplete package will be dropped.".format(age, self.__max_age))
                    tooold_packages.append(p)
            for p in tooold_packages:
                self.__packages_in_preparation.pop(p)


if __name__ == "__main__":
    launchPipelineServer(FitsInterfaceServer)

