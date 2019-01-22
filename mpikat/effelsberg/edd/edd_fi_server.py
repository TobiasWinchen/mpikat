
#start and stop commands implemented and it works.

import tornado
import logging
import signal
import socket
import Queue
import time
import errno
import threading
import coloredlogs
from threading import Thread
from threading import Event
from optparse import OptionParser
from time import sleep
import numpy as np
import struct
from katcp import AsyncDeviceServer, Sensor, ProtocolFlags, AsyncReply
from katcp.kattypes import (Str, Int, request, return_reply)



log = logging.getLogger("mpikat.edd_fi_server")

data_Queue = Queue.Queue()

class FitsInterfaceServer(AsyncDeviceServer):
    """
    Class providing an interface between EDD processes and the
    Effelsberg FITS writer
    """
    VERSION_INFO = ("edd-fi-server-api", 1, 0)
    BUILD_INFO = ("edd-fi-server-implementation", 0, 1, "")
    DEVICE_STATUSES = ["ok", "degraded", "fail"]
    PROTOCOL_INFO = ProtocolFlags(5, 0, set([
        ProtocolFlags.MULTI_CLIENT,
        ProtocolFlags.MESSAGE_IDS,
    ]))

    def __init__(self, interface, port, capture_interface, capture_port):
        """
        @brief Initialization of the FitsInterfaceServer object

        @param ip       Interface address to serve on
        @param port     Port number to serve on
        """
        self._configured = False
        self._no_active_beams = None
        self._no_channels = None
        self._integ_time = None
        self._blank_phase = None
        self._capture_interface = capture_interface
        self._capture_port = capture_port
        self._capture_thread = None
        super(FitsInterfaceServer, self).__init__(interface, port)

    def start(self):
        """
        @brief   Start the server
        """
        super(FitsInterfaceServer, self).start()

    @property
    def nbeams(self):
        return self._active_beams_sensor.value()

    @nbeams.setter
    def nbeams(self, value):
        self._active_beams_sensor.set_value(value)

    @property
    def nchannels(self):
        return self._nchannels_sensor.value()

    @nchannels.setter
    def nchannels(self, value):
        self._nchannels_sensor.set_value(value)

    @property
    def integration_time(self):
        return self._integration_time_sensor.value()

    @integration_time.setter
    def integration_time(self, value):
        self._integration_time_sensor.set_value(value)

    @property
    def nblank_phases(self):
        return self._nblank_phases_sensor.value()

    @nblank_phases.setter
    def nblank_phases(self, value):
        self._nblank_phases_sensor.set_value(value)

    def setup_sensors(self):
        """
        @brief   Setup monitoring sensors
        """
        self._device_status_sensor = Sensor.discrete(
            "device-status",
            description="Health status of FIServer",
            params=self.DEVICE_STATUSES,
            default="ok",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._device_status_sensor)
        self._active_beams_sensor = Sensor.float(
            "nbeams",
            description="Number of beams that are currently active",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._active_beams_sensor)
        self._nchannels_sensor = Sensor.float(
            "nchannels",
            description="Number of channels in each beam",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._nchannels_sensor)
        self._integration_time_sensor = Sensor.float(
            "integration-time",
            description="The integration time for each beam",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._integration_time_sensor)
        self._nblank_phases_sensor = Sensor.integer(
            "nblank-phases",
            description="The number of blank phases",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._nblank_phases_sensor)

    def _stop_capture(self):
        log.debug("Cleaning up capture thread")
        if self._capture_thread:
            self._capture_thread.stop()
            self._capture_thread.join()
            self._capture_thread = None
        log.debug("Capture thread cleaned")

    @request(Int(), Int(), Int(), Int())
    @return_reply()
    def request_configure(self, req, beams, channels, int_time, blank_phases):
        """
        @brief    Configure the FITS interface server

        @param   beams          The number of beams expected
        @param   channels       The number of channels expected
        @param   int_time       The integration time (seconds)
        @param   blank_phases   The number of blank phases (1-4)
        """
        self.nbeams = beams
        self.nchannels = channels
        self.integration_time = int_time
        self.nblank_phases = blank_phases
        self._stop_capture()
        self._configured = True
        return ("ok",)

    @request()
    @return_reply()
    def request_start(self, req):
        """
        @brief    Start the FITS interface server capturing data
        """
        if not self._configured:
            msg = "FITS interface server is not configured"
            log.error(msg)
            return ("fail", msg)
        self._stop_capture()
        self._capture_thread = CaptureData(self._capture_interface,
            self._capture_port, 4 * (self.nchannels + 2))
        self._capture_thread.start()
        return ("ok",)

    @request()
    @return_reply()
    def request_stop(self, req):
        """
        @brief    Stop the FITS interface server capturing data
        """
        if not self._configured:
            msg = "FITS interface server is not configured"
            log.error(msg)
            return ("fail", msg)
        self._stop_capture()
        return ("ok",)


class CaptureData(Thread):
    """
    @brief     Captures formatted data from a UDP socket
    """
    def __init__(self, ip, port, buffer_size, name="CaptureData"):
        Thread.__init__(self, name=name)
        self._address = (ip, port)
        self._buffer_size = buffer_size
        self._stop_event = Event()
        self._aggregator = AggregateData(2)

    def _reset_socket(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.setblocking(False)
        self._socket.bind(self._address)

    def stop(self):
        self._stop_event.set()

    def _flush(self):
        log.debug("Flushing capture socket")
        flush_count = 0
        while True:
            try:
                message, addr = self._socket.recvfrom(self._buffer_size)
                flush_count += 1
            except:
                break
        log.debug("Flushed {} messages".format(flush_count))

    def _capture(self):
        log.debug("Starting data capture")
        while not self._stop_event.is_set():
            try:
                data, addr = self._socket.recvfrom(self._buffer_size)
                log.debug("Received {} byte message from {}".format(len(data), addr))
                self._aggregator.start_aggregating(data)
            except socket.error as error:
                error_id = error.args[0]
                if error_id == errno.EAGAIN or error_id == errno.EWOULDBLOCK:
                    sleep(0.01)
                    continue
                else:
                    raise error
        log.debug("Stopping data capture")

    def run(self):
        self._reset_socket()
        try:
            self._flush()
            self._capture()
        except Exception as error:
            log.exception("Error during capture: {}".format(str(error)))
        finally:
            self._socket.close()


class AggregateData(object):
    """
    @brief Aggregates spectrometer data from polarization channel 1 and 2 for the given time
           before sending to the fits writer
    """
    def __init__(self, no_streams, name="AggregateDate"):
        self._no_streams = no_streams
        self._data_stream = []
        self._count = 0
        self._ref_seq_no = 0
        self._time_info = ""

    def phase_extract(self, num):
        mask = 0xf0000000
        phase = (num&mask) >> 28
        if (phase == 0): phase = 4
        return phase

    def pol_extract(self, num):
        mask = 0x0f000000
        pol = (num&mask) >> 24
        return pol

    def extract_sequence_num(self, num1, num2):
        counter_num = struct.pack(">II", num1,num2)
        unpack_num= np.zeros(1,dtype=np.int64)
        unpack_num = struct.unpack(">q", counter_num)
        mask = 0x00ffffffffffffff
        seq_no = unpack_num[0]&mask
        return seq_no

    #ISO time definition
    def isotime(self, s):
        ms = int(10000*(s - int(s)))
        t = time.gmtime(s)
        dateTime = str("%4.4i-%2.2i-%2.2iT%2.2i:%2.2i:%2.2i.%4.4iUTC " % (t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec, ms))
        return dateTime

    def start_data_aggregation(self, data_to_process):
        self._count += 1
        data = np.zeros(2050,dtype=np.uint32)
        data = struct.unpack('>2050I', data_to_process)
        phase = self.phase_extract(data[0])
        polID = self.pol_extract(data[0])
        sequence_num = self.extract_sequence_num(data[0],data[1])
        print "Extracted phase: ", phase,
        print "Extracted polID: ", polID,
        print "Extracted seq. no.:: ", sequence_num
        #Capturing logic based on sequence no.
        if (self._count ==1):
            self._time_info = self.isotime(time.time())
            self._data_stream = [data[2:]]
            self._ref_seq_no = sequence_num
        #TODO include time stamp in the queue
        elif ((sequence_num == (self._ref_seq_no+1)) or (sequence_num == (self._ref_seq_no-1))):
            self._data_stream.append(data[2:])
            data_Queue.put((self._time_info, self._no_streams, self._data_stream))
            self._count = 0
            self._data_stream = []
        else:
            print "packet missing for the given stamp.."
            self._count = 0
            self._data_stream = []
      #  strm, data_from_queue = data_Queue.get()
      #  print "from queue:                  ", len(data_from_queue)

    def stop_data_aggregation(self):
        self._count = 0
        self._data_stream = []


class SendToFW(Thread):
    """
    @brief The data is formated with fits header and sent to the fits writer
    """
    def __init__(self, server_ip, tcp_port, name="SendToFW"):
        Thread.__init__(self, name=name)
        self._serverAddr = (server_ip, tcp_port)
        self._serverSoc = self._server_socket()
        self._tcpSoc = self._tcp_data_socket()
        self._time_stamp = ""
        self._integ_time = 16
        self._blank_phase = 1
        self._no_streams = 0
        self._sending_stop_event = Event()
        self._sending_stop_event.set()

    def _server_socket(self):
        self._serverSoc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._serverSoc.bind((server_ip, tcp_port))
        self._serverSoc.listen(1)
        self._serverSoc.settimeout(5)
        return self._serverSoc

    def _tcp_data_socket(self):
        self._tcpSoc, addr = self._serverSoc.accept()
        return self._tcpSoc

    def start_sending_toFW(self):
        self._sending_stop_event.clear()
        self._tcpSoc, addr = self._FW_IP()

    def stop_sending_FW(self):
        self._sending_stop_event.set()
        self._tcpSoc.shutdown(socket.SHUT_RDWR)
        self._tcpSoc.close()
        del self._tcpSoc

    def pack_FI_metadata(self):
        #IEEE - big endian, EEEI - little endian
        headerData = ['EEEI']
        data_type = '<4s'

        #Data format of channel data: 'F' - Float
        channelDataType = 'F   '
        headerData.append(channelDataType)
        data_type += '4s'

        #Length of the data package
        lengthHeader = struct.calcsize('4s4si8s28siiii')
        lengthSecHeaders = struct.calcsize('ii')*self._no_active_beams
        lengthChannelData = struct.calcsize('f')*self._no_channels*self._no_active_beams
        dataPackageLength = lengthHeader+lengthSecHeaders+lengthChannelData
        headerData.append(dataPackageLength)
        data_type += 'l'

        #Backend name
        BEName = 'EDD     '
        headerData.append(BEName)
        data_type += '8s'

       #Time info
        headerData.append(self._time_stamp)
        data_type += '28s'

       #Integration time
        headerData.append(self._integ_time)
        data_type += 'l'

        #Phase number
        headerData.append(self._blank_phase)
        data_type += 'l'

        #Number of Back End Sections
        headerData.append(self._no_streams)
        data_type += 'l'

    def pack_FI_data(self, data):
        data_type = ''
        eddData = []
        dataPointer = 0

        for BESecIndex in range(self._no_active_beams):
            BESecNum = BESecIndex+1
            data_type += 'l'
            eddData.append(BESecNum)
            data_type += 'l'
            eddData.append(self._no_channels)
            #data_type += '1024f'
            data_type += str('%sf' % self._no_channels)

            eddData.extend(channelData[dataPointer:dataPointer+channels])
            dataPointer += self._no_channels

        return(data_type, eddData)

    def pack_tcpData(self, dataType, data):
        packer = struct.Struct(dataType)
        packed_data = packer.pack(*data)
        return packed_data

    def pack_data(self):
        self._time_stamp, self._no_streams, data_from_queue = data_Queue.get()
        #self._time_stamp, self._no_streams, data_from_queue = data_Queue.get()
        header_format, header_data = _pack_FI_metadata()
        data_format, pol_data = self.pack_FI_data(data_from_queue)
        tcp_data_format = header_format + data_format
        header_data.extend(pol_data)
        tcp_data = header_data
        data_to_send = self.pack_tcpData(tcp_data_format, tcp_data)
        return data_to_send

    def run(self):
        while True:
            if not self._sending_stop_event.is_set():
               try:
                #pack data
                data_to_fw = self.pack_data()
                #send data
                self._tcpSoc.send(data_to_fw)
               except socket.error as error:
                   error_id = error.args[0]
                   if error_id == errno.EAGAIN or error_id == errno.EWOULDBLOCK:
                       raise
                   return

@tornado.gen.coroutine
def on_shutdown(ioloop, server):
    print('Shutting down')
    yield server.stop()
    ioloop.stop()


def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('', '--host', dest='host', type=str,
        help='Host interface to bind to', default='127.0.0.1')
    parser.add_option('-p', '--port', dest='port', type=long,
        help='Port number to bind to', default=5000)
    parser.add_option('', '--cap-ip', dest='cap_ip', type=str,
        help='Host interface to bind to for data capture', default='127.0.0.1')
    parser.add_option('', '--cap-port', dest='cap_port', type=long,
        help='Port number to bind to for data capture', default=5001)
    parser.add_option('', '--log-level', dest='log_level', type=str,
        help='Defauly logging level', default="INFO")
    (opts, args) = parser.parse_args()
    logging.getLogger().addHandler(logging.NullHandler())
    coloredlogs.install(
        fmt="[ %(levelname)s - %(asctime)s - %(name)s - %(filename)s:%(lineno)s] %(message)s",
        level=opts.log_level.upper(),
        logger=log)
    log.setLevel(opts.log_level.upper())
    ioloop = tornado.ioloop.IOLoop.current()
    server = FitsInterfaceServer(opts.host, opts.port, opts.cap_ip, opts.cap_port)
    # Hook up to SIGINT so that ctrl-C results in a clean shutdown
    signal.signal(signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
        on_shutdown, ioloop, server))
    def start_and_display():
        server.start()
        log.info("Listening at {0}, Ctrl-C to terminate server".format(server.bind_address))
    ioloop.add_callback(start_and_display)
    #ioloop.add_callback(server.start)
    ioloop.start()

if __name__ == "__main__":
    main()
