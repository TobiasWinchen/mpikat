import logging
import time
import json
from astropy.time import Time
import tornado
import coloredlogs
import signal
import astropy.units as units
from optparse import OptionParser
from tornado.gen import coroutine
from katcp import AsyncDeviceServer, Message, Sensor, AsyncReply, KATCPClientResource
from katcp.kattypes import request, return_reply, Str
from mpikat.effelsberg.edd.edd_server_product_controller import EddServerProductController
from mpikat.effelsberg.status_server import JsonStatusServer

log = logging.getLogger("mpikat.effelsberg.edd.pipeline")
log.setLevel('DEBUG')
class Effelsberg_metadata_listener(AsyncDeviceServer):
    """
    @brief Interface object which accepts KATCP commands

    """
    VERSION_INFO = ("mpikat-edd-api", 1, 0)
    BUILD_INFO = ("mpikat-edd-implementation", 0, 1, "rc1")
    DEVICE_STATUSES = ["ok", "degraded", "fail"]

    def __init__(self, ip, port, master_ip, master_port):
        """
        @brief Initialization of the Effelsberg_metadata_listener object

        @param ip       IP address of the server
        @param port     port of the EddCommander

        """
        super(Effelsberg_metadata_listener, self).__init__(ip, port)
        self._managed_sensors = []
        self.callbacks = set()
        self._status_server = JsonStatusServer(ip, 9999)
        self._master_controller = EddServerProductController("MASTER", master_ip, master_port)
        self._scannum_callback = tornado.ioloop.PeriodicCallback(
            self.check_scannum, 1000)
        self._scannum_callback.start()
        self._scannum = None
        self._scannum_now = None
	self._first_scannum = True

    @coroutine
    def check_scannum(self):
        """
        @brief check scan number every x ms

        """
	status_dict = json.loads(self._status_server.as_json())
        self._scannum_new = status_dict["scannum"]
	if self._scannum_new == self._scannum:
            pass
        else:
            self._scannum = self._scannum_new
            json_string = json.dumps({"source_config": {"source-name": "{}".format(status_dict["source-name"]), "nchannels": 1024, "nbins": 1024, "ra": status_dict["ra"], "dec": status_dict["dec"]}})
	    if self._first_scannum != True:
		log.info("Sending {} to the EDD master contoller".format(json_string))
		self._master_controller.measurement_prepare(json_string)
	    self._first_scannum = False

    @coroutine
    def start(self):
        super(Effelsberg_metadata_listener, self).start()
	self._status_server.start()
    @coroutine
    def stop(self):
        yield super(Effelsberg_metadata_listener, self).stop()

    def setup_sensors(self):
        """
        @brief Setup monitoring sensors
        """
        self._device_status = Sensor.discrete(
            "device-status",
            "Health status of Effelsberg_metadata_listener",
            params=self.DEVICE_STATUSES,
            default="ok",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._device_status)

@coroutine
def on_shutdown(ioloop, server):
    log.info('Shutting down server')
    yield server.stop()
    ioloop.stop()


def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-H', '--host', dest='host', type=str,
                      help='Host interface to bind to', default="127.0.0.1")
    parser.add_option('-p', '--port', dest='port', type=long,
                      help='Port number to bind to', default=5000)
    parser.add_option('', '--log_level', dest='log_level', type=str,
                      help='logging level', default="INFO")
    # parser.add_option('-H', '--target-host', dest='target-host', type=str,
    #                  help='Host interface to bind to', default="127.0.0.1")
    # parser.add_option('-p', '--target-port', dest='target-port', type=long,
    #                  help='Port number to bind to', default=5000)
    (opts, args) = parser.parse_args()
    logging.getLogger().addHandler(logging.NullHandler())
    logger = logging.getLogger('mpikat')
    coloredlogs.install(
        fmt="[ %(levelname)s - %(asctime)s - %(name)s - %(filename)s:%(lineno)s] %(message)s",
        level=opts.log_level.upper(),
        logger=logger)
    logging.getLogger('katcp').setLevel('INFO')
    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting EddCommander instance")
    server = Effelsberg_metadata_listener(opts.host, opts.port, "0,0.0.0", 0)
    signal.signal(signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
        on_shutdown, ioloop, server))

    def start_and_display():
        server.start()
        log.info(
            "Listening at {0}, Ctrl-C to terminate server".format(server.bind_address))

    ioloop.add_callback(start_and_display)
    ioloop.start()

if __name__ == "__main__":
    main()

