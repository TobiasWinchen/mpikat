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

import mpikat.effelsberg.edd.pipeline.EDDPipeline as EDDPipeline
from mpikat.effelsberg.edd.edd_server_product_controller import EddServerProductController
from mpikat.effelsberg.edd import EDDDataStore

log = logging.getLogger("mpikat.effelsberg.edd.pipeline")
log.setLevel('DEBUG')

class Effelsberg_metadata_listener(EDDPipeline.EDDPipeline):
    """
    @brief Watches the scan number. Sends a measurement prepare to the amster
    controller when  san number changes.

    """

    def __init__(self, ip, port, master_ip, master_port):
        """
        @brief Initialization of the Effelsberg_metadata_listener object

        @params  ip           The IP address on which the server should listen
        @params  port         The port that the server should bind to
        @params  master_ip     IP of the master controller to connect to
        @params  master_port   Port for the conenction to the asmter controller
        """
        super(Effelsberg_metadata_listener, self).__init__(ip, port)

        self._master_controller = EddServerProductController("MASTER", master_ip, master_port)

        self._scannum_callback = tornado.ioloop.PeriodicCallback(self.check_scannum, 1000)

        self._last_scannum = None

    @coroutine
    def check_scannum(self):
        """
        @brief check scan number every x ms

        """
        current_scan_number = self.__eddDataStore.getTelescopeDataItem("scannum")
        if self._last_scannum == current_scan_number:
            pass
        else:
            log.debug("Scan number change detected from {} -> {}".format(self._last_scannum, current_scan_number))
            self._last_scannum = current_scan_number
            self._master_controller.measurement_prepare()


    @coroutine
    def configure(self, cfg):
        yield self.set(config_json)
        cfs = json.dumps(self._config, indent=4)

        # The master contoller provides the data store IP as default gloal
        # config to all pipelines
        self.__eddDataStore = EDDDataStore.EDDDataStore(self._config["data_store"]["ip"], self._config["data_store"]["port"])

    @coroutine
    def capture_start(self):
        self._scannum_callback.start()

    @coroutine
    def capture_stop(self):
        self._scannum_callback.stop()



if __name__ == "__main__":
    main()

