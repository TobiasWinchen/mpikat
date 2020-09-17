import json
import logging
from mpikat.core.scpi import ScpiAsyncDeviceServer, scpi_request, raise_or_ok, launch_server
import mpikat.effelsberg.edd.pipeline.EDDPipeline as EDDPipeline
from mpikat.effelsberg.edd.edd_server_product_controller import EddServerProductController
from mpikat.effelsberg.edd import EDDDataStore
import coloredlogs
from tornado.gen import Return, coroutine, sleep
import tornado
import signal

log = logging.getLogger('mpikat.edd_scpi_interface')



class EddScpiInterface(ScpiAsyncDeviceServer):
    def __init__(self, interface, port, master_ip, master_port, redis_ip, redis_port, scannum_check_period=1000,  ioloop=None):
        """
        @brief      A SCPI interface for a EddMasterController instance

        @param      master_controller_ip    IP of a master controll to send commands to
        @param      master_controller_port  Port of a master controll to send commands to
        @param      interface    The interface to listen on for SCPI commands
        @param      port        The port to listen on for SCPI commands
        @param      ioloop       The ioloop to use for async functions

        @note If no IOLoop instance is specified the current instance is used.
        """

        log.info("Listening at {}:{}".format(interface, port))
        log.info("Master at {}:{}".format(master_ip, master_port))
        log.info("Datastore at {}:{}".format(redis_ip, redis_port))
        super(EddScpiInterface, self).__init__(interface, port, ioloop)
        self.__controller = EddServerProductController("MASTER", master_ip, master_port)
        self.__eddDataStore = EDDDataStore.EDDDataStore(redis_ip, redis_port)

        #Periodicaly check scan number and send measurement prepare on change
        self._scannum_callback = tornado.ioloop.PeriodicCallback(self.__check_scannum, scannum_check_period)
        self._scannum_callback.start()
        self._last_scannum = None


    @coroutine
    def __check_scannum(self):
        """
        @brief check scan number
        """
        current_scan_number = self.__eddDataStore.getTelescopeDataItem("scannum")
        if not self._last_scannum:
            log.debug("First retrival of scannumbner, got {}".format(current_scan_number))
            self._last_scannum = current_scan_number
        elif self._last_scannum == current_scan_number:
            log.debug("Checking scan number {} == {}, doing nothing.".format(current_scan_number, self._last_scannum))
            pass
        else:
            log.debug("Scan number change detected from {} -> {}".format(self._last_scannum, current_scan_number))
            self._last_scannum = current_scan_number

            sourcename = self.__eddDataStore.getTelescopeDataItem("source-name")
            if sourcename.endswith("_R"):

                log.debug("Source ends with _R, enabling noise diode")
                cfg = {"set_noise_diode_firing_pattern": {"percentage":0.5, "period":1}}
            else:
                log.debug("Source ends not with _R, enabling noise diode")
                cfg = {"set_noise_diode_firing_pattern": {"percentage":0.0, "period":1}}
            self.__controller.measurement_prepare(cfg)


    @scpi_request()
    def request_edd_configure(self, req):
        """
        @brief      Configure the EDD backend

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:CONFIGURE'
        """

        @coroutine
        def wrapper():
            try:
                F = self.__controller.configure()

                @coroutine
                def cb(f):
                    yield self.__controller.capture_start()
                F.add_done_callback(cb)
                yield F
            except Exception as E:
                log.error(E)
                req.error(E)
            else:
                req.ok()
        self._ioloop.add_callback(wrapper)


    @scpi_request()
    def request_edd_deconfigure(self, req):
        """
        @brief      Deconfigure the EDD backend

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:DECONFIGURE'
        """
        self._ioloop.add_callback(self._make_coroutine_wrapper(req, self.__controller.deconfigure))


    @scpi_request()
    def request_edd_start(self, req):
        """
        @brief      Start the EDD backend processing

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:START'
        """
        self._ioloop.add_callback(self._make_coroutine_wrapper(req, self.__controller.measurement_start))


    @scpi_request()
    def request_edd_stop(self, req):
        """
        @brief      Stop the EDD backend processing

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:STOP'
        """
        self.__controller.measurement_stop()
        self._ioloop.add_callback(self._make_coroutine_wrapper(req, self.__controller.measurement_stop))


    @scpi_request(str, str)
    def request_edd_set(self, req, product_option, value):
        """
        @brief     Set an option for an edd backend component.

        @param      req   An ScpiRequst object

        @note       Suports SCPI request: 'EDD:SET ID:OPTION VALUE'
                    VALUE needs to be valid json, i.e. strings are marked with ""
        """
        log.debug(" Received {} {}".format(product_option, value))
        # Create nested option dict from colon seperateds tring
        try:
            d = {}
            g = d
            option_list = product_option.split(':')
            for el in option_list[:-1]:
                d[el] = {}
                d = d[el]
            d[option_list[-1]] = json.loads(value)
        except Exception as E:
            em = "Error parsing command: {} {}\n{}".format(product_option, value, E)
            log.error(em)
            req.error(em)
        else:
            log.debug(" - {}".format(g))
            self._ioloop.add_callback(self._make_coroutine_wrapper(req, self.__controller.set, g))


    @scpi_request(str)
    def request_edd_provision(self, req, message):
        self._ioloop.add_callback(self._make_coroutine_wrapper(req, self.__controller.provision, message))


    @scpi_request()
    def request_edd_deprovision(self, req):
        self._ioloop.add_callback(self._make_coroutine_wrapper(req, self.__controller.deprovision))


if __name__ == "__main__":

    parser = EDDPipeline.getArgumentParser()
    parser.add_argument('--master-controller-ip', dest='master_ip', type=str,
            default="edd01", help='The ip for the master controller')
    parser.add_argument('--master-controller-port', dest='master_port',
            type=int, default=7147, help='The port number for the master controller')
    parser.add_argument('--redis-ip', dest='redis_ip', type=str,
            default="localhost", help='The ip for the redis server')
    parser.add_argument('--redis-port', dest='redis_port', type=int,
            default=6379, help='The port number for the redis server')
    parser.add_argument('--scannum-check-period', dest='scannum_check_period',
            type=int, default=1000, help='Period [ms] between checks of changes of the scan number.')
    args = parser.parse_args()

    EDDPipeline.setup_logger(args)

    server = EddScpiInterface(args.host, args.port, args.master_ip, args.master_port, args.redis_ip, args.redis_port, args.scannum_check_period)
    #Scpi Server is not an EDDPipieline, but launcher work nevertheless
    EDDPipeline.launchPipelineServer(server, args)



