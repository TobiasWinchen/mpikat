"""
Interface and commandline application to katcp controlled digitizer/packetizers.
"""

from __future__ import print_function

import logging
import time
from tornado.gen import coroutine, sleep, Return
from tornado.ioloop import IOLoop
from katcp import KATCPClientResource

from mpikat.effelsberg.edd.EDDDataStore import EDDDataStore


# Conveniance settings of known packetizers. Should probably go to a config
# file
__known_packetizers = {"faraday_room":{"ip":"134.104.73.132","port":7147}, "focus_cabin":{"ip":"134.104.70.65","port":7147}}

_log = logging.getLogger("mpikat.edd_digpack_client")


class DigitiserPacketiserError(Exception):
    pass


class PacketiserInterfaceError(Exception):
    pass


class DigitiserPacketiserClient(object):

    def __init__(self, host, port=7147):
        """
        Wraps katcp commands to control a digitiser/packetiser.

        Args:
            host:  The host IP or name for the desired packetiser KATCP interface
            port:  The port number for the desired packetiser KATCP interface
        """
        self._host = host
        self._port = port
        self._client = KATCPClientResource(dict(
            name="digpack-client",
            address=(self._host, self._port),
            controlled=True))
        self._client.start()
        self._capture_started = False

        self._sampling_modes = {
            4096000000: ("virtex7_dk769b", "4.096GHz", 3),
            4000000000: ("virtex7_dk769b", "4.0GHz", 5),
            3600000000: ("virtex7_dk769b", "3.6GHz", 7),
            3520000000: ("virtex7_dk769b", "3.52GHz", 7),
            3500000000: ("virtex7_dk769b", "3.5GHz", 7),
            3200000000: ("virtex7_dk769b", "3.2GHz", 9),
            2600000000: ("virtex7_dk769b", "2.6GHz", 3),
            2560000000: ("virtex7_dk769b", "2.56GHz", 2)
            }


    def stop(self):
        self._client.stop()


    @coroutine
    def _safe_request(self, request_name, *args):
        """
        Send a request to client and prints response ok /  error message.

        Args:
            request_name: Name of the request
            *args:        Arguments passed to the request.
        """
        _log.info("Sending packetiser request '{}' with arguments {}".format(request_name, args))
        yield self._client.until_synced()
        response = yield self._client.req[request_name](*args)
        if not response.reply.reply_ok():
            _log.error("'{}' request failed with error: {}".format(
                request_name, response.reply.arguments[1]))
            raise DigitiserPacketiserError(response.reply.arguments[1])
        else:
            _log.debug("'{}' request successful".format(request_name))
            raise Return(response)


    @coroutine
    def _check_interfaces(self):
        """
        Check if interface of digitizer is in error state.
        """
        _log.debug("Checking status of 40 GbE interfaces")
        yield self._client.until_synced()

        @coroutine
        def _check_interface(name):
            _log.debug("Checking status of '{}'".format(name))
            sensor = self._client.sensor[
                'rxs_packetizer_40g_{}_am_lock_status'.format(name)]
            status = yield sensor.get_value()
            if not status == 0x0f:
                _log.warning("Interface '{}' in error state".format(name))
                raise PacketiserInterfaceError(
                    "40-GbE interface '{}' did not boot".format(name))
            else:
                _log.debug("Interface '{}' is healthy".format(name))
        yield _check_interface('iface00')
        yield _check_interface('iface01')


    @coroutine
    def set_predecimation(self, factor):
        """
        Set a predecimation factor for the packetizer - for e.g. factor=2 only every second sample is used.
        """
        allowedFactors = [1,2,4,8,16] # Eddy Nussbaum, private communication
        if factor not in allowedFactors:
            raise RuntimeError("predicimation factor {} not in allowed factors {}".format(factor, allowedFactors))
        yield self._safe_request("rxs_packetizer_edd_predecimation", factor)


    @coroutine
    def set_noise_diode_frequency(self, frequency):
        """
        Set noise diode frequency to given value.
        """
        if frequency == 0:
            yield self.set_noise_diode_firing_pattern(0.0, 0.0, "now")
        else:
            yield self.set_noise_diode_firing_pattern(0.5, 1./frequency, "now")


    @coroutine
    def set_noise_diode_firing_pattern(self, percentage, period, start="now"):
        """
        Set noise diode frequency to given value.

        Args:
            percentage: Percentage of period which the noise diode is turned on.
            period:     Period of fireing [s].
        """
        _log.debug("Set noise diode firing pattern")
        yield self._safe_request("noise_source", start, percentage, period)


    @coroutine
    def set_sampling_rate(self, rate, retries=3):
        """
        Sets the sampling rate.

        Args:
            rate:    The sampling rate in samples per second (e.g. 2.6 GHz should be passed as 2600000000.0)


        To allow time for reinitialisation of the packetiser firmware during this call we enforce a 10
        second sleep before the function returns.
        """

        try:
            args = self._sampling_modes[int(rate)]
        except KeyError as error:
            pos_freqs = "\n".join(["  - {} Hz ".format(f) for f in self._sampling_modes.keys()])
            error_msg = "Frequency {} Hz not in possible frequencies:\n{}".format(rate, pos_freqs)
            _log.error(error_msg)
            raise DigitiserPacketiserError(error_msg)

        attempts = 0
        while True:
            response = yield self._safe_request("rxs_packetizer_system_reinit", *args)
            yield sleep(20)
            try:
                yield self._check_interfaces()
            except PacketiserInterfaceError as error:
                if attempts >= retries:
                    raise error
                else:
                    _log.warning("Retrying system initalisation")
                    attempts += 1
                    continue
            else:
                break


    @coroutine
    def set_bit_width(self, nbits):
        """
        Sets the number of bits per sample out of the packetiser

        Args:
            nbits:  The desired number of bits per sample (e.g. 8 or 12)
        """
        valid_modes = {
            8: "edd08",
            10: "edd10",
            12: "edd12"
        }
        try:
            mode = valid_modes[int(nbits)]
        except KeyError as error:
            msg = "Invalid bit depth, valid bit depths are: {}".format(
                valid_modes.keys())
            _log.error(msg)
            raise DigitiserPacketiserError(msg)
        yield self._safe_request("rxs_packetizer_edd_switchmode", mode)


    @coroutine
    def flip_spectrum(self, flip):
        """
        Flip spectrum flip = True/False to adjust for even/odd nyquist zone
        """
        if flip:
            yield self._safe_request("rxs_packetizer_edd_flipsignalspectrum", "on")
        else:
            yield self._safe_request("rxs_packetizer_edd_flipsignalspectrum", "off")


    @coroutine
    def set_destinations(self, v_dest, h_dest):
        """
        Sets the multicast destinations for data out of the packetiser

        Args:
            v_dest:  The vertical polarisation channel destinations
            h_dest:  The horizontal polarisation channel destinations

        The destinations should be provided as composite stream definition
        strings, e.g. 225.0.0.152+3:7148 (this defines four multicast groups:
        225.0.0.152, 225.0.0.153, 225.0.0.154 and 225.0.0.155, all using
        port 7148). Currently the packetiser only accepts contiguous IP
        ranges for each set of destinations.
        """
        yield self._safe_request("capture_destination", "v", v_dest)
        yield self._safe_request("capture_destination", "h", h_dest)


    @coroutine
    def set_predecimation_factor(self, factor):
        """
        Sets the predecimation_factorfor data out of the packetiser

        Args:
            factor: (e.g. 1,2,4,8)

        """
        yield self._safe_request("rxs_packetizer_edd_predecimation", factor)


    @coroutine
    def set_flipsignalspectrum(self, value):
        """
        Sets the rxs-packetizer-edd-flipsignalspectrum data out of the packetiser

        Args:
            value: (e.g. 0, 1)

        """
        yield self._safe_request("rxs_packetizer_edd_flipsignalspectrum", value)


    @coroutine
    def set_interface_address(self, intf, ip):
        """
        Set the interface address for a packetiser qsfp interface

        Args:

            intf:   The interface specified as a string integer, e.g. '0' or '1'
            ip:     The IP address to assign to the interface
        """
        yield self._safe_request("rxs_packetizer_40g_source_ip_set", intf, ip)


    @coroutine
    def capture_start(self):
        """
        Start data transmission for both polarisation channels

        This method uses the packetisers 'capture-start' method which is an
        aggregate command that ensures all necessary flags on the packetiser
        and set for data transmission.  This includes the 1PPS flag required by
        the ROACH2 boards.
        """
        if not self._capture_started:
            """
            Only start capture once and not twice if received configure
            """
            self._capture_started = True
            yield self._safe_request("capture_start", "vh")


    @coroutine
    def configure(self, config):
        """
        Applying configuration recieved in dictionary
        """
        self._capture_started = False
        yield self._safe_request("capture_stop", "vh")
        yield self.set_sampling_rate(config["sampling_rate"])
        yield self.set_predecimation(config["predecimation_factor"])
        yield self.flip_spectrum(config["flip_spectrum"])
        yield self.set_bit_width(config["bit_width"])
        yield self.set_destinations(config["v_destinations"], config["h_destinations"])
        if "noise_diode_frequency" in config:
            yield self.set_noise_diode_frequency(config["noise_diode_frequency"])

        for interface, ip_address in config["interface_addresses"].items():
            yield self.set_interface_address(interface, ip_address)
        if "sync_time" in config:
            yield self.synchronize(config["sync_time"])
        else:
            yield self.synchronize()
        yield self.capture_start()


    @coroutine
    def deconfigure(self):
        """
        Deconfigure. Not doing anythin
        """
        raise Return()


    @coroutine
    def measurement_start(self):
        """
        """
        raise Return()


    @coroutine
    def measurement_stop(self):
        """
        """
        raise Return()


    @coroutine
    def measurement_prepare(self, config = {}):
        """
        """
        if "noise_diode_frequency" in config:
            yield self.set_noise_diode_frequency(config["noise_diode_frequency"])
        elif "noise_diode_pattern" in config:
            c = config["noise_diode_pattern"]
            yield self.set_noise_diode_firing_pattern(c["percentage"], c["period"])

        raise Return()


    @coroutine
    def capture_stop(self):
        """
        Stop data transmission for both polarisation channels
        """
        _log.warning("Not stopping data transmission")
        raise Return()
        #yield self._safe_request("capture_stop", "vh")


    @coroutine
    def get_sync_time(self):
        """
        Get the current packetiser synchronisation epoch

        Return:
            The synchronisation epoch as a unix time float
        """
        response = yield self._safe_request("rxs_packetizer_40g_get_zero_time")
        sync_epoch = float(response.informs[0].arguments[0])
        raise Return(sync_epoch)

    @coroutine
    def synchronize(self, unix_time=None):
        """
        Set the synchronisation epoch for the packetiser

        Args:
            unix_time:  The unix time to synchronise at. If no value is provided a
                               resonable value will be selected.

        When explicitly setting the synchronisation time it should be a second
        or two into the future allow enough time for communication with the
        packetiser. If the time is in the past by the time the request reaches
        the packetiser the next 1PPS tick will be selected.  Users *must* call
        get_sync_time to get the actual time that was set.  This call will
        block until the sync epoch has passed (i.e. if a sync epoch is chosen
        that is 10 second in the future, the call will block for 10 seconds).

        """
        if not unix_time:
            unix_time = round(time.time() + 2)
        yield self._safe_request("synchronise", 0, unix_time)
        sync_epoch = yield self.get_sync_time()
        if sync_epoch != unix_time:
            _log.warning("Requested sync time {} not equal to actual sync time {}".format(
                unix_time, sync_epoch))

    @coroutine
    def populate_data_store(self, host, port):
        """
        Populate the data store

        Args:
            host:     ip of the data store to use
            port:     port of the data store
        """
        _log.debug("Populate data store @ {}:{}".format(host, port))
        dataStore =  EDDDataStore(host, port)
        _log.debug("Adding output formats to known data formats")

        descr = {"description": "Digitizer/Packetizer spead. One heap per packet.",
                "ip": None,
                "port": None,
                "bit_depth" : None,                 # Dynamic Parameter
                "sample_rate" : None,
                "sync_time" : None,
                "samples_per_heap": 4096}

        dataStore.addDataFormatDefinition("MPIFR_EDD_Packetizer:1", descr)
        raise Return()




if __name__ == "__main__":
    import coloredlogs
    from argparse import ArgumentParser
    parser = ArgumentParser(description="Configures edd digitiezer. By default, send syncronize and capture start along with the given options.")
    parser.add_argument('host', type=str,
        help='Digitizer to bind to, either ip or one of [{}]'.format(", ".join(__known_packetizers)))
    parser.add_argument('-p', '--port', dest='port', type=int,
        help='Port number to bind to', default=7147)
    parser.add_argument('--nbits', dest='nbits', type=int,
        help='The number of bits per output sample')
    parser.add_argument('--sampling-rate', dest='sampling_rate', type=float,
        help='The digitiser sampling rate (Hz)')
    parser.add_argument('--v-destinations', dest='v_destinations', type=str,
        help='V polarisation destinations')
    parser.add_argument('--h-destinations', dest='h_destinations', type=str,
        help='H polarisation destinations')
    parser.add_argument('--log-level',dest='log_level',type=str,
        help='Logging level',default="INFO")
    parser.add_argument('--predecimation-factor', dest='predecimation_factor', type=int,
        help='Predecimation factor')
    parser.add_argument('--sync', dest='synchronize', action='store_true',
        help='Send sync command.')
    parser.add_argument('--no-capture-start', dest='capture_start', action='store_false',
        help='Do not send capture start command.')
    parser.add_argument('--sync-time', dest='sync_time', type=int,
        help='Use specified synctime, otherwise use current time')
    parser.add_argument('--noise-diode-frequency', dest='noise_diode_frequency', type=float,
        help='Set the noise diode frequency', default=-1)
    parser.add_argument('--noise-diode-pattern', dest='noise_diode_pattern', type=float, nargs=2,
        help='Set the noise diode pattern, percentage [0...1], perios [s]')

    parser.add_argument('--flip-spectrum', action="store_true", default=False, help="Flip the spectrum")
    args = parser.parse_args()

    if args.host in __known_packetizers:
        print("Found {} in known packetizers, use stored lookup ip and port.".format(args.host))
        args.port = __known_packetizers[args.host]['port']
        args.host = __known_packetizers[args.host]['ip']
    print("Configuring paketizer {}:{}".format(args.host, args.port))
    client = DigitiserPacketiserClient(args.host, port=args.port)

    logging.getLogger().addHandler(logging.NullHandler())
    _logger = logging.getLogger('mpikat')
    coloredlogs.install(
        fmt="[ %(levelname)s - %(asctime)s - %(name)s - %(filename)s:%(lineno)s] %(message)s",
        level=args.log_level.upper(),
        logger=_logger)

    actions = []
    if args.sampling_rate:
        actions.append((client.set_sampling_rate, dict(rate=args.sampling_rate)))
    if args.nbits:
        actions.append((client.set_bit_width, dict(nbits=args.nbits)))
    if args.v_destinations:
        actions.append((client.set_destinations, dict(v_dest=args.v_destinations, h_dest=args.h_destinations)))
    if args.predecimation_factor:
        actions.append((client.set_predecimation, dict(factor=args.predecimation_factor)))
    # Always flip spectrum to either on or off
    actions.append((client.flip_spectrum, dict(flip=args.flip_spectrum)))
    if args.noise_diode_frequency >= 0.:
        actions.append((client.set_noise_diode_frequency, dict(frequency=args.noise_diode_frequency)))
    if args.noise_diode_pattern:
        actions.append((client.set_noise_diode_firing_pattern, dict(percentage=args.noise_diode_pattern[0], period=args.noise_diode_pattern[1])))

    # Sync + capture start should come last
    if args.synchronize:
            actions.append((client.synchronize, dict(unix_time=args.sync_time)))
    if args.capture_start:
        actions.append((client.capture_start, {}))

    @coroutine
    def perform_actions():
        for action, params in actions:
            yield action(**params)
    ioloop = IOLoop.current()

    ioloop.run_sync(perform_actions)
