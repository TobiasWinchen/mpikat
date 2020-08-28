"""
Copyright (c) 2020 Tobias Winchen <twinchen@mpifr-bonn.mpg.de>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from __future__ import print_function, division, unicode_literals

from mpikat.utils.ip_utils import split_ipstring, is_validat_multicast_range

from tornado.gen import coroutine, sleep
from tornado.ioloop import IOLoop
import casperfpga
import logging

DEFAULTFIRMWARE="s_ubb_64ch_codd_2020-07-31_1348.fpg"

log = logging.getLogger("mpikat.effelsberg.edd.edd_skarab_client")

class SkarabInterface(object):
    """
    Interface to skarab FPGAs via casperfpga but with asyncio.
    """
    def __init__(self, ip, port, firmwarefile=None):
        """ing Skarab instance")
 
        ip and port of the FPGA board to connect to.
        """
        log.debug("Starting Skarab instance")
        self.__ip = ip
        self.__port = port
        self._firmwarefile = firmwarefile
        self._client = None

    def setFirmware(self, firmwarefile):
        self._firmwarefile = firmwarefile

    @coroutine
    def connect(self):
        """
        Connects to a client. Raises runtime error after timeout.
        """
        log.debug("Connecting to FPGA {}:{}".format(self.__ip, self.__port))
        self._client = casperfpga.CasperFpga(self.__ip, self.__port)
        info = self._client.transport.get_skarab_version_info()
        log.debug("Succcessfully Connected to FPGA - Retrieved Skarab info:" + "\n".join(["    -  {}: {}".format(k,v) for k,v in info.iteritems()]))


    def is_connected(self):
        """
        Returns true if connection to skarab exists and is active
        """
        if self._client:
            return self._client.is_connected()
        else:
            return False

    @coroutine
    def program(self):
        """
        Programs FPGA with the chosen firmware
        """
        log.debug("Loading firmware from {} ... ". format(self._firmwarefile))

        #log.warning("NOT LOADING FIRMWARE DUE TO (Hard-Coded) TEST MODE")
        #res = True
        res = self._client.transport.upload_to_ram_and_program(self._firmwarefile)
        if not res:
            raise RuntimeError("Error loading firmware: {}. Result: {}".format(self._firmwarefile, res))
        log.debug("Done loading firmware {}". format(self._firmwarefile))


    @coroutine
    def initialize(self):
        """
        Connect to FPGA and try to read system information. Reprograms FPGA if this fails.
        """
        yield self.connect()
        try:
            self._client.get_system_information(self._firmwarefile)
        except:
            log.error('Error getting system information for firmware {} - reprogramming '.format(self._firmwarefile))
            yield self.connect()
            yield self.program()

            self._client.get_system_information(self._firmwarefile)



class SkarabChannelizerClient(SkarabInterface):
    """
    Client accessingd a Skarab wih channelizers firmware [REFERENCE TO PROEJCT HERE]

    The interface wraps all register oprations in  high-level function calls,
    so that the user can be mostly agnostic about the inner workings on the
    firmware.
    """
    def __init__(self, ip, port, firmwarefile="s_ubb_64ch_codd_2020-07-31_1348.fpg"):
        """
        @brief Creates client, does NOT connect

        @param ip
        @param port
        @param firmwarefile
        """
        SkarabInterface.__init__(self, ip, port, firmwarefile)


    @coroutine
    def configure_inputs(self, ips0, ips1, port=None):
        """
        Does multicast group subscription. ip0, ip1 are of format
        225.0.0.152+3 to give .152, .153, .154, .155
        """
        rx_core0_name = 'gbe0'
        rx_core1_name = 'gbe1'
        log.debug('Subscribing to multicast groups ...')

        ip0, N0, p0 = split_ipstring(ips0)
        ip1, N1, p1 = split_ipstring(ips1)
        if not is_validat_multicast_range(ip0, N0, p0):
            raise RuntimeError("Invalid multicast range {}".format(ips0))
        if not is_validat_multicast_range(ip1, N1, p1):
            raise RuntimeError("Invalid multicast range {}".format(ips1))
        if not N0 == N1:
            raise RuntimeError("Multicast range of both interfaces have to match {} {}".format(ips0, ips1))
        if not port and p1:
            port = p1


        log.debug(" - set IGMP version to 2")
        self._client.set_igmp_version("2")

        mask = casperfpga.gbe.IpAddress("255.255.255.252")
        ip0 = casperfpga.gbe.IpAddress(ip0)

        log.debug(" - configure gbe0 to {} with mask {}".format(ip0.ip_str, mask.ip_str))
        self._client.transport.multicast_receive("gbe0", ip0, mask, 1)
        self._client.gbes.gbe0.set_port(port)

        ip1 = casperfpga.gbe.IpAddress(ip1)
        log.debug(" - configure gbe1 to {} with mask {}".format(ip1.ip_str, mask.ip_str))
        self._client.transport.multicast_receive("gbe1", ip1, mask, 2)
        self._client.gbes.gbe1.set_port(port)





    @coroutine
    def generate_snapshots(self, size=4096):
        """
        @brief Generates ADC snapshot data

        @params size
        """
        log.debug('Getting ADC snapshot')

        self._client.registers.control.write(adc_snap_arm='pulse')
        self._client.write_int('adc0_ctrl', 0x00)
        self._client.write_int('adc0_ctrl', 0x01)
        self._client.write_int('adc1_ctrl', 0x00)
        self._client.write_int('adc1_ctrl', 0x01)

        fmt = '>{}b'.format(size)
        log.debug(' byte format: {}'.format(fmt))

        _adc0_snap = np.asarray(struct.unpack(fmt, self._client.read('adc0_bram', size)))
        _adc1_snap = np.asarray(struct.unpack(fmt, self._client.read('adc1_bram', size)))

        timestamp = fpga.read_int('adc0_val')
        if timestamp < 0:
            timestamp = timestamp + 2**32
        log.debug(' timestamp: {}'.format(timestamp))

        return (timestamp, _adc0_snap, _adc1_snap)


    @coroutine
    def configure_output(self, dest_ip, dest_port, number_of_groups=8, channels_per_group=8, board_id=07):
        """
        @brief      Configur skarab output

        @param      dest_ip
        @param      dest_port
        @param      number_of_groups
        @param      channels_per_group
        @param      board_id            Board ID [0 .. 255] inserted into the spead header
        """
        log.debug('Configuring ouput')
        log.debug("  - Staring ip: {}".format(dest_ip))
        log.debug("  - Port: {}".format(dest_port))
        ip = casperfpga.gbe.IpAddress(dest_ip)
        log.debug("  - Packed ip: {}".format(ip.ip_int))
        self._client.write_int('iptx_base', ip.ip_int)

        # 3 hex numbers of one byte, defining number of frequency channels in each ouput MC,
        # numper of output MC, and a magic number 8
        log.debug("  - Number of groups: {}".format(number_of_groups))
        log.debug("  - Channels per group: {}".format(channels_per_group))
        x_setup = (channels_per_group << 16) + (number_of_groups << 8) + 8
        log.debug("  - x_setup: {}".format(hex(x_setup)))

        self._client.write_int('x_setup', x_setup)

        # tx_meta consists of 24 bits for the port and on byte for the board_id
        # tx_meta = 0xEA6107 :: PORT=EA61 (60001), BOARDID=07
        tx_meta =  ((dest_port << 8) + board_id)
        self._client.write_int('tx_metadata',tx_meta)
        log.debug("  - Board id: {}".format(hex(board_id)))
        log.debug("  - tx_meta: {}".format(hex(tx_meta)))


    @coroutine
    def capture_start(self):
        log.debug("Starting capture ...")
        self._client.registers.control.write(gbe_txen=True)


    @coroutine
    def capture_stop(self):
        log.debug("Stopping capture ...")
        self._client.registers.control.write(gbe_txen=False)


    @coroutine
    def get_fpga_clock(self):
        clk = round(self._client.estimate_fpga_clock())
        log.debug("FPGA running at {}".format(clk))
        return clk


    @coroutine
    def read_output_config(self):
        """
        Reads destination IP and port from the firmware register
        """
        log.debug("Rading dstination ip:")
        d_ip = self._client.read_int('iptx_base') + 2**32 # Convert to unsigned int
        log.debug("  - packed ip: {}".format(d_ip))
        ip = casperfpga.gbe.IpAddress(d_ip)

        tx_meta = self._client.read_int('tx_metadata')
        log.debug("  - tx_meta: {}".format(hex(tx_meta)))
        dest_port = (tx_meta >> 8) & 0xFFFF
        board_id = tx_meta & 0xFF
        return (ip.ip_str, dest_port, board_id)


    @coroutine
    def configure_quantization_factor(self, quant_factor):
        """
        Writes quantization factor value in the firmware register
        """
        self._client.write_int('quant_coeff',quant_factor)
        yield sleep(0.5)
        quant_val = self._client.read_int('quant_coeff')
        if quant_factor != quant_val:
            raise RuntimeError("Error setting quantization factor {}".format(hex(quant_factor)))


    @coroutine
    def configure_fft_shift(self, fft_shift):
        """
        Writes FFT Shift 
        """
        self._client.write_int('fft_shift',fft_shift)
        yield sleep(0.5)
        ffs = self._client.read_int('fft_shift')
        if ffs != fft_shift:
            raise RuntimeError("Error setting fft_shift {}".format(hex(ffs)))


    @coroutine
    def reset_registers(self):
        """
        Reset all internal registers to intial state.
        """
        self._client.write_int("control",0) # gbe disable
        self._client.registers.control.write(auto_rst_enable=True)
        self._client.registers.control.write(sys_rst='pulse') #256: sys_rst, 511 mrst pulse
        yield sleep(1)

#    @coroutine
#    def program(self):
#        yield SkarabInterface.program(self)
#        yield self.reset_registers()

#
#@coroutine
#def main():
#    port = 7147
#    ip ="10.10.1.62"
#    skarab = SkarabChannelizer(ip, port)
#    yield skarab.initialize()
#    #yield skarab.subscribe_multicast_groups("225.0.0.152", "225.0.0.156", 7148)
#    yield skarab.configure_output("225.0.1.111", 1230)
#    D = yield skarab.read_output_config()
#    yield skarab.capture_start()
#    print(D)
#    #S = yield skarab.generate_snapshots() 
#    #print(S)
#
#
#    #yield skarab.program(FIRMWARE)
#
#########################################################################
## Casper FPGA IP Config
##
##def ips(v):
##    ip = casperfpga.gbe.IpAddress("225.0.5.152")
##    return ip.ip2str(v)
##
##ip = casperfpga.gbe.IpAddress("225.0.0.152")
##mask = casperfpga.gbe.IpAddress("0.0.0.255")
##
##ip_high = ip.ip_int >> 16
##ip_low = ip.ip_int & (2 ** 16 - 1)
##mask_high = mask.ip_int >> 16
##mask_low = mask.ip_int & (2 ** 16 - 1)
##
##print("High {}".format(ips(ip_high)))
##print("Low {}".format(ips(ip_low)))
##
##print("mask High {}".format(ips(mask_high)))
##print("mask Low {}".format(ips(mask_low)))
#
#io_loop = ioloop.IOLoop.current()
#log.info("Starting ioloop")
#io_loop.run_sync(main)

if __name__ == "__main__":
    import coloredlogs
    from argparse import ArgumentParser
    parser = ArgumentParser(description="Configures skarab board")
    parser.add_argument('host', type=str,
        help='IP of Skarab board to bind to')
    parser.add_argument('-p', '--port', dest='port', type=long,
        help='Port to bind to (default=7147)', default=7147)

    parser.add_argument('--firmwarefile', dest='firmwarefile', type=str, default=DEFAULTFIRMWARE,
        help='firmwarefile to load')
    parser.add_argument('--configure-inputs', dest='inputs', type=str,
            help='input multicast of format 225.0.0.152+3:7148,225.0.0.156+3:7148')

    parser.add_argument('--configure-outputs', dest='outputs', type=str,
            help='Ouput destinations format baseip:port, e.g. 239.0.0.111+7:7148')

    parser.add_argument('--log-level',dest='log_level',type=str,
        help='Logging level',default="INFO")

    parser.add_argument('--set-quantization', dest='quantization',
        help='Sets quanization factor')

    parser.add_argument('--set-fftshift', dest='fftshift', type=int, 
        help='Sets fft shift')

    parser.add_argument('--program', action="store_true", default=False, help="Programs he FPGA with the given firmware")
    args = parser.parse_args()

    print("Configuring skarab{}:{} wih firmware {}".format(args.host, args.port, args.firmwarefile))
    client = SkarabChannelizerClient(args.host, args.port, args.firmwarefile)

    logging.getLogger().addHandler(logging.NullHandler())
    log = logging.getLogger('mpikat')
    coloredlogs.install(
        fmt="[ %(levelname)s - %(asctime)s - %(name)s - %(filename)s:%(lineno)s] %(message)s",
        level=args.log_level.upper(),
        logger=log)

    actions = [(client.initialize, {})]
    if args.program:
        actions.append((client.program, {}))

    if args.outputs:
        ip, N, port = split_ipstring(args.outputs) 
        actions.append((client.configure_output, dict(dest_ip=ip, dest_port=port, number_of_groups=N)))
    if args.inputs:
        inp0, inp1 = args.inputs.split(',')
        actions.append((client.configure_inputs, dict(ips0=inp0, ips1=inp1)))

    if args.quantization:
        if args.quantization.startswith("0x"):
            v = int(args.quantization, 16)
        else:
            v = int(args.quantization)
        actions.append((client.configure_quantization_factor, dict(quant_factor=v)))
    if args.fftshift:
        actions.append((client.configure_fft_shift, dict(fft_shift=args.fftshift)))

    if args.program or args.outputs or args.inputs:
        actions.append((client.capture_start, {}))

    @coroutine
    def perform_actions():
        for action, params in actions:
            yield action(**params)
    ioloop = IOLoop.current()

    ioloop.run_sync(perform_actions)

