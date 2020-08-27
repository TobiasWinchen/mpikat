from __future__ import print_function, division, unicode_literals
"""
Utils to work with IP addresses
"""

import ipaddress
import logging

log = logging.getLogger('mpikat.ip_utils')

def split_ipstring(ips):
    """
    Split an string of form iprange:port, e.g. 123.0.0.4+7:1234 into baseip,
    total number of ips and port. Raise an exception on invalid ip, port or number.
    """
    try:
        if ":" not in ips:
            R = ips
            port = None
        else:
            R, port = ips.split(':')
            port = int(port)
        if '+' not in R:
            ip = R
            N = 1
        else:
            ip, N = R.split('+')
            N = int(N) + 1
        ipaddress.IPv4Address(ip)
    except Exception as E:
        log.error("Cannot parse string: {}".format(ips))
        raise E

    return ip, N, port


def is_validat_multicast_range(ips):
    """
    Validates a multicast range of format 225.0.0.4+7:1234.
        - The base ip has to be a valid multicast address.
        - The address range has to be a valid power of two.
        - The address has to be a multiple of the blocksize. A block of 8 can
          start at a.b.c.0, a.b.c.8, a.b.c.16, ...
          This is a limitation we inherited from the casperfpga skarab firmware(s).
    The function will return True if the string specifies a valid range and false otherwise. IF the string cannot be interpreted an error is raised.
    """

    log.debug("Checing network string: {}".format(ips))
    ip, N, port = split_ipstring(ips)
    if (N & (N - 1)) != 0:
        log.warning("{} is not a power of 2".format(N))
        return False
    network = ipaddress.ip_network('{}/{}'.format(ip, 32-(N).bit_length() - 1), strict=False)
    log.debug(" Validating network {}".format(network))

    if not network.is_multicast:
        log.warning("{} is not a multicsat address".format(ip))
        return False

    if not network[0] == ipaddress.ip_address(ip):
        log.warning("{} does not start a valid range of size {} ({}) - please use {}".format(ip, 2**N.bit_length(), N, network))
        return False
    return True





