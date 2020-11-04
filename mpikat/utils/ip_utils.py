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
    log.debug("Checking string: {}".format(ips))
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


def is_valid_multicast_range(ip, N, port):
    """
    Validates a multicast ranges of format 225.0.0.4, 7, 1234.
        - The base ip has to be a valid multicast address.
        - The address range has to be a valid power of two.
        - The address has to be a multiple of the blocksize. A block of 8 can
          start at a.b.c.0, a.b.c.8, a.b.c.16, ...
          This is a limitation we inherited from the casperfpga skarab firmware(s).
    The function will return True if the string specifies a valid range and false otherwise. IF the string cannot be interpreted an error is raised.
    """
    if (N & (N - 1)) != 0:
        log.warning("{} is not a power of 2".format(N))
        return False
    try:
        network = ipaddress.ip_network('{}/{}'.format(ip, 32-(N.bit_length() - 1), strict=False))
    except ValueError as V:
        log.warning("{} does not start a valid range of size {} ({})".format(ip, 2**(N.bit_length()-1), N))
        return False

    log.debug(" Validating network {}".format(network))
    if not network.is_multicast:
        log.warning("{} is not a multicsat address".format(ip))
        return False

    if not network[0] == ipaddress.ip_address(ip):
        log.warning("{} does not start a valid range of size {} ({}) - please use {}".format(ip, 2**(N.bit_length()-1), N, network))
        return False
    return True


def ipstring_to_list(ipstring):
    """
    Generate list of ips from string of form a.b.c.d+3
    """
    ip, N, port = split_ipstring(ipstring)
    ips = [(ipaddress.IPv4Address(ip) + i).compressed for i in range(N)]
    return ips


def ipstring_from_list(ipstrings):
    """
    Creates a string of format a.b.c.d+x from a list of ips.
    """
    ips = [ipaddress.IPv4Address(ip) for ip in ipstrings]
    ips.sort()

    if ips[0] + len(ips) - 1 != ips[-1]:
        raise RuntimeError("IP list not continous")

    return "{}+{}".format(ips[0].compressed, len(ips) - 1)



