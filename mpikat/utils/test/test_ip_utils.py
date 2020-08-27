from __future__ import print_function, division, unicode_literals
import mpikat.utils.ip_utils as ip_utils
import unittest
import logging

class Test_split_ip_string(unittest.TestCase):
    def test_invalid_ip(self):
        with self.assertRaises(Exception) as cm:
            ip_utils.split_ipstring("foo")
        with self.assertRaises(Exception) as cm:
            ip_utils.split_ipstring("123.456.789.123")

    def test_invalid_port(self):
        with self.assertRaises(ValueError) as cm:
            ip_utils.split_ipstring("123.0.0.1:foo")
        with self.assertRaises(ValueError) as cm:
            ip_utils.split_ipstring("123.0.0.1+4:foo")

    def test_invalid_range(self):
        with self.assertRaises(Exception) as cm:
            ip_utils.split_ipstring("123.0.0.1+foo")
        with self.assertRaises(Exception) as cm:
            ip_utils.split_ipstring("123.0.0.1+foo:1234")

    def test_split(self):
        ip = "123.0.0.1"
        port = 42
        N = 8
        ips = "{}+{}:{}".format(ip, N-1, port)
        a,b,c = ip_utils.split_ipstring(ips)
        self.assertEqual(ip, a)
        self.assertEqual(N, b)
        self.assertEqual(port, c)


class Test_multicast_ranges(unittest.TestCase):
    def test_multicast(self):
        self.assertTrue(ip_utils.is_validat_multicast_range("225.0.0.0"))
        self.assertFalse(ip_utils.is_validat_multicast_range("127.0.0.1"))

    def test_range(self):
        self.assertTrue(ip_utils.is_validat_multicast_range("225.0.0.0+1"))
        self.assertTrue(ip_utils.is_validat_multicast_range("225.0.0.0+7"))
        self.assertTrue(ip_utils.is_validat_multicast_range("225.0.0.0+15"))
        self.assertFalse(ip_utils.is_validat_multicast_range("225.0.0.0+2"))
        self.assertFalse(ip_utils.is_validat_multicast_range("225.0.0.0+6"))

def test_block_start(self):
        self.assertTrue(ip_utils.is_validat_multicast_range("225.0.0.0+1"))
        self.assertTrue(ip_utils.is_validat_multicast_range("225.0.0.8+7"))
        self.assertTrue(ip_utils.is_validat_multicast_range("225.0.0.4+3"))
        self.assertFalse(ip_utils.is_validat_multicast_range("225.0.0.4+7"))
        self.assertFalse(ip_utils.is_validat_multicast_range("225.0.0.7+15"))


if __name__ == '__main__':
    #logging.basicConfig(level=logging.DEBUG, format='%(name)s %(levelname)s %(message)s')
    unittest.main()
