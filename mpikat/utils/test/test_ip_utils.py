from __future__ import print_function, division, unicode_literals
from mpikat.utils.ip_utils import split_ipstring, is_validat_multicast_range, ipstring_from_list, ipstring_to_list
import unittest
import logging

class Test_split_ip_string(unittest.TestCase):
    def test_invalid_ip(self):
        with self.assertRaises(Exception) as cm:
            split_ipstring("foo")
        with self.assertRaises(Exception) as cm:
            split_ipstring("123.456.789.123")

    def test_invalid_port(self):
        with self.assertRaises(ValueError) as cm:
            split_ipstring("123.0.0.1:foo")
        with self.assertRaises(ValueError) as cm:
            split_ipstring("123.0.0.1+4:foo")

    def test_invalid_range(self):
        with self.assertRaises(Exception) as cm:
            split_ipstring("123.0.0.1+foo")
        with self.assertRaises(Exception) as cm:
            split_ipstring("123.0.0.1+foo:1234")

    def test_split(self):
        ip = "123.0.0.1"
        port = 42
        N = 8
        ips = "{}+{}:{}".format(ip, N-1, port)
        a,b,c = split_ipstring(ips)
        self.assertEqual(ip, a)
        self.assertEqual(N, b)
        self.assertEqual(port, c)


class Test_multicast_ranges(unittest.TestCase):
    def test_multicast(self):
        self.assertTrue(is_validat_multicast_range(*split_ipstring("225.0.0.0")))
        self.assertFalse(is_validat_multicast_range(*split_ipstring("127.0.0.1")))

    def test_range(self):
        self.assertTrue(is_validat_multicast_range(*split_ipstring("225.0.0.0+1")))
        self.assertTrue(is_validat_multicast_range(*split_ipstring("225.0.0.0+7")))
        self.assertTrue(is_validat_multicast_range(*split_ipstring("225.0.0.0+15")))
        self.assertFalse(is_validat_multicast_range(*split_ipstring("225.0.0.0+2")))
        self.assertFalse(is_validat_multicast_range(*split_ipstring("225.0.0.0+6")))

    def test_block_start(self):
        self.assertTrue(is_validat_multicast_range(*split_ipstring("225.0.0.0+1")))
        self.assertTrue(is_validat_multicast_range(*split_ipstring("225.0.0.8+7")))
        self.assertTrue(is_validat_multicast_range(*split_ipstring("225.0.0.4+3")))
        self.assertFalse(is_validat_multicast_range(*split_ipstring("225.0.0.4+7")))
        self.assertFalse(is_validat_multicast_range(*split_ipstring("225.0.0.7+15")))


class Test_ipstring_from_list(unittest.TestCase):
    def test_range(self):
        s = ipstring_from_list(["225.0.0.1", "225.0.0.2", "225.0.0.3"])
        self.assertEqual(s, "225.0.0.1+2")

    def test_noncont_range(self):
        with self.assertRaises(Exception) as cm:
            s = ipstring_from_list(["225.0.0.1", "225.0.0.3"])


class Test_ipstring_to_list(unittest.TestCase):
    def test_range(self):
        s = ipstring_to_list("225.0.0.1+5")
        self.assertEqual(len(s), 6)
        self.assertEqual(s[0], "225.0.0.1")
        self.assertEqual(s[1], "225.0.0.2")
        self.assertEqual(s[2], "225.0.0.3")
        self.assertEqual(s[3], "225.0.0.4")
        self.assertEqual(s[4], "225.0.0.5")
        self.assertEqual(s[5], "225.0.0.6")

    def test_zero_range(self):
        s = ipstring_to_list("225.0.0.1")
        self.assertEqual(len(s), 1)
        self.assertEqual(s[0], "225.0.0.1")


if __name__ == '__main__':
    #logging.basicConfig(level=logging.DEBUG, format='%(name)s %(levelname)s %(message)s')
    unittest.main()
