from __future__ import print_function, division, unicode_literals
from mpikat.effelsberg.edd.pipeline.EDDPipeline import EDDPipeline
from katcp import FailReply
import unittest
import logging

class TestEDDPipeline(unittest.TestCase):
    def test_set(self):
        pipeline = EDDPipeline("localhost", 1234, dict(foo=''))
        pipeline.set({"foo":"bar"})
        self.assertEqual(pipeline._config['foo'], 'bar')

        with self.assertRaises(FailReply) as cm:
            yield pipeline.set({"bar":"foo"})



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(name)s %(levelname)s %(message)s')
    unittest.main()
