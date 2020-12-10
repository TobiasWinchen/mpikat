from __future__ import print_function, division, unicode_literals
from mpikat.effelsberg.edd.pipeline.edd_hdf_pipeline import EDDHDF5WriterPipeline 
from katcp import FailReply
import unittest
import tornado.testing
import tornado.gen

import logging

class TestHDFPipeline(tornado.testing.AsyncTestCase):

    def setUp(self):
        super(TestHDFPipeline, self).setUp()
        self.pipeline = EDDHDFFileWriter("localhost", 1234)


    @tornado.gen.coroutine
    def test_sequence(self):
        self.assertEqual(self.pipeline.state, 'idle')
        result = self.pipeline.configure('{"output_directory":"/tmp"}')
        yield result
        self.assertEqual(self.pipeline.state, 'configured')

        yield self.pipeline.capture_start()
        self.assertEqual(self.pipeline.state, 'streaming')

        yield self.pipeline.measurement_prepare()
        self.assertEqual(self.pipeline.state, 'ready')

        # Ignore mesaurement start, stop prepare
        yield self.pipeline.measurement_start()
        self.assertEqual(self.pipeline.state, 'measuring')

        yield self.pipeline.measurement_stop()
        self.assertEqual(self.pipeline.state, 'ready')

        # This test needs to be available,as otherwise on successfull test
        # pycoverage wont exit
        yield self.pipeline.deconfigure()
        self.assertEqual(self.pipeline.state, 'idle')


#    @tornado.testing.gen_test(timeout=120)
#    def test_measurement_prepare_prefix(self):



if __name__ == '__main__':
    logging.basicConfig(filename='debug.log',
        format=("[ %(levelname)s - %(asctime)s - %(name)s "
             "- %(filename)s:%(lineno)s] %(message)s"),
            level=logging.DEBUG)
    unittest.main()
