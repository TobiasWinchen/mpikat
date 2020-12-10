from __future__ import print_function, division, unicode_literals
from mpikat.effelsberg.edd.pipeline.edd_hdf_pipeline import EDDHDF5WriterPipeline 
from katcp import FailReply
import unittest
import tornado.testing
import tornado.gen
import tempfile
import shutil
import logging
#
#class TestHDFPipeline(tornado.testing.AsyncTestCase):
#
#    #def setUp(self):
#    #    super(TestHDFPipeline, self).setUp()
#    #    print(self.datadir)
#
#
#    @tornado.testing.gen_test
#    def test_sequence(self):
#        pipeline = EDDHDF5WriterPipeline("localhost", 1234)
#        self.datadir = tempfile.mkdtemp()
#
#
#        self.assertEqual(pipeline.state, 'idle')
#
#        #js =  '{"output_directory":"' + self.datadir + '"}'
#        #print(js)
#
#        yield pipeline.configure()
#        self.assertEqual(pipeline.state, 'configured')
#
#        yield pipeline.capture_start()
#        self.assertEqual(pipeline.state, 'ready')
#
#        yield pipeline.measurement_prepare()
#        self.assertEqual(pipeline.state, 'set')
#
#        # Ignore mesaurement start, stop prepare
#        yield pipeline.measurement_start()
#        self.assertEqual(pipeline.state, 'measuring')
#
#        yield pipeline.measurement_stop()
#        self.assertEqual(pipeline.state, 'ready')
#
#        # This test needs to be available,as otherwise on successfull test
#        # pycoverage wont exit
#        yield pipeline.deconfigure()
#        self.assertEqual(pipeline.state, 'idle')
#
#
##    @tornado.testing.gen_test(timeout=120)
##    def test_measurement_prepare_prefix(self):
##        super(TestHDFPipeline, self).setUp()
##        pass
##        #shutil.rmtree(self.datadir)
#
#
#
#if __name__ == '__main__':
#    logging.basicConfig(filename='debug.log',
#        format=("[ %(levelname)s - %(asctime)s - %(name)s "
#             "- %(filename)s:%(lineno)s] %(message)s"),
#            level=logging.DEBUG)
#    unittest.main()
