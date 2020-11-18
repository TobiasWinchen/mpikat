from __future__ import print_function, division, unicode_literals
from mpikat.effelsberg.edd.pipeline.GatedSpectrometerPipeline import GatedSpectrometerPipeline
from mpikat.effelsberg.edd.pipeline.GatedFullStokesSpectrometerPipeline import GatedFullStokesSpectrometerPipeline
from katcp import FailReply
import unittest
import tornado.testing
import tornado.gen

import logging

class TestGatedSpectrometer(tornado.testing.AsyncTestCase):

    @tornado.gen.coroutine
    def __test_sequence(self, pipeline):
        self.assertEqual(pipeline.state, 'idle')
        result = pipeline.configure('{"nonfatal_numacheck":true,"dummy_input":true,"output_type":"null"}')
        self.assertEqual(pipeline.state, 'configuring')
        yield result
        self.assertEqual(pipeline.state, 'configured')

        yield pipeline.capture_start()
        self.assertEqual(pipeline.state, 'streaming')

        # Ignore mesaurement start, stop prepare
        yield pipeline.measurement_start()
        self.assertEqual(pipeline.state, 'streaming')

        yield pipeline.measurement_stop()
        self.assertEqual(pipeline.state, 'streaming')

        yield pipeline.measurement_prepare()
        self.assertEqual(pipeline.state, 'streaming')

        #yield pipeline.capture_stop()
        #self.assertEqual(pipeline.state, 'idle')

        # This test needs to be available,as otherwise on successfull test
        # pycoverage wont exit
        yield pipeline.deconfigure()
        self.assertEqual(pipeline.state, 'idle')


    @tornado.testing.gen_test(timeout=120)
    def test_FullStokes_sequence(self):
        pipeline = GatedFullStokesSpectrometerPipeline("localhost", 1234)
        yield self.__test_sequence(pipeline)

    @tornado.testing.gen_test(timeout=120)
    def test_2Pol_sequence(self):
        pipeline = GatedSpectrometerPipeline("localhost", 1234)
        yield self.__test_sequence(pipeline)


if __name__ == '__main__':
    logging.basicConfig(filename='debug.log',
        format=("[ %(levelname)s - %(asctime)s - %(name)s "
             "- %(filename)s:%(lineno)s] %(message)s"),
            level=logging.DEBUG)
    unittest.main()
