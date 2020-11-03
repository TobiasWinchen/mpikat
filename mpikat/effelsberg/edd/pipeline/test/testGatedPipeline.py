from __future__ import print_function, division, unicode_literals
from mpikat.effelsberg.edd.pipeline.GatedSpectrometerPipeline import GatedSpectrometerPipeline
from katcp import FailReply
import unittest
import tornado.testing

import logging

class TestEDDPipeline(tornado.testing.AsyncTestCase):
    @tornado.testing.gen_test(timeout=120)
    def test_sequence(self):
        pipeline = GatedSpectrometerPipeline("localhost", 1234)
        self.assertEqual(pipeline.state, 'idle')
        result = pipeline.configure("")
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

        yield pipeline.capture_stop()
        self.assertEqual(pipeline.state, 'idle')

        yield pipeline.deconfigure()
        self.assertEqual(pipeline.state, 'idle')


if __name__ == '__main__':
    unittest.main()