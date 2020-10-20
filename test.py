#!/usr/bin/env python
#
# Executes all discovered tests and logs debug output to file
#


import logging
import unittest

if __name__ == "__main__":
    logging.basicConfig(filename='debug.log',
        format=("[ %(levelname)s - %(asctime)s - %(name)s "
             "- %(filename)s:%(lineno)s] %(message)s"),
            level=logging.DEBUG)

    loader = unittest.TestLoader()
    tests = loader.discover(pattern="test*.py",
                            start_dir="mpikat")
    runner = unittest.runner.TextTestRunner()
    runner.run(tests)
