#!/usr/bin/env python
#
# Dummy script to replace numactl in testing environment
#
import argparse
import subprocess

print("Using dummy numactl")

parser = argparse.ArgumentParser()
parser.add_argument("cmd", nargs="*")

args, unknown = parser.parse_known_args()

p = subprocess.Popen(args.cmd)
p.wait()

