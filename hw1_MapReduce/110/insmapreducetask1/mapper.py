#! /usr/bin/python

import sys
import random

range = range(10**4)

for s in sys.stdin:
    key = random.choice(range)
    print(f"{key} {s.strip()}")
