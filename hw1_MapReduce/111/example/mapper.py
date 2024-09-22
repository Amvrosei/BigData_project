#!/usr/bin/python
import sys
import numpy as np

range_ = range(10**6)

for line_ in sys.stdin:
    key = np.random.choice(range_)
    print('{} {}'.format(str(key), line_.strip()))