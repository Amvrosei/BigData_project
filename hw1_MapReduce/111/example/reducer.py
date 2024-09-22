#!/usr/bin/python
import sys
import numpy as np

rng_cnt = range(1, 6)
n_ = np.random.choice(rng_cnt)
str_ = ""

for line in sys.stdin:
    key, passed_id = line.strip().split(" ")
    str_ += passed_id
    n_ -= 1
    if (n_ == 0):
        print(str_)
        str_ = ""
        n_ = np.random.choice(rng_cnt)
    else:
        str_+= ","
if str_:
    print(str_[:-1])