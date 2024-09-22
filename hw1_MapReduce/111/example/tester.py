import sys
import numpy as np

range_ = range(10**6)
rng_cnt = range(1, 6)
n_ = np.random.choice(rng_cnt)
str_ = ""

f = open(r'C:\\Users\\andy\\Documents\\BigData\\hw1\\example\\new.txt', 'r') 
    

for line_ in f:
    print(line_)
    key = np.random.choice(range_)
    line = '{} {}'.format(str(key), line_.strip())

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
