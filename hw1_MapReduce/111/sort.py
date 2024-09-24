#!/usr/bin/env python3.6
 
import sys
 
for line in sys.stdin:
    count, key = line.strip().split('\t')
    print("{}\t{}".format(key, count))