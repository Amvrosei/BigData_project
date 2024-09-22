#!/usr/bin/env python
 
import sys
 
for line in sys.stdin:
    count, key, forms = line.strip().split('\t')
    print("{}\t{}\t{}".format(key, count, forms))