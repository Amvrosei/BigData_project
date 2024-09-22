#!/usr/bin/env python
 
import sys
import re
 
for line in sys.stdin:
    article_id, text = line.strip().split('\t', 1)
    text = re.sub('\W', ' ', text).split(' ')
    for word in text:
        if len(word) >= 3:
            key = "".join(sorted(word.lower()))
            print("{}\t{}\t{}".format(key, word.lower(), 1))