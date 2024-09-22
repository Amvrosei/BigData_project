#!/usr/bin/env python

import sys

select_second_field = lambda x: -x[1]
MAX_WORDS_IN_LINE = 5


def out_ans(words_arr):
    outstr = ""
    for form, form_count in sorted(words_arr, key=select_second_field)[:MAX_WORDS_IN_LINE]:
        outstr += "{}:{};".format(form, str(form_count))
    print("{}\t{}\t{}".format(str(sum_count), curr_key, outstr))

curr_key = ""
sum_count = 0
words = dict()


for line in sys.stdin:
    key, word, word_count = line.split('\t')
    word_count = int(word_count)
    if key != curr_key:
    
        if curr_key:
            out_ans(words.items())
        curr_key = key
        sum_count = word_count
        words = {word : word_count}
    else:
        
        sum_count += word_count
        if word in words.keys():
            words[word] += word_count
        else:
            words[word] = word_count

if curr_key:
    out_ans(words.items())

