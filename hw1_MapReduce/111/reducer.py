#!/usr/bin/env python3

import sys

prev_word = None
good_word = True
curr_count = 0

for line in sys.stdin:
    word, is_capital = line.strip().split('\t', 1) 
    # если первое слово не подходит
    if prev_word is None:
        prev_word = word
        if is_capital == '0':
            good_word = False 
    # если слово совпадает с предыдущим
    elif prev_word == word:
        if is_capital == '0':
            good_word = False
        curr_count += 1
    # если новое слово
    else: 
        # вывод подходящего слова
        if good_word:
            print("{0}\t{1}".format(curr_count, prev_word))
        prev_word = word
        
        # обработка нового слова
        if is_capital == '0':
            good_word = False
        else: 
            good_word = True
            curr_count = 1

# последнее слово
if is_capital == '1' and good_word:
    print("{0}\t{1}".format(curr_count, prev_word))