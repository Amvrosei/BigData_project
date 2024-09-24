#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
 
import sys
import re
 
def proper(word): # проверка принадлежности к имени собственному
    if word[0].isupper() and word[1:].islower():
        return True
    else:
        return False
    
    # if len(word) >= 6: # длина от 6 до 9 символов
    #     return True
    # if len(word) <= 9:
    #     return True
    # if word.isalpha(): # состоит из букв, причем первая - заглавная, остальные строчные
    #     return True
    # if word[0].isupper():
    #     return True
    # if word[1:].islower():
    #     return True
    # else:
    #     return False

for line in sys.stdin:
    try:
        article_id, text = line.strip().split('\t', 1)
    except ValueError as e:
        continue
    
    words = re.sub('\W', ' ', text).split(' ')
    for word in words:
        if len(word) >= 6 and len(word) <= 9 and word.isalpha():
            status = int(proper(word)) # применяем функцию, проверяем слово
            print("{}\t{}".format(word.lower(), status))
