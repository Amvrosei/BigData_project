import sys
from random import randint

n = randint(1, 5)
str_ = ""

for line in sys.stdin:
    key, passed_id = line.strip().split(" ")
    str_ += passed_id
    n -= 1
    if n == 0:
        print(str_)
        str_ = ""
        n = randint(1, 5)
    else:
        str_ += ","
  
if str:
    print(str_)
