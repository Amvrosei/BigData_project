import sys

current_key = ''
sum_count = 0
words = {}

for line in sys.stdin:
    try:
        key, word, count = line.strip().split('\t')
        count = int(count)
    except ValueError as e:
        continue

    if current_key != key:
        if current_key:
            #words = {word : count}
            print("{}\t{}".format(word, count))
        current_key = key
        sum_count = count
    
    else:
        sum_count += count

        if word in words.keys():
            words[word] += word_count
        else:
            words[word] = word_count

    
  
if current_key:
    print(str_)
