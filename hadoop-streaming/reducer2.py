#!/usr/bin/env python

from operator import itemgetter
import sys

current_subreddit = None
current_count = 0
current_squared = 0
subreddit = None
count = 0
squared = 0

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    subreddit,count=line.split("\t")

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_subreddit == subreddit:
        current_count += count
        current_squared += count*count
    else:
        if current_subreddit:
            # write result to STDOUT
            print current_subreddit + "\t" + str(current_count) + "\t" + str(current_squared)
        current_count = count
        current_subreddit = subreddit
	current_squared = count*count
# do not forget to output the last word if needed!
if current_subreddit == subreddit:
    print current_subreddit + "\t" + str(current_count) + "\t" + str(current_squared)
#    print '%s\t%s\t%s' % (current_author, current_subreddit,current_count)
