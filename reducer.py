#!/usr/bin/env python

from operator import itemgetter
import sys

current_author = None
current_count = 0
current_subreddit = None
author = None
subreddit = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    author,subreddit,count = line.split('\t')

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_author == author:
        if current_subreddit == subreddit:
            current_count += count
        else:
            print '%s\t%s\t%s' % (current_author, current_subreddit,current_count)
            current_subreddit=subreddit
            current_count=count
    else:
        if current_author:
            if current_subreddit:
            # write result to STDOUT
                print '%s\t%s\t%s' % (current_author, current_subreddit,current_count)
        current_count = count
        current_author = author
        current_subreddit = subreddit
# do not forget to output the last word if needed!
if current_author == author:
    print '%s\t%s\t%s' % (current_author, current_subreddit,current_count)

