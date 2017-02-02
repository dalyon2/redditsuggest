#!/usr/bin/env python

import sys
import json

# input comes from STDIN (standard input)

for line in sys.stdin:
    # remove leading and trailing whitespace
    json_data=json.loads(line)
    # split the line into words
    user=json_data["author"]
    subreddit=json_data["subreddit"]
    # increase counters
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
    if user!='[deleted]':
        print '%s,%s,%s' % (user,subreddit,1)

