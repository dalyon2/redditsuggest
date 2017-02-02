#!/usr/bin/env python

import sys
import json

# input comes from STDIN (standard input)

for line in sys.stdin:
    # remove leading and trailing whitespace
    user=line.split(" [ ")[0]
    # split the line into words
    subreddits=line.split(" [ ")[1].split(" ]")[0].split(", ")
    # increase counters
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
    for thing in subreddits:
        print thing.split("=")[0] + '\t' + thing.split("=")[1]

