#!/usr/bin/env python

from operator import itemgetter
import sys

current_author = None
current_count = 0
current_subreddit = None
author = None
subreddit = None
commentlist = []

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    author,subreddit,count = line.split(',')

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
	    commentlist.append((current_subreddit,str(current_count)))
#            print '%s\t%s\t%s' % (current_author, current_subreddit,current_count)
            current_subreddit=subreddit
            current_count=count
    else:
        if current_author:
            if current_subreddit:
                commentlist.append((current_subreddit,str(current_count)))
            # write result to STDOUT
                print current_author + " [ "+', '.join('{}={}'.format(*e1) for e1 in commentlist) + " ]"
        current_count = count
        current_author = author
        current_subreddit = subreddit
	commentlist=[]
# do not forget to output the last word if needed!
if current_author == author:
    commentlist.append((current_subreddit,str(current_count)))
    print current_author + " [ " +', '.join('{}={}'.format(*e1) for e1 in commentlist)+" ]"
#    print '%s\t%s\t%s' % (current_author, current_subreddit,current_count)
