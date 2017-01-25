#!/bin/bash

for year in 2008 2009 2010 2011 2012 2013 2014 2015

do 
    wget -qO- https://s3-us-west-2.amazonaws.com/reddit-comments/${year}/RC_${year}-01 | hadoop fs -put - /tmp/RC_${year}-01 
    wget -qO- https://s3-us-west-2.amazonaws.com/reddit-comments/${year}/RC_${year}-02 | hadoop fs -put - /tmp/RC_${year}-02
    wget -qO- https://s3-us-west-2.amazonaws.com/reddit-comments/${year}/RC_${year}-03 | hadoop fs -put - /tmp/RC_${year}-03
    wget -qO- https://s3-us-west-2.amazonaws.com/reddit-comments/${year}/RC_${year}-04 | hadoop fs -put - /tmp/RC_${year}-04
    wget -qO- https://s3-us-west-2.amazonaws.com/reddit-comments/${year}/RC_${year}-05 | hadoop fs -put - /tmp/RC_${year}-05
    wget -qO- https://s3-us-west-2.amazonaws.com/reddit-comments/${year}/RC_${year}-06 | hadoop fs -put - /tmp/RC_${year}-06
    wget -qO- https://s3-us-west-2.amazonaws.com/reddit-comments/${year}/RC_${year}-07 | hadoop fs -put - /tmp/RC_${year}-07
    wget -qO- https://s3-us-west-2.amazonaws.com/reddit-comments/${year}/RC_${year}-08 | hadoop fs -put - /tmp/RC_${year}-08
    wget -qO- https://s3-us-west-2.amazonaws.com/reddit-comments/${year}/RC_${year}-09 | hadoop fs -put - /tmp/RC_${year}-09
    wget -qO- https://s3-us-west-2.amazonaws.com/reddit-comments/${year}/RC_${year}-10 | hadoop fs -put - /tmp/RC_${year}-10
    wget -qO- https://s3-us-west-2.amazonaws.com/reddit-comments/${year}/RC_${year}-11 | hadoop fs -put - /tmp/RC_${year}-11
    wget -qO- https://s3-us-west-2.amazonaws.com/reddit-comments/${year}/RC_${year}-12 | hadoop fs -put - /tmp/RC_${year}-12
done

