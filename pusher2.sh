#!/bin/bash

for month in 02 03 04 05 06 07 08 09 10 11 12

do 
    spark-submit --master spark://172.31.1.75:7077 ml2.py RC_2011-${month}
    spark-submit --master spark://172.31.1.75:7077 ml2.py RC_2012-${month}
    spark-submit --master spark://172.31.1.75:7077 ml2.py RC_2013-${month}
    spark-submit --master spark://172.31.1.75:7077 ml2.py RC_2014-${month}
    spark-submit --master spark://172.31.1.75:7077 ml2.py RC_2015-${month}
    spark-submit --master spark://172.31.1.75:7077 ml2.py RC_2016-${month}
done

