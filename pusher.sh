#!/bin/bash

for year in 2010 2011 2012 2013 2014 2015

do 
    spark-submit --master spark://172.31.1.72:7077 ml.py hdfs://172.31.1.72:9000/tmp/input/RC_${year}-01
    spark-submit --master spark://172.31.1.72:7077 ml.py hdfs://172.31.1.72:9000/tmp/input/RC_${year}-02
    spark-submit --master spark://172.31.1.72:7077 ml.py hdfs://172.31.1.72:9000/tmp/input/RC_${year}-03
    spark-submit --master spark://172.31.1.72:7077 ml.py hdfs://172.31.1.72:9000/tmp/input/RC_${year}-04
    spark-submit --master spark://172.31.1.72:7077 ml.py hdfs://172.31.1.72:9000/tmp/input/RC_${year}-05
    spark-submit --master spark://172.31.1.72:7077 ml.py hdfs://172.31.1.72:9000/tmp/input/RC_${year}-06
    spark-submit --master spark://172.31.1.72:7077 ml.py hdfs://172.31.1.72:9000/tmp/input/RC_${year}-07
    spark-submit --master spark://172.31.1.72:7077 ml.py hdfs://172.31.1.72:9000/tmp/input/RC_${year}-08
    spark-submit --master spark://172.31.1.72:7077 ml.py hdfs://172.31.1.72:9000/tmp/input/RC_${year}-09
    spark-submit --master spark://172.31.1.72:7077 ml.py hdfs://172.31.1.72:9000/tmp/input/RC_${year}-10
    spark-submit --master spark://172.31.1.72:7077 ml.py hdfs://172.31.1.72:9000/tmp/input/RC_${year}-11
    spark-submit --master spark://172.31.1.72:7077 ml.py hdfs://172.31.1.72:9000/tmp/input/RC_${year}-12
done

