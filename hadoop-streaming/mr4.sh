#!/bin/bash

for year in 2008 2009 2010 2011 2012 2013

do 
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar  -file ~/mapper2.py -mapper ~/mapper2.py -file ~/reducer2.py -reducer ~/reducer2.py -input /tmp/output/${year}-01 -output /tmp/output2/${year}-01
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar  -file ~/mapper2.py -mapper ~/mapper2.py -file ~/reducer2.py -reducer ~/reducer2.py -input /tmp/output/${year}-02 -output /tmp/output2/${year}-02
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar  -file ~/mapper2.py -mapper ~/mapper2.py -file ~/reducer2.py -reducer ~/reducer2.py -input /tmp/output/${year}-03 -output /tmp/output2/${year}-03
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar  -file ~/mapper2.py -mapper ~/mapper2.py -file ~/reducer2.py -reducer ~/reducer2.py -input /tmp/output/${year}-04 -output /tmp/output2/${year}-04
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar  -file ~/mapper2.py -mapper ~/mapper2.py -file ~/reducer2.py -reducer ~/reducer2.py -input /tmp/output/${year}-05 -output /tmp/output2/${year}-05
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar  -file ~/mapper2.py -mapper ~/mapper2.py -file ~/reducer2.py -reducer ~/reducer2.py -input /tmp/output/${year}-06 -output /tmp/output2/${year}-06
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar  -file ~/mapper2.py -mapper ~/mapper2.py -file ~/reducer2.py -reducer ~/reducer2.py -input /tmp/output/${year}-07 -output /tmp/output2/${year}-07
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar  -file ~/mapper2.py -mapper ~/mapper2.py -file ~/reducer2.py -reducer ~/reducer2.py -input /tmp/output/${year}-08 -output /tmp/output2/${year}-08
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar  -file ~/mapper2.py -mapper ~/mapper2.py -file ~/reducer2.py -reducer ~/reducer2.py -input /tmp/output/${year}-09 -output /tmp/output2/${year}-09
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar  -file ~/mapper2.py -mapper ~/mapper2.py -file ~/reducer2.py -reducer ~/reducer2.py -input /tmp/output/${year}-10 -output /tmp/output2/${year}-10
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar  -file ~/mapper2.py -mapper ~/mapper2.py -file ~/reducer2.py -reducer ~/reducer2.py -input /tmp/output/${year}-11 -output /tmp/output2/${year}-11
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar  -file ~/mapper2.py -mapper ~/mapper2.py -file ~/reducer2.py -reducer ~/reducer2.py -input /tmp/output/${year}-12 -output /tmp/output2/${year}-12
done

