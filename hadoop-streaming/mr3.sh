#!/bin/bash

for year in 2008 2009 2010 2011 2012 2013

do 
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -Dstream.num.map.output.key.fields=2 -file ~/mapper.py -mapper ~/mapper.py -file ~/reducer.py -reducer ~/reducer.py -input /tmp/input/RC_${year}-01 -output /tmp/output/${year}-01
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -Dstream.num.map.output.key.fields=2 -file ~/mapper.py -mapper ~/mapper.py -file ~/reducer.py -reducer ~/reducer.py -input /tmp/input/RC_${year}-02 -output /tmp/output/${year}-02
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -Dstream.num.map.output.key.fields=2 -file ~/mapper.py -mapper ~/mapper.py -file ~/reducer.py -reducer ~/reducer.py -input /tmp/input/RC_${year}-03 -output /tmp/output/${year}-03
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -Dstream.num.map.output.key.fields=2 -file ~/mapper.py -mapper ~/mapper.py -file ~/reducer.py -reducer ~/reducer.py -input /tmp/input/RC_${year}-04 -output /tmp/output/${year}-04
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -Dstream.num.map.output.key.fields=2 -file ~/mapper.py -mapper ~/mapper.py -file ~/reducer.py -reducer ~/reducer.py -input /tmp/input/RC_${year}-05 -output /tmp/output/${year}-05
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -Dstream.num.map.output.key.fields=2 -file ~/mapper.py -mapper ~/mapper.py -file ~/reducer.py -reducer ~/reducer.py -input /tmp/input/RC_${year}-06 -output /tmp/output/${year}-06
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -Dstream.num.map.output.key.fields=2 -file ~/mapper.py -mapper ~/mapper.py -file ~/reducer.py -reducer ~/reducer.py -input /tmp/input/RC_${year}-07 -output /tmp/output/${year}-07
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -Dstream.num.map.output.key.fields=2 -file ~/mapper.py -mapper ~/mapper.py -file ~/reducer.py -reducer ~/reducer.py -input /tmp/input/RC_${year}-08 -output /tmp/output/${year}-08
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -Dstream.num.map.output.key.fields=2 -file ~/mapper.py -mapper ~/mapper.py -file ~/reducer.py -reducer ~/reducer.py -input /tmp/input/RC_${year}-09 -output /tmp/output/${year}-09
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -Dstream.num.map.output.key.fields=2 -file ~/mapper.py -mapper ~/mapper.py -file ~/reducer.py -reducer ~/reducer.py -input /tmp/input/RC_${year}-10 -output /tmp/output/${year}-10
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -Dstream.num.map.output.key.fields=2 -file ~/mapper.py -mapper ~/mapper.py -file ~/reducer.py -reducer ~/reducer.py -input /tmp/input/RC_${year}-11 -output /tmp/output/${year}-11
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -Dstream.num.map.output.key.fields=2 -file ~/mapper.py -mapper ~/mapper.py -file ~/reducer.py -reducer ~/reducer.py -input /tmp/input/RC_${year}-12 -output /tmp/output/${year}-12
done

