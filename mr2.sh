#!/bin/bash

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -file ~/mapper2.py -mapper ~/mapper2.py -file ~/reducer2.py -reducer ~/reducer2.py -input /tmp/output -output /tmp/output2
