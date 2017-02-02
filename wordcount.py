#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

import sys,json

from operator import add

from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession,SQLContext,Row
from pyspark.sql.types import *
from itertools import chain
#from pyspark.sql.functions import split,explode
from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors, VectorUDT


def record_to_row(record):
    schema = record[0],{record[1]:record[2]}
    return Row(**schema)

def make_vector(record,sublist):
    ret=[]    
    for sub in sublist:
        if sub in record[1]:
            ret.append(record[1][record[1].index(sub)+1])
        else:
            ret.append(0)          
    return ret
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .enableHiveSupport()\
        .getOrCreate()
    
#    lines=sc.textFile(sys.argv[1])

#    authorsub(sys.argv[1])

    lines = spark.read.json(sys.argv[1])
#    print ('Number of comments before filtering:' + str(lines.count()))
    lines = lines.filter(lines["author"] != "[deleted]")
#    print ('Number of comments after filtering: ' + str(lines.count()))
#    lines.printSchema()

#    words = lines.select(explode(split(lines["body"]," ")).alias("word"))
#    wordcount=words.count()
#    print ('Total number of words: ' + str(wordcount))
#    topwordscount = words.groupBy('word').count().orderBy('count',ascending=False)
#    print (topwordscount.show())
#    print (topwordscount.count())
    allsubs=lines.select("subreddit").groupBy('subreddit').rdd.flatMap(lambda x: x).collect()
    sublist=allsubs.select("subreddit").rdd.flatMap(lambda x: x).collect()
    broadcastSubs=spark.sparkContext.broadcast(sublist)    
#    fields=[StructField("name",StringType(),True)] + [StructField(field_name,In$
#    schema=StructType(fields)



#    print (topsubscount.show())
#    print (subcount)
#    header=topsubscount.rdd.map(lambda r: r.subreddit).collect()
#    print (header)
    
#    topauthorcount=lines.select("author").groupBy('author').count().orderBy('count',ascending=False)
#    print (topauthorcount.show())
#    authorcount=topauthorcount.count()
#    print (authorcount)
    posthistory=lines.select("author","subreddit").groupBy('author','subreddit').count().orderBy('count',ascending=False)
    postrdd=posthistory.rdd.map(lambda (x,y,z): (x,[y,z])).reduceByKey(lambda p,q: p+q)
    print (postrdd.take(2))
    datavector=postrdd.map(lambda x:make_vector(x,sublist))
    print (datavector.take(2))
    df = spark.createDataFrame(datavector,["features"])
    model = PCA(k=10,inputCol="features",outputCol="pcaFeatures").fit(df)
#    row_rdd=posthistory.rdd.map(tuple)
#    row_rdd=row_rdd.map(lambda x:record_to_row(x))
#    schema_my_rdd=spark.createDataFrame(row_rdd,schema)
#    schema_my_rdd.show()
#    indexer=StringIndexer(inputCol="subreddit",outputCol="SubredditIndex")
#    indexedposts=indexer.fit(posthistory).transform(posthistory)
#    indexer2=indexer=StringIndexer(inputCol="author",outputCol="AuthorIndex")
#    indexedposts=indexer2.fit(indexedposts).transform(indexedposts)
#    datardd=indexedposts.rdd
    spark.stop()
