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
from pyspark.ml.feature import PCA
import numpy
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import Normalizer

def record_to_row(record):
    schema = record[0],{record[1]:record[2]}
    return Row(**schema)

def make_vector(record,authorlist):
    feat=[]
    for author in authorlist:
        if author in record[1]:
            feat.append(int(record[1][record[1].index(author)+1]))
        else:
            feat.append(0)
    ret=Row(subreddit=record[0],features=Vectors.dense(feat))
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
    allsubs=lines.select("subreddit").groupBy('subreddit').count()
    subcount=allsubs.count()
    print (subcount)
#    sublist=allsubs.select("subreddit").rdd.flatMap(lambda x: x).collect()
#    broadcastSubs=spark.sparkContext.broadcast(sublist)    
#    fields=[StructField("name",StringType(),True)] + [StructField(field_name,In$
#    schema=StructType(fields)



#    print (topsubscount.show())
#    print (subcount)
#    header=topsubscount.rdd.map(lambda r: r.subreddit).collect()
#    print (header)
    
    topauthorcount=lines.select("author").groupBy('author').count().orderBy('count',ascending=False).limit(subcount*int(numpy.sqrt(subcount)))
    print (topauthorcount.count())
    authorlist=topauthorcount.select("author").rdd.flatMap(lambda x:x).collect()
#    broadcastAuthors=spark.sparkContext.broadcast(authorlist)
#    print (authorcount)
    posthistory=lines.select("subreddit","author").groupBy('subreddit','author').count().orderBy('count',ascending=False)
    subrdd=posthistory.rdd.map(lambda (x,y,z): (x,[y,z])).reduceByKey(lambda p,q: p+q)
    data=subrdd.map(lambda x:make_vector(x,authorlist))
#    schema = StructType([
#        StructField("label", StringType(), True),
#        StructField("features", VectorUDT(), True)
#    ])    
#    datavector=data[0],pyspark.ml.linalg.DenseVector(data[1])
#    df = datavector.toDF(schema).printSchema()
    df = spark.createDataFrame(data)
    normalizer=Normalizer(inputCol="features",outputCol="normFeatures")
    NormData=normalizer.transform(df)
    NormData.show()
#    pcastuff=df.select("features").collect()
#    print (pcastuff)
#    model=PCA.fit(pcastuff)
    model = PCA(k=int(numpy.sqrt(subcount)),inputCol="normFeatures",outputCol="pcaFeatures").fit(NormData)
    result=model.transform(df).select("pcaFeatures")
    print (model.explainedVariance)
    print (model.pc)
    result.show()
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
