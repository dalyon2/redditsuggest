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
#from pyspark.ml.feature import PCA
import numpy
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import Normalizer
from pyspark.sql.functions import udf
from datetime import datetime
from datetime import timedelta, date
#from pyspark.ml.clustering import KMeans
import redis

#def record_to_row(record):
#    schema = record[0],{record[1]:record[2]}
#    return Row(**schema)

def make_vector(record,authorlist):
    """Take a record of how many times each author posted on a subreddit
    merge it with the entire global list of authors to make
    a feature vector where the row label is subreddit and the columns
    are all authors"""

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
        print("Usage: ml <file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonKMeans")\
        .config("spark.worker.memory","90g")\
        .config("spark.executor.memory","10g")\
        .getOrCreate()

    r=redis.StrictRedis(host='172.31.1.69',port=6379)

    """read in a month of reddit comment data and remove posts by [deleted]
    keep only four fields, convert unix time to day"""
    lines = spark.read.json(sys.argv[1])
    lines = lines.filter(lines["author"] != "[deleted]")
    lines = lines.select("author","subreddit","body","created_utc")
     
    toDate = udf(lambda x: datetime.utcfromtimestamp(float(x)),DateType())
    lines=lines.select("author","subreddit","body",toDate(lines.created_utc).alias("Date"))

    """main loop through every day in a month"""

    yyyy=int(sys.argv[1][-7:-3])
    mm=int(sys.argv[1][-2:])
    today = date(yyyy,mm, 1)
    while today.month==mm:
        daylines=lines.filter(lines.Date==today)
        """to decide how many PCA dimensions to keep we need to know how many
        active subreddits there are today"""

        allsubs=daylines.select("subreddit").groupBy('subreddit').count()
        subcount=allsubs.count()
        print ('Number of active subreddits on ' +str(today) + " was " + str(subcount))

        """the authors are our features for clustering subreddits"""

        topauthorcount=daylines.select("author").groupBy('author').count().orderBy('count',ascending=False)
        topauthorcount=topauthorcount.filter(topauthorcount['count']>1)
        authorlist=topauthorcount.select("author").rdd.flatMap(lambda x:x).collect()
        print ("Number of authors used for clustering: " + str(len(authorlist)) + " on date: " + str(today))

        """combine posting history of each author into one line, then combine with author list to make feature vector"""

        posthistory=daylines.select("subreddit","Author").groupBy('subreddit',"Author").count().orderBy('count',ascending=False)
        subrdd=posthistory.rdd.map(lambda (x,y,z): (x,[y,z])).reduceByKey(lambda p,q: p+q)
        data=subrdd.map(lambda x:make_vector(x,authorlist))
        df = spark.createDataFrame(data)

        """normalize our features to prepare for PCA"""
        normalizer=Normalizer(inputCol="features",outputCol="normFeatures")
        NormData=normalizer.transform(df).select("normFeatures","subreddit")
        features=NormData.collect()
        ftdata=[]
        fttags=[]
        for row in features:
            ftdata.append(row['normFeatures'])
            fttags.append(row['subreddit'].encode('utf-8'))     
        featurearray=numpy.array(ftdata)
        k=int(numpy.sqrt(subcount))
        """fancy new Facebook random PCA"""
        sklearn_pca=PCA(k,copy=False,whiten=False,svd_solver='randomized',iterated_power=2)
        sklearn_pca.fit(featurearray)
        transformed = sklearn_pca.transform(featurearray)

        kmeans = KMeans(n_clusters=k+1,n_jobs=-1)
        transform2 = kmeans.fit_transform(transformed)
        partitions= kmeans.fit_predict(transformed)
        zip1=zip(partitions,transform2.tolist())
        zipped=dict(zip(fttags,zip1))
        r.set(today.isoformat(),zipped)
        today=today+timedelta(1)

    spark.stop()
