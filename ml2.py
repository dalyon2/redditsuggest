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
#from sklearn.metrics import  silhouette_samples, silhouette_score
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import Normalizer,VectorAssembler
from pyspark.sql.functions import udf
from datetime import datetime
from datetime import timedelta, date
#from pyspark.ml.clustering import KMeans
import redis
import time

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
        print("Usage: ml2 <file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonKMeans")\
        .config("spark.worker.memory","90g")\
        .config("spark.executor.memory","10g")\
        .config("spark.driver.memory","20g")\
        .getOrCreate()

    r=redis.StrictRedis(host='172.31.1.69',port=6379, password='')

    t0=time.time()
    """read in a month of reddit comment data and remove posts by [deleted]
OB    keep only four fields, convert unix time to day"""
    lines=spark.read.json('s3n://dr-reddit-comment-data/' + sys.argv[1])

    lines = lines.filter(lines["author"] != "[deleted]")
#    lines = lines.select("author","subreddit","created_utc")
    lines=lines.select("author","subreddit")
#    toDate = udf(lambda x: datetime.utcfromtimestamp(float(x)),DateType())
#    lines=lines.select("author","subreddit",toDate(lines.created_utc).alias("Date"))

#    """main loop through every day in a month"""

#    yyyy=int(sys.argv[1][-7:-3])
#    mm=int(sys.argv[1][-2:])
#    today = date(yyyy,mm, 1)
#    while today.month==mm:
#    daylines=lines.filter(lines.Date==today)
#    print ('number of posts today: ',daylines.count())
    """to decide how many PCA dimensions to keep we need to know how many
        active subreddits there are today"""

    allsubs=lines.select("subreddit").groupBy('subreddit').count()
    allsubs=allsubs.withColumnRenamed('subreddit','subreddit2')
    allsubs=allsubs.filter(allsubs['count']>900)
    topsubcount=allsubs.count()
    subcountdf = lines.join(allsubs,lines['subreddit']==allsubs['subreddit2'],'inner')
#    subcountdf=subcountdf.filter(subcountdf['count']>900)
#    topsubcount=subcountdf.groupBy('subreddit').count().count()
    subcountdf=subcountdf.drop('count').drop('subreddit2')
#    allsubcount=allsubs.count()
    subdict=allsubs.rdd.collectAsMap()
    print ('On ' + sys.argv[1] + ' the number of active subreddits after filtering was: ' + str(topsubcount))
    """the authors are our features for clustering subreddits, only keeping authors with more than 5 posts"""
    topauthors=subcountdf.select("author").groupBy('author').count()
    topauthorcount=topauthors.count()
    topauthors=topauthors.filter(topauthors['count']>300)
    authorlist=topauthors.select("author").rdd.flatMap(lambda x:x).collect()

    print ("Number of authors on active subs was: " + str(topauthorcount) + " used for clustering: " + str(len(authorlist)) + " on date: " + sys.argv[1])

    """combine posting history of each author into one line, then combine with author list to make feature vector"""
    
    posthistory=subcountdf.select("subreddit","Author").groupBy('subreddit',"Author").count()
    subrdd=posthistory.rdd.map(lambda (x,y,z): (x,[y,z])).reduceByKey(lambda p,q: p+q)
    data=subrdd.map(lambda x:make_vector(x,authorlist))
    df = spark.createDataFrame(data)
    t1=time.time()
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
    numdims=int(numpy.sqrt(topsubcount))
    """spark distributed PCA"""
#    spark_pca = PCA(k=numdims,inputCol='normFeatures',outputCol='pca_features')
#    model = spark_pca.fit(NormData)
#    dimreduced=model.transform(NormData).cache()    

    """fancy new Facebook random PCA"""

    sklearn_pca=PCA(n_components=numdims,copy=False,whiten=False,svd_solver='randomized',iterated_power=2)
    dimreduced = sklearn_pca.fit_transform(featurearray)
#    dimreduced = sklearn_pca.transform(featurearray)
    t2=time.time()
    
#    model = KMeans(featuresCol="pca_features",k=numdims).fit(dimreduced)
#    transformed=model.transform(dimreduced).select("subreddit","prediction")
#    rows=transformed.collect()
#        print(sklearn_pca.explained_variance_)
#        print(sklearn_pca.explained_variance_ratio_)
    kmeans = KMeans(n_clusters=numdims,n_jobs=-1)
    partitions=kmeans.fit_predict(dimreduced)
#    clusterspace = kmeans.fit_transform(dimreduced)
    t3=time.time()
    print('Pre-PCA took ' + str(t1-t0) + ' PCA took ' + str(t2-t1) + ' and K-means took' + str(t3-t2))

#    zip1=zip(partitions,clusterspace.tolist())
#    zip2=[]
#    for s in zip1:
#        zip2.append((s[0],s[1][s[0]]))
    zipped=zip(fttags,partitions)
    output=[]
#    for row in zipped:
#        output.append({'subreddit':row['subreddit'],'cluster':row['prediction'],'subsize':subdict[row['subreddit']]})
    for row in zipped:
        output.append({'subreddit':row[0],'cluster':row[1],'subsize':subdict[row[0]]})
    r.set(sys.argv[1].replace('RC_',''),output)
    print(sys.argv[1].replace('RC_','') + " sent to Redis!")

#    r.set(today.isoformat(),output)
#    print(today.isoformat() + " sent to Redis!")
#        today=today+timedelta(1)
    spark.stop()
