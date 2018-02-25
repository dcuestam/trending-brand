'''

    File name: spark_consumer.py
    Author: David Cuesta
    Python Version: 3.6

'''

import os

########################################################################################################################
# PACKAGES CONFIGURATION
########################################################################################################################
os.system("yum install python-pip -y")
os.system("pip install pyhdfs")

########################################################################################################################
# IMPORTS
########################################################################################################################
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import StructType
import json
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType,LongType
from pyspark.sql import Row
from pyspark.sql import SparkSession
import datetime
import pyhdfs
########################################################################################################################

def load_wordlist(filename):

    hdfs = pyhdfs.HdfsClient(hosts='hdfs-namenode:50070')
    words = {}
    f = hdfs.open(filename)
    text = f.read().decode('utf-8')
    text = text.split('\n')
    for line in text:
        words[line] = 1
    f.close()
    return words


def getSparkSessionInstance(sparkConf):

    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]



def write_full_info(time, rdd):

    spark = getSparkSessionInstance(rdd.context.getConf())
    rowRdd = rdd.map(lambda w: Row(tweet=w,dtt=str(datetime.datetime.today())[:-10]))
    schema = StructType([StructField("tweet", StringType(), True)
                            ,StructField("dtt", StringType(), True)

                         ])
    wordsDataFrame = spark.createDataFrame(rowRdd,schema)
    wordsDataFrame.write.parquet(path="hdfs://hdfs-namenode:9000/spark/full" , mode="append", compression=None)

def write_sentiments_by_time(time, rdd):

    spark = getSparkSessionInstance(rdd.context.getConf())
    rowRdd = rdd.map(lambda w: Row(tweet=w, dtt=str(datetime.datetime.today())[:-10]))
    schema = StructType([StructField("tweet", LongType(), True)
                            , StructField("dtt", StringType(), True)
                         ])

    wordsDataFrame = spark.createDataFrame(rowRdd, schema)
    wordsDataFrame.write.parquet(path="hdfs://hdfs-namenode:9000/spark/sentiments", mode="append", compression=None)

def write_count_by_time(time, rdd):

    spark = getSparkSessionInstance(rdd.context.getConf())
    rowRdd = rdd.map(lambda w: Row(tweet=w, dtt=str(datetime.datetime.today())[:-10]))
    schema = StructType([StructField("tweet", LongType(), True)
                            , StructField("dtt", StringType(), True)
                         ])

    wordsDataFrame = spark.createDataFrame(rowRdd, schema)
    wordsDataFrame.write.parquet(path="hdfs://hdfs-namenode:9000/spark/count", mode="append", compression=None)

def create_context():


    spark=SparkSession.builder.appName("Streamer").getOrCreate()
    ssc = StreamingContext(spark.sparkContext, 10)
    kafkaStream = KafkaUtils.createStream(ssc, 'zk_1:2181','3', {'beer': 1})
    parsed = kafkaStream.map(lambda v: json.loads(v[1]))
    text =  parsed.map(lambda tweet: tweet['text'])

    words = text.flatMap(lambda line: line.split(" "))


    positive = words.map(lambda word: ('Sentiment', 1) if word in pwords else ('Sentiment', 0))
    negative = words.map(lambda word: ('Sentiment', -1) if word in nwords else ('Sentiment', 0))


    count = text.map(lambda word: ('Count', 1) )



    allSentiments = positive.union(negative)



    sentimentCounts = allSentiments.reduceByKey(lambda x, y: x + y)
    countCounted = count.reduceByKey(lambda x,y: x+y)



    sentimentsMapped = sentimentCounts.map(lambda s : s[1])
    sentimentsMapped.foreachRDD(write_sentiments_by_time)

    countMapped = countCounted.map(lambda c : c[1])
    countMapped.foreachRDD(write_count_by_time)



    text.foreachRDD(write_full_info)

    return ssc



ssc = StreamingContext.getOrCreate('checkpoint_tfm', lambda: create_context())

pwords = load_wordlist("hdfs://hdfs-namenode:50070/positive.txt")
nwords = load_wordlist("hdfs://hdfs-namenode:50070/negative.txt")


ssc.start()
ssc.awaitTermination()
