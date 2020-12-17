import operator
import pyspark
from pymongo import MongoClient
from pyspark import SparkContext
from pyspark.sql import SparkSession

client = MongoClient("mongodb+srv://harpreet:B00833691@mycluster.ytdpt.mongodb.net")
twitter_db = client.ProcessedDb

search_collection = twitter_db.search
stream_collection = twitter_db.stream

tweets = []
for tweet in search_collection.find():
    if tweet['text'] is not None:
        tweets.append(tweet['text'])
for tweet in stream_collection.find():
    if tweet['text'] is not None:
        tweets.append(tweet['text'])

keywords = ["storm", "winter", "canada", "hot", "cold", "flu", "snow", "indoor", "safety", "rain", "ice"]

tweets = spark.sparkContext.parallelize(tweets)
words = tweets.flatMap(lambda line: line.split(" ")).map(lambda word: word.strip().lower()).filter(lambda word: word in keywords).map(lambda word: (word,1))
counts = words.reduceByKey(operator.add)
print("ProcessedDb:")
print(counts.collect())
