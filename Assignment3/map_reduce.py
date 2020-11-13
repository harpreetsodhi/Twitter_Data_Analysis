import operator
import pyspark
from pymongo import MongoClient
from pyspark import SparkContext
from pyspark.sql import SparkSession

client = MongoClient("mongodb+srv://harpreet:B00833691@mycluster.ytdpt.mongodb.net/test")
twitter_db = client.ProcessedDb
reuter_db = client.ReuterDb

search_collection = twitter_db.search
stream_collection = twitter_db.stream
news_collection = reuter_db.news

tweets = []
for tweet in search_collection.find():
    if tweet['text'] is not None:
        tweets.append(tweet['text'])
for tweet in stream_collection.find():
    if tweet['text'] is not None:
        tweets.append(tweet['text'])

reuters = []
for reuter in news_collection.find():
    text = ""
    title = ""
    if reuter["text"] is not None:
        text = reuter["text"]
    if reuter["title"] is not None:
        title = reuter["title"]
    line = text +" "+ title
    reuters.append(line)

keywords = ["storm", "winter", "canada", "hot", "cold", "flu", "snow", "indoor", "safety", "rain", "ice"]

reuters = spark.sparkContext.parallelize(reuters)
words = reuters.flatMap(lambda line: line.split(" ")).map(lambda word: word.strip().lower()).filter(lambda word: word in keywords).map(lambda word: (word,1))
counts = words.reduceByKey(operator.add)
print("ReuterDb:")
print(counts.collect())

tweets = spark.sparkContext.parallelize(tweets)
words = tweets.flatMap(lambda line: line.split(" ")).map(lambda word: word.strip().lower()).filter(lambda word: word in keywords).map(lambda word: (word,1))
counts = words.reduceByKey(operator.add)
print("ProcessedDb:")
print(counts.collect())
