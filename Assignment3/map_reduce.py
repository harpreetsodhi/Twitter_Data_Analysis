import operator
import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("WordCount") \
    .config("spark.driver.extraClassPath","org.mongodb.spark:mongo-spark-connector_2.11:3.0.0") \
    .getOrCreate()

keywords = ["Storm", "Winter", "Canada", "hot", "cold", "Flu", "Snow", "Indoor", "Safety", "rain", "ice"]

news = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("spark.mongodb.input.uri", "mongodb+srv://harpreet:B00833691@mycluster.ytdpt.mongodb.net/ReuterDb.news").load()

lines = []
for row in news.rdd.collect():
    text = ""
    title = ""
    if row.text is not None:
        text = row.text
    if row.title is not None:
        title = row.title
    line = text +" "+ title
    lines.append(line)

lines = spark.sparkContext.parallelize(lines)
words = lines.flatMap(lambda line: line.split(" ")).map(lambda word: word.strip()).filter(lambda word: word in keywords).map(lambda word: (word,1))
counts = words.reduceByKey(operator.add)
print("ReuterDb:")
print(counts.collect())

search_tweets = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("spark.mongodb.input.uri", "mongodb+srv://harpreet:B00833691@mycluster.ytdpt.mongodb.net/ProcessedDb.search").load()
stream_tweets = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("spark.mongodb.input.uri", "mongodb+srv://harpreet:B00833691@mycluster.ytdpt.mongodb.net/ProcessedDb.stream").load()

lines = []
for row in search_tweets.rdd.collect():
    if row.text is not None:
        lines.append(row.text)
for row in stream_tweets.rdd.collect():
    if row.text is not None:
        lines.append(row.text)

lines = spark.sparkContext.parallelize(lines)
words = lines.flatMap(lambda line: line.split(" ")).map(lambda word: word.strip()).filter(lambda word: word in keywords).map(lambda word: (word,1))
counts = words.reduceByKey(operator.add)
print("ProcessedDb:")
print(counts.collect())
