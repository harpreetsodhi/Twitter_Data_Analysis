from pymongo import MongoClient
import csv

positive_words = open("positive_words.txt", "r").read().splitlines()
negative_words = open("negative_words.txt", "r").read().splitlines()

client = MongoClient("mongodb+srv://harpreet:B00833691@mycluster.ytdpt.mongodb.net")
twitter_db = client.ProcessedDb
stream_collection = twitter_db.stream

tweets = []
bows = []

for tweet in stream_collection.find():
    if tweet['text'] is not None:
        tweets.append(tweet['text'])

for tweet in tweets:
    bow = {}
    tweet_words = tweet.split(" ")
    for word in tweet_words:
        if word in bow.keys():
            bow[word] += 1
        else:
            bow[word] = 1
    bows.append(bow)

result = []
count = 1
for bow in bows:
    polarity = 0
    match_set = set()
    for key, value in bow.items():
        if key in positive_words:
            polarity += value
            match_set.add(key)
        if key in negative_words:
            polarity -= value
            match_set.add(key)
    if polarity == 0:
        sentiment = "neutral"
    elif polarity > 0:
        sentiment = "positive"
    elif polarity < 0:
        sentiment = "negative"
    result.append({"tweet": count, "message": ' '.join(bow.keys()), "match": ', '.join(match_set), "polarity": sentiment})
    count += 1

column_names = ['tweet', 'message', 'match', "polarity"]

with open('sentiment.csv', 'w', newline='') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=column_names)
    writer.writeheader()
    writer.writerows(result)
