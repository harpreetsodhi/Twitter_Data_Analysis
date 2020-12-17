import math
from pymongo import MongoClient
from tabulate import tabulate
import re

client = MongoClient("mongodb+srv://harpreet:B00833691@mycluster.ytdpt.mongodb.net")
reuter_db = client.ReuterDb
news_collection = reuter_db.news

news = []

for news_article in news_collection.find():
    text = ""
    if news_article['text'] is not None:
        text = news_article['text'].lower()
    if news_article['title'] is not None:
        if text == "":
            text = news_article['title'].lower()
        else:
            text += " " + news_article['title'].lower()
    text = re.sub(r"\s+", " ", text)
    text = re.sub("\n", " ", text)
    news.append(text)

documents = []
document_id = 1
for news_article in news:
    canada_count = news_article.count("canada")
    rain_count = news_article.count("rain")
    cold_count = news_article.count("cold")
    hot_count = news_article.count("hot")
    documents.append({"id": document_id, "document": news_article,
                      "values": (canada_count, rain_count, cold_count, hot_count)})
    document_id += 1

total_documents = len(news)
canada_appeared = list(filter(lambda x: x["values"][0] != 0, documents))
rain_appeared = list(filter(lambda x: x["values"][1] != 0, documents))
cold_appeared = list(filter(lambda x: x["values"][2] != 0, documents))
hot_appeared = list(filter(lambda x: x["values"][3] != 0, documents))

canada = ["canada", len(canada_appeared), total_documents / len(canada_appeared),
          math.log10(total_documents / len(canada_appeared))]
rain = ["rain", len(rain_appeared), total_documents / len(rain_appeared),
        math.log10(total_documents / len(rain_appeared))]
cold = ["cold", len(cold_appeared), total_documents / len(cold_appeared),
        math.log10(total_documents / len(cold_appeared))]
hot = ["hot", len(hot_appeared), total_documents / len(hot_appeared), math.log10(total_documents / len(hot_appeared))]

print("\nNo of documents:", total_documents)
print(tabulate([canada, rain, cold, hot], headers=['Search Query', 'Document containing term(df)',
                                                   'total documents(N)/ number of documents term appeared (df)',
                                                   "Log(N/df)"]))

canada_documents = list(filter(lambda x: x["values"][0] != 0, documents))
canada_articles = [(x["id"], len(x["document"]), x["document"].count("canada")) for x in canada_documents]
print(tabulate(canada_articles, headers=['Article No', 'Total words(m)', 'Frequency(f)']))

idx_max_occurrence_canada = max(range(len(canada_articles)), key=lambda x: canada_articles[x][2])
print("Highest frequency of word Canada is:", canada_articles[idx_max_occurrence_canada][2])
print("Document with highest frequency of word Canada is:",
      documents[canada_articles[idx_max_occurrence_canada][0]-1]["document"])

idx_max_relative_frequency_canada = max(range(len(canada_articles)),
                                        key=lambda x: canada_articles[x][2] / canada_articles[x][1])
print("Highest relative frequency of word Canada is:", canada_articles[idx_max_relative_frequency_canada][2] /
      canada_articles[idx_max_relative_frequency_canada][1])
print("Document with highest relative frequency of word Canada is:",
      documents[canada_articles[idx_max_relative_frequency_canada][0]-1]["document"])
