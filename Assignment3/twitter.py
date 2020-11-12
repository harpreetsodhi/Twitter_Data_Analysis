import constants
import tweepy
from pymongo import MongoClient
import re


class StreamListener(tweepy.StreamListener):
    def __init__(self, api=None):
        super().__init__(api)
        self.count = 0

    def on_status(self, status):
        if status.lang != "en":
            return

        self.count += 1

        if self.count == constants.NO_OF_STREAM_TWEETS:
            self.stream_obj.disconnect()

        tweet = process_tweet(status)
        self.raw_db_obj.stream.insert_one(tweet)
        tweet = clean_tweet(tweet)
        self.processed_db_obj.stream.insert_one(tweet)

    def on_error(self, status_code):
        if status_code == 420:
            return False


def connect_to_db():
    client = MongoClient(constants.CONNECTION_STRING)
    raw_db_obj = client.RawDb
    processed_db_obj = client.ProcessedDb
    return raw_db_obj, processed_db_obj


def connect_to_twitter():
    auth = tweepy.OAuthHandler(constants.API_KEY, constants.API_SECRET_KEY)
    auth.set_access_token(constants.ACCESS_TOKEN, constants.ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth)
    return api


def process_tweet(status):
    tweet = {
        "tweet_id": status.id,
        "text": status.text,
        "tweet_created_at": status.created_at,
        "tweet_location": status.coordinates,
        "user_id": status.user.id,
        "user_name": status.user.name,
        "user_location": status.user.location,
        "user_description": status.user.description,
        "user_created_at": status.user.created_at,
        "retweet_count": status.retweet_count,
        "favorite_count": status.favorite_count
    }
    return tweet


def clean_tweet(tweet):
    if tweet["text"] is not None:
        tweet["text"] = remove_symbols(tweet["text"])
    if tweet["user_name"] is not None:
        tweet["user_name"] = remove_symbols(tweet["user_name"])
    if tweet["user_location"] is not None:
        tweet["user_location"] = remove_symbols(tweet["user_location"])
    if tweet["user_description"] is not None:
        tweet["user_description"] = remove_symbols(tweet["user_description"])
    return tweet


def remove_symbols(tweet):
    # to remove HASHTAGS, MENTIONS, PUNCTUATION and EMOJIS
    tweet = ' '.join(re.sub("([^0-9A-Za-z \t]+)|(\w+:\/\/\S+)", " ", tweet).split())

    # to remove links that start with HTTP/HTTPS in the tweet
    tweet = re.sub(r'https?:\/\/(www\.)?[-a-zA-Z0–9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0–9@:%_\+.~#?&//=]*)', '', tweet, flags=re.MULTILINE)

    # to remove other url links
    tweet = re.sub(r'[-a-zA-Z0–9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0–9@:%_\+.~#?&//=]*)', '', tweet, flags=re.MULTILINE)
    return tweet


def save_search_api_data(raw_db_obj, processed_db_obj, api):
    for status in tweepy.Cursor(api.search, q=constants.SEARCH_TERMS, lang="en").items(constants.NO_OF_SEARCH_TWEETS):
        tweet = process_tweet(status)
        raw_db_obj.search.insert_one(tweet)
        tweet = clean_tweet(tweet)
        processed_db_obj.search.insert_one(tweet)


def save_stream_api_data(raw_db_obj, processed_db_obj, api):
    stream_listener = StreamListener(api)
    stream_obj = tweepy.Stream(auth=api.auth, listener=stream_listener)
    stream_listener.stream_obj = stream_obj
    stream_listener.raw_db_obj = raw_db_obj
    stream_listener.processed_db_obj = processed_db_obj
    stream_obj.filter(track=constants.TWEET_FIELDS)


def main():
    raw_db_obj, processed_db_obj = connect_to_db()
    api = connect_to_twitter()
    save_search_api_data(raw_db_obj, processed_db_obj, api)
    save_stream_api_data(raw_db_obj, processed_db_obj, api)


if __name__ == "__main__":
    main()
