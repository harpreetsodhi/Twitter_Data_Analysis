import re
import os
from pymongo import MongoClient
import constants


class ReutersParser:

    def __init__(self, db):
        self.bad_keywords_pattern = re.compile(r"&#\d;")
        self.extra_new_lines_pattern = re.compile(r"\n+")
        self.reuters_pattern = re.compile(r"<REUTERS.*?<\/REUTERS>", re.S)
        self.places_pattern = re.compile(r"<D.*?<\/D>", re.S)
        self.db = db

    def parse(self, data):
        # clean data
        data = self.bad_keywords_pattern.sub('', data)
        data = self.extra_new_lines_pattern.sub('\n', data)

        all_reuters = self.reuters_pattern.findall(data)
        for reuter in all_reuters:
            date = re.search(r"<DATE.*?<\/DATE>", reuter, re.S)

            reuter_places = re.search(r"<PLACES.*?<\/PLACES>", reuter, re.S)
            reuter_places = self.places_pattern.findall(reuter_places.group())
            places = []
            for place in reuter_places:
                places.append(place[3:-4])
            if len(places) == 0:
                places = None

            if date is not None:
                date = date.group()[6:-7]
            text = re.search(r"<TEXT.*?<\/TEXT>", reuter, re.S)
            title = re.search(r"<TITLE.*?<\/TITLE>", text.group(), re.S)
            if title is None:
                text = re.search(r">.*<\/TEXT>", text.group(), re.S)
                text = text.group()[1:-7]
            else:
                body = re.search(r"<BODY.*?<\/BODY>", text.group(), re.S)
                title = title.group()[7:-8]
                if body is not None:
                    text = body.group()[6:-7]
                else:
                    text = None

            news_article = {
                "text": text,
                "title": title,
                "date": date,
                "places": places
            }
            self.db.news.insert_one(news_article)

    def read_data(self, filepath):
        reuters_data = ''
        for line in open(filepath, 'rb').readlines():
            line = line.decode('utf-8', 'ignore')
            reuters_data += line+"\n"
        return self.parse(reuters_data)


def connect_to_db():
    client = MongoClient(constants.CONNECTION_STRING)
    db = client.ReuterDb
    return db


def main():
    db = connect_to_db()
    parser = ReutersParser(db)
    dirname = os.path.dirname(__file__)
    dirname = os.path.join(dirname, 'reuter_files')
    for filename in os.listdir(dirname):
        filepath = os.path.join(dirname, filename)
        parser.read_data(filepath)


if __name__ == "__main__":
    main()
