API_KEY = 'Dz3O1fhHtYWdPzmdRIpPt3VLs'
API_SECRET_KEY = 'q55qnbJObFEJx7x2pxklwXJ1baGywL52XJpeR93Z684gY1Gfyt'
BEARER_TOKEN = 'AAAAAAAAAAAAAAAAAAAAABMNJgEAAAAAaw0p0mDUkdOY%2FoUsqWeRtgOStZI%3DBG4ExhlMcmLxyMwhFobXj1f6ohJ7Qah081wlo0kM9NXRTbHXcP'
ACCESS_TOKEN = '1324077615205015559-1bujtnY769gySzqx8CIlYTdrH3wrR5'
ACCESS_TOKEN_SECRET = 'NA12CiCOBgRBkiV2x9nTNPDO93BWoUDmaJcDF3gq3wV77'
TWEET_FIELDS = ["Storm", "Winter", "Canada", "Temperature", "Flu", "Snow", "Indoor", "Safety"]
SEARCH_TERMS = "Storm OR Winter OR Canada OR Temperature OR Flu OR Snow OR Indoor OR Safety"
CONNECTION_STRING = "mongodb+srv://harpreet:B00833691@mycluster.ytdpt.mongodb.net/test"
FREQUENCY_TERMS = ["storm", "winter", "canada", "hot", "cold", "flu", "snow", "indoor", "safety", "rain", "ice"]
NO_OF_STREAM_TWEETS = 2000
NO_OF_SEARCH_TWEETS = 2000

data = [('Magnetics', 5), ('Storm', 24), ('Canada', 1)]
values = filter(lambda x: x[0] in FREQUENCY_TERMS, data)
print(list(values))
