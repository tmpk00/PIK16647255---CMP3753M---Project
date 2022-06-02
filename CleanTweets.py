# Cleans collected tweets
import pandas as pd
# Import pymongo library for MognoDB
from pymongo import MongoClient

#import nltk for stopword removal
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

import re

# connection parameters for db
client = MongoClient('mongodb://localhost:27017')
db = client["ThirdYearProject"]
coll = db["Transform_Tweets"]
allColl = db["Transform_Tweets_All"]
newColl = db["Clean_Tweets"]
newallColl = db["Clean_Tweets_All"]

def truncate(newColl, newallColl):
    #truncate collection
    newColl.delete_many({})
    newallColl.delete_many({})
    
def getData(coll):
    # only returns the tweet column
    rows = coll.find({}, {'1': 1, '2': 1})
    
    # creates list of tweets 
    tweets = []
    ids = []
    dates = []

    # iterates through queried rows
    for i in rows:
        tweet = i['2']
        tweets.append(tweet)

        tweetId = i['_id']
        ids.append(tweetId)

        tweetDate = i['1']
        dates.append(tweetDate)
    
    #returns tweet text, id, and date
    return tweets, ids, dates

def removeStops(data):
    #stopwords
    stop_words = set(stopwords.words('english'))
    #list to be populated with tweets with stopwords removed
    new_tweets = []

    # foreach tweet
    for i in data:
        tweet = i
        tweet = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())
        tweet_tokens = word_tokenize(tweet)

        rem_tweet = []
        for t in tweet_tokens:
            if t not in stop_words:
                rem_tweet.append(t)

        stop_tweet = ''
        for j in rem_tweet:
            stop_tweet += j + ' '

        new_tweets.append(stop_tweet)

    return new_tweets


def insertData(data, inp_id, dates, coll):

    #create tuples
    tuples = list(zip(inp_id, dates, data))

    #create df from tuples
    df = pd.DataFrame(tuples, columns = ['ID', 'Date', 'Tweet'])

    coll.insert_many(df.to_dict('records'))


#run functions
truncate(newColl, newallColl)

tweetData, tweetId, tweetDate = getData(coll)

#check rows have been returned
count = 0
for i in tweetData:
    count += 1

if (count>= 1):
    tweetWords = removeStops(tweetData)
    insertData(tweetWords, tweetId, tweetDate, newColl)

tweetAllData, tweetAllId, tweetAllDate = getData(allColl)
tweetAllWords = removeStops(tweetAllData)
insertData(tweetAllWords, tweetAllId, tweetAllDate, newallColl)