# file for train dataset to be labelled
# Import pymongo library for MognoDB
from pymongo import MongoClient

#import textblob for labelling dataset
from textblob import TextBlob


# connection parameters for db
client = MongoClient('mongodb://localhost:27017')
db = client["ThirdYearProject"]
coll = db["Clean_Tweets"]
collAll = db["Clean_Tweets_All"]
newcoll = db["Sentiment_Tweets"]
newCollAll = db["Sentiment_Tweets_All"]

#empty list of analysed data
analysis = []
analysisAll = []

def truncate(newCol):
    #truncate collection
    newCol.delete_many({})

def getData(coll):
    # only returns the tweet column
    rows = coll.find({}, {'_id': 0, 'ID': 1, 'Date': 1, 'Tweet': 1})
    
    # creates list of tweets 
    tweets = []
    ids = []
    dates = []

    # iterates through queried rows
    for i in rows:
        tweet = i['Tweet']
        tweets.append(tweet)

        tweetId = i['ID']
        ids.append(tweetId)

        tweetDate = i['Date']
        dates.append(tweetDate)
    
    return tweets, ids, dates

#asigns sentiment scores
def getSentiment(tweet):

    # using textblob sentiment score
    senti = TextBlob(tweet)
    score = senti.sentiment.polarity

    if score > 0:
        polarity = 'positive'
    elif score < 0:
        polarity = 'negative'
    else:
        polarity = 'neutral'

    return score, polarity

#insert single row into collection
def insertRows(data, coll):
    for i in data:
        coll.insert_one(i)


#run functions
truncate(newcoll)
tweetData, tweetId, tweetDate = getData(coll)

#run analysis one tweet at a time
count = 0
for i in tweetData:
    sentimentData = {}

    sentimentData['ID'] = tweetId[count]

    sentimentData['Date'] = tweetDate[count]
    
    sentimentData['Tweet'] = i

    score, polarity = getSentiment(i)

    sentimentData['Score'] = score

    sentimentData['Polarity'] = polarity

    analysis.append(sentimentData)

    count += 1
#check if any rows have been returned
if (count >= 1):
    insertRows(analysis, newcoll)

truncate(newCollAll)
tweetAllData, tweetAllId, tweetAllDate = getData(collAll)

#run analysis one tweet at a time
count = 0
for i in tweetAllData:
    sentimentData = {}

    sentimentData['ID'] = tweetAllId[count]

    sentimentData['Date'] = tweetAllDate[count]
    
    sentimentData['Tweet'] = i

    score, polarity = getSentiment(i)

    sentimentData['Score'] = score

    sentimentData['Polarity'] = polarity

    analysisAll.append(sentimentData)

    count += 1

insertRows(analysisAll, newCollAll)