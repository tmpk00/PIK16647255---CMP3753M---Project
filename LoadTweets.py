#Load the tweets into the "Main" table

import pandas as pd
#Import pymongo library for MognoDB
from pymongo import MongoClient
#json conversion needed for mongodb load
import json

#vars
#connection parameters for staging db
client = MongoClient('mongodb://localhost:27017')
db = client["ThirdYearProject"]
TweetColl = db["Sentiment_Tweets"]
TweetAllColl = db["Sentiment_Tweets_All"]
PlaceColl = db["Transform_Place"]

newTweetColl = db["Tweets"]
newTweetAllColl = db["Tweets_All"]
newPlaceColl = db["Place"]

def getTrans(coll):
    #gets all rows from transform table
    findall = coll.find()
    return findall

def loadTab(rows, newColl):
    #inserts all rows from transform
    newColl.insert_many(rows)

def loadRow(row, newColl):
    #inserts single row
    newColl.insert_one(row)

#functions
rows = getTrans(TweetColl)
rowlist = list(rows)
count = 0

#check for number of location tweet rows
for i in rowlist:
    count += 1
if (count > 1):
    loadTab(rowlist, newTweetColl)
elif(count == 1):
    loadRow(i, newTweetColl)

rows = getTrans(TweetAllColl)
loadTab(rows, newTweetAllColl)

rows = getTrans(PlaceColl)
rowlist = list(rows)
count = 0

for i in rowlist:
    count += 1
if (count > 1):
    loadTab(rowlist, newPlaceColl)
elif (count == 1):
    loadRow(i, newPlaceColl)