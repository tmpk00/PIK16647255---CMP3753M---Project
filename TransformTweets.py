#Transform the data collected

import pandas as pd
#Import pymongo library for MognoDB
from pymongo import MongoClient

#vars
#connection parameters for staging db
client = MongoClient('mongodb://localhost:27017')
db = client["ThirdYearProject"]
coll = db["Extract_Tweets"]
newColl = db["Transform_Tweets"]
newAllColl = db["Transform_Tweets_All"]

def truncate(newCol):
    #truncate collection
    newCol.delete_many({})

def allTweets(coll, newColl):
    rows = coll.find({})
    newColl.insert_many(rows)

#collects all rows with none null location field
def collectLocTweets(coll):
    #querys all rows checking col 3 (location column)
    query = {"3":{'$ne': None}}
    findNotNull = coll.find(query)

    return findNotNull

#insert rows into collection
def insertTransRows(rowl, newColl):
    newColl.insert_many(rowl)

#insert single row into collection
def insertTransRow(row, newColl):
    newColl.insert_one(row)

#functions
#insert into transform table
truncate(newColl)
truncate(newAllColl)
allTweets(coll, newAllColl)
rows = collectLocTweets(coll)
rowlist = list(rows)
#check that any rows have been returned
count = 0
for i in rowlist:
    count += 1

if (count == 1):
    
    insertTransRow(i, newColl)

elif (count == 0):
    print("No data")

else:
    insertTransRows(rowlist, newColl)