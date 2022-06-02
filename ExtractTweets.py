#2021-10-29
#Script to extract data from Twitter in the form of replies and mentions of notable British politicians
#Load raw into MongoDB table

#Import Tweepy library to use TwitterApi
import tweepy
#import pandas
import pandas as pd
#Import pymongo library for MognoDB
from pymongo import MongoClient
#json conversion needed for mongodb load
import json

from dotenv import load_dotenv
import os

load_dotenv()
consumer_key = os.getenv('consumer_key')
consumer_secret = os.getenv('consumer_secret')
access_token = os.getenv('access_token')
access_token_secret = os.getenv('access_token_secret')

#connection parameters for db
client = MongoClient('mongodb://localhost:27017')
db = client["ThirdYearProject"]
coll = db["Extract_Tweets"]
pcoll = db["Extract_Place"]

#truncate table
def truncate(col):
    #truncate collection
    col.delete_many({})

#collect tweets based on specific key word(s)
def scrape():
#set up authourisation with TwitterAPI
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)

    #find tweets featuring 'Boris Johnson', filter out retweets
    text_query = 'Boris Johnson -filter:retweets'
    #number of tweets to be collected
    count = 100
    tweets_list = []
    place_list = []

    #create query
    tweets = tweepy.Cursor(api.search_tweets, q = text_query, lang = 'en').items(count)
    #specify the info we want from tweets
    for tweet in tweets:
       tweets_list.append([tweet.id, str(tweet.created_at), tweet.text, tweet.place])
       place_list.append([tweet.id, tweet.place])

    #create dataframe of gathered tweets
    df = pd.DataFrame(tweets_list)
    df1 = pd.DataFrame(place_list)
    #return df
    return df, df1

def dataInsert(df, coll):
    #convert dataframe to json format for mongodb compatability
    df_json = json.loads(df.to_json(orient='records'))

    #insert into mongodb
    coll.insert_many(df_json)

def csv_to_json(filename, header):
    data = pd.read_csv(filename, header=header)
    return data.to_dict('records')

def insert_CSV(data, coll):
    
    coll.insert_many(csv_to_json('scraped1.csv', header = 0))

#truncate table
truncate(coll)
truncate(pcoll)

#run functions
scraped, scraped1 = scrape()

#create CSV to retain Place object details
scraped1.to_csv('scraped1.csv')


#Insert data into mongodb 
dataInsert(scraped, coll)
insert_CSV(scraped1, pcoll)