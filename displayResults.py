#graph results of sentiment analysis and location relations
import seaborn as sns
import matplotlib.pyplot as plt
# Import pymongo library for MognoDB
from pymongo import MongoClient
# import pandas
import pandas as pd
import numpy as np

# connection parameters for db
client = MongoClient('mongodb://localhost:27017')
db = client["ThirdYearProject"]
coll = db["Tweets"]
collAll = db["Tweets_All"]
placeColl = db["Place"]

#get sentiment data
def getData(coll):
    
    rows = coll.find({}, {'_id': 0, 'ID': 1, 'Date': 1, 'Tweet': 1, 'Score': 1, 'Polarity': 1})
    df = pd.DataFrame(list(rows))
    return df

#get place data
def getPlace(place):
    rows = place.find({}, {'_id': 0, 'ID': 1, 'Location': 1, 'Country': 1})
    df = pd.DataFrame(list(rows))
    df = df.rename(columns={"ID": "locID"})
    return df

#creates bar chart of percentages for each polarity
def popularPerc(tweets):
    countPos = 0
    countNeg = 0
    countNeu = 0
    
    for i in tweets['Polarity']:
        if (i == 'positive'):
            countPos += 1
        elif(i == 'negative'):
            countNeg += 1
        else:
            countNeu += 1

    dfPerc = {'polarity': ['positive', 'negative', 'neutral'], 'score': [countPos, countNeg, countNeu]}
    dfPerc = pd.DataFrame(dfPerc, columns = ['polarity', 'score'])
    dfPerc['percentage'] = (dfPerc['score'] / dfPerc['score'].sum()) * 100
    print(dfPerc)

    sns.barplot(x = 'polarity', y = 'percentage', data = dfPerc)
    plt.show()

#shows distribution of scores for each polarity
def distribution(tweet):
    sns.stripplot(x = 'Score', y = 'Polarity', data = tweet)
    plt.show()

#functions
tweets = getData(coll)
alltweets = getData(collAll)
places = getPlace(placeColl)
popularPerc(alltweets)
distribution(alltweets)