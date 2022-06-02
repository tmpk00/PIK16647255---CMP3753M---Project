#predictions about sentiment based on location of tweet

# Import pymongo library for MognoDB
from pymongo import MongoClient

# import pandas
import pandas as pd
from sklearn.metrics import accuracy_score

# ML model
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import MultinomialNB
from sklearn import metrics

from sklearn.feature_extraction.text import CountVectorizer
from nltk.tokenize import RegexpTokenizer

# connection parameters for db
client = MongoClient('mongodb://localhost:27017')
db = client["ThirdYearProject"]
coll = db["Tweets"]
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

#combine to create dataset
def setDf(data, place):
    df = data.join(place)
    return df

#split dataset
def trainTestSplitLoc(data):
    # split dataset between train and test data
    
    token = RegexpTokenizer(r'[a-zA-Z0-9]+')
    cv = CountVectorizer(stop_words = 'english', ngram_range = (1,1), tokenizer = token.tokenize)
    
    #define x and y inputs
    x = cv.fit_transform(data['Location'])
    y = data['Score']
    y = y.astype('int')

    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size = 0.5)

    return x_train, x_test, y_train, y_test

def trainTestSplitCountry(data):
    # split dataset between train and test data
    
    token = RegexpTokenizer(r'[a-zA-Z0-9]+')
    cv = CountVectorizer(stop_words = 'english', ngram_range = (1,1), tokenizer = token.tokenize)    
    
    #define x and y inputs
    x = cv.fit_transform(data['Country'])
    y = data['Score']
    y = y.astype('int')

    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size = 0.5)

    return x_train, x_test, y_train, y_test

#perform analysis
def locAnalysis(x_train, x_test, y_train, y_test):
    
    #use multinomial naive bayes classifier to predict sentiment scores
    mnnb = MultinomialNB()
    mnnb.fit(x_train, y_train)
    y_pred = mnnb.predict(x_test)
    accuracy_score = metrics.accuracy_score(y_pred, y_test)

    return accuracy_score

def countryAnalysis(x_train, x_test, y_train, y_test):
    
    #use multinomial naive bayes classifier to predict sentiment scores
    mnnb = MultinomialNB()
    mnnb.fit(x_train, y_train)
    y_pred = mnnb.predict(x_test)
    accuracy_score = metrics.accuracy_score(y_pred, y_test)

    return accuracy_score

#run functions
senti = getData(coll)
place = getPlace(placeColl)
data = setDf(senti, place)
trainxLoc, testxLoc, trainyLoc, testyLoc = trainTestSplitLoc(data)
trainxCou, testxCou, trainyCou, testyCou = trainTestSplitCountry(data)
locAcc = locAnalysis(trainxLoc, testxLoc, trainyLoc, testyLoc)
countryAcc = countryAnalysis(trainxCou, testxCou, trainyCou, testyCou)

#display accuracy scores
print("Location Accuracy: ", locAcc)
print("Country Accuracy: ", countryAcc)