from numpy import NaN
import pandas as pd
#Import pymongo library for MognoDB
from pymongo import MongoClient

#vars
#connection parameters for staging db
client = MongoClient('mongodb://localhost:27017')
db = client["ThirdYearProject"]
coll = db["Extract_Place"]
newColl = db["Transform_Place"]

def truncate(newColl):
    #truncate collection
    newColl.delete_many({})

#collects all rows with none null location field
def collectLocTweets(coll):
    #querys all rows checking col 1 (location column)
    query = {"1":{'$ne': NaN}}
    findNotNull = coll.find(query)

    return findNotNull

# selects relevant columns
def setRows(rows):
    place = []
    ids = []
    
    for i in rows:
        places = i['1']
        place.append(places)

        tweetId = i['_id']
        ids.append(tweetId)

    return place, ids

#splits place data to relevent parts
def cleanPlace(place):
    
    location = []
    
    for i in place:
        locData = []

        #place data is a string split by , and =
        #use split to get to relevent parts of place string

        j = i.split(',')
        
        locId = j[1]
        locIdsplit = locId.split('=')
        locIds = locIdsplit[1]
        locData.append(locIds)

        loc = j[4]
        locsplit = loc.split('=')
        locs = locsplit[1]
        locData.append(locs)
        
        country = j[8]
        countrysplit = country.split('=')
        countries = countrysplit[1]
        locData.append(countries)
        
        location.append(locData)

    return location

#inserts data into collection
def insertData(data, coll):

    df = pd.DataFrame(data, columns = ['ID', 'Location', 'Country'])

    coll.insert_many(df.to_dict('records'))


#run functions
truncate(newColl)
rows = collectLocTweets(coll)
place, ids = setRows(rows)
locData = cleanPlace(place)

count = 0
for i in locData:
    count += 1

if (count >= 1):
    insertData(locData, newColl)