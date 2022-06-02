#2021-10-29
#main project file
#Run Scripts in order
import ExtractTweets
ExtractTweets
print("Extract Done!")

import TransformTweets
TransformTweets
print("Transform Done!")

import CleanTweets
CleanTweets
print("Clean Done!")

import TransformPlace
TransformPlace
print("Transform Done!")

import Sentiment
Sentiment
print("Sentiment Done!")

import LoadTweets
LoadTweets
print("Load Done!")