# -*- coding: utf-8 -*-
"""
Created on Tue Jun 16 20:30:23 2020

@author: James Nelan
"""


# This will import the neccessary modules and set the working directory
import pyodbc
import pandas
import string
import re
import os
import sklearn
os.chdir("\\\\desktop")

# This will store the csv files in a dataframe
dataFrameWithJimmyTweets = pandas.read_csv("jimmy.csv", encoding = "ISO-8859-1")
dataFrameWithStephenTweets = pandas.read_csv("Stephen.csv", encoding = "ISO-8859-1")

# this will create a connection to the database then will store the csv files into the correct tables in SQL database
sqlConnection = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=home.new.db,6395;DATABASE=tweet_database;Trusted_Connection=yes")
cursor = sqlConnection.cursor()

# For loop will iterate through csv file to store the data in the database
for index,row in dataFrameWithJimmyTweets.iterrows():
    cursor.execute("INSERT INTO [jimmyfallon]([created_at],[text],[url],[replies],[retweets],[favorites],[user]) VALUES (?,?,?,?,?,?,?)", row["created_at"], row["text"], row["url"], row["replies"], row["retweets"], row["favorites"], row["user"])

for index,row in dataFrameWithStephenTweets.iterrows():
    cursor.execute("INSERT INTO [StephenAtHome]([created_at],[text],[url],[replies],[retweets],[favorites],[user]) VALUES (?,?,?,?,?,?,?)", row["created_at"], row["text"], row["url"], row["replies"], row["retweets"], row["favorites"], row["user"])    


# This will committ it to the data base and then close the cursor and sql connection
sqlConnection.commit() 
cursor.close()
try:
    sqlConnection.close()
    print("connection closed")
except:
    print("connection already closed")
    
# This will tag the data to know which dataset it is
dataFrameWithJimmyTweets["whichDataset"] = "jimmy"
dataFrameWithStephenTweets["whichDataset"] = "Stephen"

# this will combined the dataset into a single dataframe and will reset the index
dataFrameWithCombinedTweets = dataFrameWithJimmyTweets.append(dataFrameWithStephenTweets)
dataFrameWithCombinedTweets = dataFrameWithCombinedTweets.reset_index()
del(dataFrameWithCombinedTweets["index"])

# this will import the positive and negative word lexicons for the sentiment analysis
with open("positive.txt", "r") as myFile:
    listOfPositiveWords = myFile.read().split("\n")

with open("negative.txt", "r") as myFile:
    listOfNegativeWords = myFile.read().split("\n")
    
# This list will store the overall score
listWithOverallScore = []
    
# this for loop will clean the text data and then perform the sentiment analysis
for index,row in dataFrameWithCombinedTweets.iterrows():
    positiveCounter = 0
    negativeCounter = 0
    # this will store the tweet in a variable
    eachTweet = row["text"]
    
    # this will remove the html
    eachTweetCleaned = re.sub("<.*?>","", eachTweet)
    
    # this will make it lowercase
    eachTweetCleaned = eachTweetCleaned.lower()
    
    # remove punctuation
    eachTweetCleaned = eachTweetCleaned.translate(str.maketrans("","",string.punctuation))
    
    # remove all whitespace characters
    eachTweetCleaned = " ".join(eachTweetCleaned.split())
    
    # remove extra grouped spaces
    eachTweetCleaned = re.sub("\s\s+", " ", eachTweetCleaned)
    
    # split eachTweetCleaned into list of words to count positive and negative words
    listOfWords = eachTweetCleaned.split(" ")
    for eachWord in listOfWords:
            if eachWord in listOfPositiveWords:
                positiveCounter = positiveCounter + 1
            elif eachWord in listOfNegativeWords:
                negativeCounter = negativeCounter + 1
                
    # this will keep track of the overall score
    listWithOverallScore.append(positiveCounter - negativeCounter)
    
# this will add the score the the combined tweet dataframe
dataFrameWithCombinedTweets["score"] = listWithOverallScore

# this will create the connection to the database then store the combined data into the database
sqlConnection = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=home.new.db,6395;DATABASE=tweet_database;Trusted_Connection=yes")
cursor = sqlConnection.cursor()
for index,row in dataFrameWithCombinedTweets.iterrows():
    cursor.execute("INSERT INTO [CombinedTweetsAfterETL]([created_at],[text],[url],[replies],[retweets],[favorites],[user],[whichDataset],[score]) VALUES (?,?,?,?,?,?,?,?,?)", row["created_at"], row["text"], row["url"], row["replies"], row["retweets"], row["favorites"], row["user"], row["whichDataset"], row["score"])
sqlConnection.commit()
cursor.close()
try:
    sqlConnection.close()
    print("connection closed")
except:
    print("connection already closed")
    

# this will implement the Lemmatize function
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
lemmatizer = WordNetLemmatizer()
def nltk2wn_tag(nltk_tag):
  if nltk_tag.startswith('J'):
    return wordnet.ADJ
  elif nltk_tag.startswith('V'):
    return wordnet.VERB
  elif nltk_tag.startswith('N'):
    return wordnet.NOUN
  elif nltk_tag.startswith('R'):
    return wordnet.ADV
  else:                    
    return None

def lemmatize_sentence(sentence):
  nltk_tagged = nltk.pos_tag(nltk.word_tokenize(sentence))    
  wn_tagged = map(lambda x: (x[0], nltk2wn_tag(x[1])), nltk_tagged)
  res_words = []
  for word, tag in wn_tagged:
    if tag is None:                        
      res_words.append(word)
    else:
      res_words.append(lemmatizer.lemmatize(word, tag))
  return " ".join(res_words)

# this will store list of post processed paragraphs
listOfPostProcessedParagraphs = []

# this for loop will clean the data, lemmatize the data and remove stop words
for index,row in dataFrameWithCombinedTweets.iterrows():
    # this will store the text to be cleaned
    eachTweet = row["text"]
    # this will remove HTML
    modifiedTweet = re.sub("<.*?>", "", eachTweet)
    # this will remove punctuation
    modifiedTweet = modifiedTweet.translate(str.maketrans("","",string.punctuation))
    # this will remove white spaces
    modifiedTweet = " ".join(modifiedTweet.split())
    # this will lemmatize the words
    modifiedTweet = lemmatize_sentence(modifiedTweet)
    # this will hold the list of lemmatized words
    listOfLemmatizedWordsWithStopWordsRemoved = []
    # this for loop will remove the stop words and append to lemmatized list
    for word in modifiedTweet.split(" "):
        if word.lower() not in nltk.corpus.stopwords.words("english"):
            listOfLemmatizedWordsWithStopWordsRemoved.append(word)
    
    # this will turn listOfLemmatizedWordsWithStopWordsRemoved into a paragraph
    modifiedTweet = " ".join(listOfLemmatizedWordsWithStopWordsRemoved)
    # this will remove the grouped spaces
    modifiedTweet = re.sub("\s\s+", " ", modifiedTweet)
    # this will add processed data into listOfPostProcessedParagraphs
    listOfPostProcessedParagraphs.append(modifiedTweet)
    
# This will create a Term Document Matrix form listOfPostProcessedParagraphsTwo
cv = sklearn.feature_extraction.text.CountVectorizer()
data = cv.fit_transform(listOfPostProcessedParagraphs)                  
dfTDM = pandas.DataFrame(data.toarray(), columns=cv.get_feature_names())

# This will export the term document matrix into an excel file
dfTDM.to_excel("termDocumentMatrix.xlsx", index = False)


# This will create a list to hold the words of the paragraphs
tokenizedText = []
 
# This for loop will split the sentence apart from the words
for word in " ".join(listOfPostProcessedParagraphs).split(" "):
    tokenizedText.append(word)
    
# this will generate the frequency distribution
nltk.probability.FreqDist(tokenizedText)


# This will generate the 3,5,7 most frequently used terms
nltk.probability.FreqDist(tokenizedText).most_common(3)
nltk.probability.FreqDist(tokenizedText).most_common(5)
nltk.probability.FreqDist(tokenizedText).most_common(7)


# This will generate a frequency distribution plot of 15 most common terms
nltk.probability.FreqDist(tokenizedText).plot(15)
