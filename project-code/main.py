import matplotlib
matplotlib.use('Agg')

from textblob import TextBlob
import numpy as np
import time
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import count, col
import matplotlib.pyplot as plt
import os
import re
from os import path
from wordcloud import WordCloud
from pyspark.sql.functions import udf



#start time for benchmarking.
start = time.time()

if os.path.isfile('Overview.txt'):
    os.remove('Overview.txt')

g = open('Overview.txt','w')

def convert_to_ascii(unicodestring):
    return unicodestring.encode("ascii","ignore")

#patterns utilized in reg_express function.
patterns = ['http\S+', '@\S+', 'RT','[^A-Za-z0-9_]+']

#NLP, removes links, usernames, the text RT, and hashtags.
def reg_express(ascii):
    for i in patterns:
        pattern = re.compile(i)
        ascii = re.sub(pattern," ",ascii)
    return ascii
    

#create a spark session.
spark = SparkSession.builder.appName("Twitter Sentiment").getOrCreate()

#importing twitter json dataset.
tweets = spark.read.json("file:/root/spark-2.3.0-bin-hadoop2.7/bin/obamcare2018_1.json")

#counts all the distinctIDs, every tweet has a unique ID thus it counts all tweets.
x = tweets.select(countDistinct('id')).collect()[0][0]
overall_statement = 'There are '+ str(x) +' tweets in this dataset.'

g.write(overall_statement)

#Counts all distinct texts, Retweets will not be distinct thus are filtered out and only unique tweets are counts.
y = tweets.select(countDistinct('text')).collect()[0][0]

unique_statement = '\nThere are '+ str(y) +' unique tweets in this dataset.'
g.write(unique_statement)
z = x - y 

retweet_statement = '\nThere are '+ str(z) +' retweets in this dataset.'
g.write(retweet_statement)

all_text = tweets.select('text')

converted_udf = udf(convert_to_ascii)
converted_express = udf(reg_express)
converted_ascii = all_text.select('text').withColumn('converted', converted_udf(all_text.text))

#--------------------------------textblob processing--------------------------------------

def sentiment_analysis(sentence):
    blob = TextBlob(sentence)
    sentiment = blob.sentiment.polarity
    return sentiment

#assign sentiment analysis a user defined function.
sentiment_udf = udf(sentiment_analysis)

#select all text converted from unicode to ascii.
all_processed_text = converted_ascii.select('converted')

#processes text with regular expression NLP.
converted_all = all_processed_text.select('converted').withColumn('textconvert', converted_express(all_processed_text.converted))

#creates a datframe containing processed text and polarity of tweet.
sentiment_table = converted_all.select('textconvert').withColumn('Polarity', sentiment_udf(converted_all.textconvert))

tweet_pol = sentiment_table.collect()

hist_polarity_data = sentiment_table.select('Polarity').collect()

#write all sentiment data and text to a table
if os.path.isfile('twitter_sentiment_list.txt'):
    os.remove('twitter_sentiment_list.txt')
    
h = open('twitter_sentiment_list.txt', 'w')
    
for row in tweet_pol:
    sen = row[0]
    value = row[1]
    h.write('\n'+sen+' , '+value)
h.close()


#most popular retweet
  
filtered_RTs = converted_ascii.filter(converted_ascii.converted.like("%RT%"))
most_popular_retweet = filtered_RTs.select('converted').collect()[0][0]

most_pop = '\nThe most popular retweet is: '+ str(most_popular_retweet)
g.write(most_pop)



#selects all text that is unique in the dataset.
unique_text = converted_ascii.select('converted').distinct()
converted_unique = unique_text.select('converted').withColumn('textconvert', converted_express(unique_text.converted)) 
processed_unique_text = converted_unique.select('textconvert').collect()





#if file is present delete it,(useful when we rerunning script).
if os.path.isfile('processed_text.txt'):
    os.remove('processed_text.txt')
if os.path.isfile('ACAtextcloud.png'):
    os.remove('ACAtextcloud.png')
if os.path.isfile('Polarity_histogram.png'):
    os.remove('Polarity_histogram.png')
if os.path.isfile('polarity_list.txt'):
    os.remove('polarity_list.txt')
	
	
#add all filtered text of tweets into a single text file.
f = open('processed_text.txt', 'w')
for row in processed_unique_text:
    sentence = row[0]
    f.write('\n'+sentence)
f.close()


# Read the whole text of previously processed tweets.
text = open('processed_text.txt').read()

# Generate a word cloud image
wordcloud = WordCloud().generate(text)


# word cloud parameters.
wordcloud = WordCloud(max_font_size=40).generate(text)

plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")
plt.close()

#save wordcloud as image.
plt.savefig("ACAtextcloud.png")

zz = open('polarity_list.txt','w')
polarity_list = []

for i in hist_polarity_data:
    w = i[0]
    polarity_list.append(float(w))
    zz.write('\n'+w)
zz.close()

mean_all = '\nThe mean polarity for sentiment of this dataset is '+str(np.mean(polarity_list))+'.'

g.write(mean_all)

g.close()


plt.hist(polarity_list)
plt.title("All Polarities")
plt.xlabel("Sentiment")
plt.ylabel("Number of tweets")
plt.savefig("Polarity_histogram.png")



end = time.time()
print(round(end - start,2),"seconds")

