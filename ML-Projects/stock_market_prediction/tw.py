import pymysql

import re
import tweepy
# import nltk
from tweepy import OAuthHandler
from textblob import TextBlob
import pandas as pd
from hdfs import InsecureClient
import os

client_hdfs = InsecureClient('http://localhost:50070')

connection = pymysql.connect(host="localhost", user="root", password="", database="110twittersentiment")
cursor = connection.cursor()

class TwitterClient:
    '''
    Generic Twitter Class for sentiment analysis.
    '''

    def __init__(self):
        '''
        Class constructor or initialization method.
        '''
        # keys and tokens from the Twitter Dev Console
        consumer_key = '5L4iF101tBHb0vVUQ7uCph3LR'
        consumer_secret = 'kKCDjgvrIO012yCAE8FCsL6kcHDo344i0SJjSlm4FG2YxL7x5f'
        access_token = '2842121736-dv73nAcb76ssBtHt0YSimalWRnvOiwnyXeEE9SW'
        access_token_secret = 'MgeXZivCLXglBxxAjtPafsveVMQiJLeSTn82zCKm3JnpB'

        # attempt authentication
        try:
            # create OAuthHandler object
            self.auth = OAuthHandler(consumer_key, consumer_secret)
            # set access token and secret
            self.auth.set_access_token(access_token, access_token_secret)
            # create tweepy API object to fetch tweets
            self.api = tweepy.API(self.auth)
        except:
            print("Error: Authentication Failed")

    def clean_tweet(self, tweet):
        '''
        Utility function to clean tweet text by removing links, special characters
        using simple regex statements.
        '''
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

    def get_tweet_sentiment(self, tweet):
        '''
        Utility function to classify sentiment of passed tweet
        using textblob's sentiment method
        '''
        # create TextBlob object of passed tweet text
        analysis = TextBlob(self.clean_tweet(tweet))
        # set sentiment
        if analysis.sentiment.polarity > 0:
            return 'positive'
        elif analysis.sentiment.polarity == 0:
            return 'neutral'
        else:
            return 'negative'
import tweepy 
  
# Fill the X's with the credentials obtained by  
# following the above mentioned procedure. 
consumer_key = '5L4iF101tBHb0vVUQ7uCph3LR'
consumer_secret = 'kKCDjgvrIO012yCAE8FCsL6kcHDo344i0SJjSlm4FG2YxL7x5f'
access_key = '2842121736-dv73nAcb76ssBtHt0YSimalWRnvOiwnyXeEE9SW'
access_secret = "MgeXZivCLXglBxxAjtPafsveVMQiJLeSTn82zCKm3JnpB"
  
# Function to extract tweets 
def get_tweets(username): 
          
        # Authorization to consumer key and consumer secret 
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret) 
  
        # Access to user's access key and access secret 
        auth.set_access_token(access_key, access_secret) 
  
        # Calling api 
        api = tweepy.API(auth) 
  
        # 200 tweets to be extracted 
        number_of_tweets=200
        tweets = api.user_timeline(screen_name=username) 
  
        # Empty Array 
        tmp=[]  
  
        # create array of tweet information: username,  
        # tweet id, date/time, text 
        tweets_for_csv = [tweet.text for tweet in tweets] # CSV file created  
        for j in tweets_for_csv: 
  
            # Appending tweets to the empty array tmp 
            tmp.append(j)  
  
        # Printing the tweets 
        print(tmp) 
        return tmp
    
    def main(self, username):
        global df
        # creating object of TwitterClient Class
        # inp = input("You: ")
        api = TwitterClient()
        print('hi')

        # calling function to get tweets
        query = username
        print('query')

        tweets = .get_tweets(query, count=200)
        print("hii")
        # cursor.execute("insert into testtable2(id) values('"+tweet+"')")

        # picking positive tweets from tweets
        ptweets = [tweet for tweet in tweets if tweet['sentiment'] == 'positive']
        # percentage of positive tweets
        positive = (format(100 * len(ptweets) / len(tweets)))
        print("Positive tweets percentage: {} %".format(100 * len(ptweets) / len(tweets)))
        # picking negative tweets from tweets
        ntweets = [t for t in tweets if t['sentiment'] == 'negative']
        # percentage of negative tweets
        negative = (format(100 * len(ntweets) / len(tweets)))
        print("negative tweets percentage:", negative)
        # percentage of neutral tweets
        neutral = (format(100 * (len(tweets) - len(ntweets) - len(ptweets)) / len(tweets)))
        print("Neutral tweets percentage: {} %".format(100 * (len(tweets) - len(ntweets) - len(ptweets)) / len(tweets)))



        # printing first 5 positive tweets
        print("\n\n tweets:")

        for tweet2 in ptweets[:100]:
            print(query)
            print(tweet2['text'])
            positivedata=(tweet2['text'])
          
            sql = "INSERT INTO tablename1 (name,positive) VALUES (%s,%s)"
            val = (query,positivedata )
            cursor.execute(sql, val)

            connection.commit()

            #sql = "INSERT INTO testtable2 (Id, Name) VALUES (%s,%s )"
            #print(sql)
            #val = (tweet2['text'], query)

            #cursor.execute(sql, val)
            # cursor.execute("insert into testtable2(id) values('"+tweet+"')")

            # print(tweet['text'])

        # printing first 5 negative tweets

        # if query== query:

        # sql = "DELETE  FROM testtable WHERE Name = '"+query+"'"
        # print(sql)

        if query == query:

            for tweet3 in ntweets[:100]:
                print(query)
                print(negative)
                print(tweet3['text'])
                print(tweet2['text'])


                sql = "INSERT INTO tweeterdata ( negative,twwetname,nsrate,psrate,neutral) VALUES (%s,%s,%s,%s ,%s)"
                val = ( tweet3['text'], query, negative,positive,neutral)
                cursor.execute(sql, val)

                connection.commit()

                #sql = "INSERT INTO testtable (tweets, Name,negative) VALUES (%s,%s,%s )"
                #val = (tweet['text'], query, negative)

                #cursor.execute(sql, val)
                # cursor.execute("insert into testtable2(id) values('"+tweet+"')")

                # print(tweet['text'])
                #
                # cursor.execute("insert into testtable2 (id) VALUES (%s)",(tweets['text']))
                # cursor.execute("insert into testtable2(id) values('"+tweet+"')")

def main(inp):
    # creating object of TwitterClient Class
    #inp = input("You: ")
    api = TwitterClient()

    # calling function to get tweets
    query = inp

    tweets = api.get_tweets(query, count=200)

    # cursor.execute("insert into testtable2(id) values('"+tweet+"')")

    # picking positive tweets from tweets
    ptweets = [tweet for tweet in tweets if tweet['sentiment'] == 'positive']
    # percentage of positive tweets
    print("Positive tweets percentage: {} %".format(100 * len(ptweets) / len(tweets)))
    # picking negative tweets from tweets
    ntweets = [t for t in tweets if t['sentiment'] == 'negative']
    # percentage of negative tweets
    negative = (format(100 * len(ntweets) / len(tweets)))
    print("negative tweets percentage:", negative)
    # percentage of neutral tweets
    print("Neutral tweets percentage: {} %".format(100 * (len(tweets) - len(ntweets) - len(ptweets)) / len(tweets)))
    labels = 'Python', 'C++', 'Ruby', 'Java'
    sizes = ['negative', 130, 245, 210]
    colors = ['gold', 'yellowgreen', 'lightcoral', 'lightskyblue']
    explode = (0.1, 0, 0, 0)  # explode 1st slice

    # Plot

    # printing first 5 positive tweets
    print("\n\n tweets:")

    for tweet2 in ptweets[:100]:
        print(query)
        print(tweet2['text'])
       # sql = "INSERT INTO testtable2 (Id, Name) VALUES (%s,%s )"
       # print(sql)
       # val = (tweet1['text'], query)

        #cursor.execute(sql, val)
        # cursor.execute("insert into testtable2(id) values('"+tweet+"')")

        # print(tweet['text'])

    # printing first 5 negative tweets

    # if query== query:

    # sql = "DELETE  FROM testtable WHERE Name = '"+query+"'"
    # print(sql)

    if query == query:

        for tweet in ntweets[:100]:
            print(query)
            print(negative)
            print(tweet['text'])






            # cursor.execute("insert into testtable2(id) values('"+tweet+"')")

            # print(tweet['text'])
            #
            # cursor.execute("insert into testtable2 (id) VALUES (%s)",(tweets['text']))
            # cursor.execute("insert into testtable2(id) values('"+tweet+"')")


if __name__ == "__main__":
    # calling main function
    main('0.0.0.0')