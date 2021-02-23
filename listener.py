#!/usr/bin/env python
# coding: utf-8

import tweepy
from textblob import TextBlob
import json
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import pandas as pd
import numpy as np
import time

import os
import psycopg2




import nltk
def twitter_listener():
    try:
        print('vader_lexicon not installed: installing now')
        nltk.download('vader_lexicon')
    except:
        print('vader_lexicon installed')




    consumer_key='mx2DBLn5UBlSAE72VqflYwchn'
    consumer_secret='HZ99ImAXFVGJ6GaaOaH8EbYPJ7rEyqUKGx9obP9OFwI5dWAN9Q'
    access_token='1188186968783556614-TYGRVuIEp5Vof7eLoucBSKqN6iljQe'
    access_token_secret='Z5oeS0z7jjXKzf09p5bSOSKgNTdD0eMxxONvQXHjvlg9e'

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)


    twitter_dicc = []
    sid = SentimentIntensityAnalyzer()
    class StreamListener(tweepy.StreamListener):
        
        def __init__(self):
            super().__init__()
            self.counter = 0
            self.limit = 1000
        
        def on_status(self, status):
            if status.retweeted:
                return
            polarity = sid.polarity_scores(status.text)
            twitter_dicc.append([status,polarity])
        
            self.counter += 1
            if (self.counter % 100) == 0:
                positive = [x['pos'] for y,x in twitter_dicc]
                negative = [x['neg'] for y,x in twitter_dicc]
                neutral = [x['neu'] for y,x in twitter_dicc]
                fecha = [y.created_at for y,x in twitter_dicc]
                id_user = [y.user.screen_name for y,x in twitter_dicc]
                texto = [y.text for y,x in twitter_dicc]
            
                data_sent = pd.DataFrame(list(zip(fecha,positive,negative,neutral,id_user,texto)),columns = ['Date','Pos','Neg','Neu','Us','Text'])
            
                data_sent.Neg = data_sent.Neg.apply(lambda x: x*-1)
            
                #data_sent.to_csv('Data_sent.csv')
                if self.counter < self.limit:
                    return True
                else:
                    stream.disconnect()        
            
        def on_error(self, status_code):
            if status_code == 420:
                #returning False in on_data disconnects the stream
                return True
          


    stream_listener = StreamListener()
    stream = tweepy.Stream(auth=api.auth, listener=stream_listener)


    stream.filter(track = ['bitcoin','BTC','BITCOIN'],languages=['en'])



    positive = [x['pos'] for y,x in twitter_dicc]
    negative = [x['neg'] for y,x in twitter_dicc]
    neutral = [x['neu'] for y,x in twitter_dicc]
    #fecha = [y.created_at.strftime('%d/%m/%y %H:%M:%S') for y,x in twitter_dicc]
    fecha = [y.created_at for y,x in twitter_dicc]
    id_user = [y.user.screen_name for y,x in twitter_dicc]
    texto = [y.text for y,x in twitter_dicc]


    data_sent = pd.DataFrame(list(zip(fecha,positive,negative,neutral,id_user,texto)),columns = ['Date','Pos','Neg','Neu','Us','Text'])


    data_sent.Neg = data_sent.Neg.apply(lambda x: x*-1)


    database_url = os.getenv(
        'DATABASE_URL',
        default='postgres://ouhieniktislqg:670cd50a97c032fd1f58880c7b4ad96893c94e7b40706e850a03630a5a7ba8ca@ec2-3-221-49-44.compute-1.amazonaws.com:5432/d7l11l3desflh0',  # E.g., for local dev
    )

    connection = psycopg2.connect(database_url)

    cursor = connection.cursor()


    # delete_table = "DROP TABLE twitter ;"

    # cursor.execute(delete_table)



    # Get the updated list of tables

    sqlGetTableList = "SELECT * FROM information_schema.tables where table_schema='public' ORDER BY table_schema,table_name ;"

    cursor.execute(sqlGetTableList)

    tables = [x[2] for x in cursor.fetchall()]

    if 'twitter' not in tables:
        # Create a table in PostgreSQL database

        name_Table = "twitter"


        sqlCreateTable = "create table "+name_Table+" (Date timestamp, Pos numeric, Neg numeric, Neu numeric, Us varchar, Text varchar);"

     

        cursor.execute(sqlCreateTable)
        connection.commit()


    def single_insert(conn, insert_req):
        """ Execute a single INSERT request """
        cursor = conn.cursor()
        try:
            cursor.execute(insert_req)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        cursor.close()
    # Connecting to the database

    select = "SELECT * from twitter ;"

    dataframe = pd.read_sql(select, con = connection)
    
    dataframe['Date'] = pd.to_datetime(dataframe['Date'])
    dataframe['Text'] = dataframe['Text'].str.replace("'", "''")


    # In[11]:


    # Inserting each row
    for i in dataframe.index:
        row = dataframe.loc[i]
        query = """
        INSERT into twitter (Date, Pos, Neg, Neu, Us, Text) values('%s','%s','%s','%s','%s','%s');
        """ % (row['Date'], row['Pos'],
               row['Neg'], row['Neu'], 
               row['Us'], row['Text'])
        single_insert(connection, query)
    # Close the connection
    connection.close()


