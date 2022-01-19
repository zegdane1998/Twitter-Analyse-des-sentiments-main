#from tweepy.streaming import StreamListener
from textblob import TextBlob
from elasticsearch import Elasticsearch

class TweetStreamListener():
    def __init__(self, index, doc_type):
        #super(TweetStreamListener, self).__init__(consumer_key,consumer_secret,access_token,access_token_secret)

        self.index = index
        self.doc_type = doc_type

    def on_data(self, data):
        #En cas de succès. Pour récupérer, traiter et organiser les tweets pour obtenir des données structurées
        #et injecter des données dans Elasticsearch

        print("=> Retrievd a tweet")

        # Decode json
        #dict_data = json.loads(data)
        dict_data = (data)


        # Process data
        polarity, subjectivity, sentiment, author = self._get_sentiment(dict_data)
        print("[sentiment]", sentiment)
        print("[polarity]", polarity)



        # Inject data into Elasticsearch
        doc = {
               "polarity": polarity,
               "subjectivity": subjectivity,
               "sentiment": sentiment,
               "author": author,
             }
        
        es = Elasticsearch("http://elastic:changeme@localhost:9200")
        es.index(index=self.index,
                 doc_type=self.doc_type,
                 body=doc)

        return True

    def on_error(self, status):
        """On failure"""
        print(status)
    
    def _get_sentiment(self, decoded):
        # Pass textual data to TextBlob to process
        tweet = TextBlob(decoded)
        author=tweet.split(' ')[0]

        # [0, 1] where 1 means very subjective
        subjectivity = tweet.sentiment.subjectivity
        # [-1, 1]
        polarity = tweet.sentiment.polarity
        
        # Determine if sentiment is positive, negative, or neutral
        if polarity < 0:
            sentiment = "negative"
        elif polarity == 0:
            sentiment = "neutral"
        else:
            sentiment = "positive"

        return polarity, subjectivity, sentiment, author
    
