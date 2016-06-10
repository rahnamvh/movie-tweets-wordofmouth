from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import sentimentfinder
import sys

stringtokens = []
tokens = 0
movie = ' '
ckey = "7BVX7HVJQG17042phVKlH2BlS"
csecret = "gSCgF2lLmoWzdq19q1Zvqyt5LP7WPI3gILftzceZ8rjE9rCbrN"
atoken = "2745731274-kr4gRzMYsLWt0ds0NiiqsB9fuaWKMScGRlVmJ52"
asecret = "hYm9W0Aumz5yIk9eKFcG2T4ko8bGq0jLCaKobjh9Y7EU3"


class Listener(StreamListener):

    def on_data(self, data):
        tweet = json.loads(data)
        tweet_mentions = []
        tweet_hashtags = []

        #print(all_data)

        tweet_id = str(tweet.get('id'))
        tweet_created_at = str(tweet.get('created_at'))
        tweet_text = tweet.get('text')
        tweet_user = tweet.get('user')
        tweet_user_screen_name = tweet_user.get('screen_name')
        tweet_retweeted_status = tweet.get('retweeted_status')

        if tweet_retweeted_status is None:
            tweet_retweeted_status_id = 'None'
        else:
            tweet_retweeted_status_id = str(tweet_retweeted_status.get('id'))

        tweet_entities = tweet.get('entities')
        tweet_user_mentions = tweet_entities.get('user_mentions')
        tweet_user_hashtags = tweet_entities.get('hashtags')

        for tweet_user_mention in tweet_user_mentions:
            screen_name = tweet_user_mention.get('screen_name')
            tweet_mentions.append(screen_name)

        tweet_mentions_list = ','.join(tweet_mentions)

        for tweet_user_hashtag in tweet_user_hashtags:
            hashtags = tweet_user_hashtag.get('text')
            tweet_hashtags.append(hashtags)

        tweet_hashtags_list = ','.join(tweet_hashtags)

        sentimentfinderobj = sentimentfinder.SentimentFinder(tweet_text)
        sentiment = sentimentfinderobj.getsentiment()
        keywords = sentimentfinderobj.getkeywordlist()
        keywordslist = ','.join(keywords)
        tweet_text = tweet_text.replace('|',' ')
        tweet_text = tweet_text.replace('\n', ' ')

        tweetoutput = movie + '|' + tweet_id + '|' + tweet_created_at + '|' + tweet_text.encode('ascii','replace') + '|' + tweet_user_screen_name + '|' + tweet_retweeted_status_id + '|' + tweet_mentions_list + '|' + tweet_hashtags_list + '|' + sentiment + '|' + keywordslist

        print(tweetoutput)

        return True

    def on_error(self, status):
        print(status)

auth = OAuthHandler(ckey,csecret)
auth.set_access_token(atoken,asecret)

sentimentfinder.CreateKeywordList()

with open('trackwords.txt', 'r') as f:
    for line in f:
        #print(line)
        tokens += 1
        if tokens == 1:
            movie = line.strip('\n')
        else:
            stringtokens = line.split(",")

twitterstream = Stream(auth, Listener())
twitterstream.filter(track=stringtokens, languages=["en"])
