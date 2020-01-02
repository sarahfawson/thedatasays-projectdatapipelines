import json
import tweepy
import os
import pandas as pd
from collections import defaultdict

#check if the data folder exists (if not then create it)
#check if the etc folder exists (if not then create it)
    # Also create the state json object too

current_wd = os.getcwd()
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

if not os.path.exists('data/'):
    os.mkdir("data")

# if not os.path.exists('etc/'):
#     os.mkdir("etc")
#     state_file_obj = {
#         ''
#     }


with open("config.json") as json_file:
    config = json.load(json_file)

auth = tweepy.OAuthHandler(config['twitter-oauth']['consumer-keys']['api-key'], config['twitter-oauth']['consumer-keys']['secret-key'])

api = tweepy.API(auth)


#Filling tweet dataframe with tweet metrics. Then use pandas processing to clean and put into usable datasets
raw_tweets_data = defaultdict(list)
count = 0
for follow_account in config['twitter-accounts-following']:
    for tweet in tweepy.Cursor(api.user_timeline, screen_name=follow_account, tweet_mode='extended').items(50):
        print(count)
        count+=1
        if 'retweeted_status' in dir(tweet):
            text=tweet.retweeted_status.full_text
            retweet_status = True
        else:
            text=tweet.full_text
            retweet_status = False
        raw_tweets_data['created_at'].append(tweet.created_at)
        raw_tweets_data['id'].append(tweet.id)
        raw_tweets_data['text'].append(text)
        raw_tweets_data['source'].append(tweet.source)
        raw_tweets_data['source_url'].append(tweet.source_url)
        raw_tweets_data['in_reply_to_status_id'].append(tweet.in_reply_to_status_id)
        raw_tweets_data['in_reply_to_user_id'].append(tweet.in_reply_to_user_id)
        raw_tweets_data['in_reply_to_screen_name'].append(tweet.in_reply_to_screen_name)
        raw_tweets_data['author_id'].append(tweet.author.id)
        raw_tweets_data['user_id'].append(tweet.user.id)
        # raw_tweets_data['contributors'].append(tweet.contributors)
        # raw_tweets_data['retweeted_status'].append(tweet.retweeted_status)
        raw_tweets_data['is_quote_status'].append(tweet.is_quote_status)
        raw_tweets_data['retweet_count'].append(tweet.retweet_count)
        raw_tweets_data['favorite_count'].append(tweet.favorite_count)
        raw_tweets_data['favorited'].append(tweet.favorited)
        raw_tweets_data['retweeted'].append(tweet.retweeted)
        #raw_tweets_data['possibly_sensitive'].append(tweet.possibly_sensitive)
        raw_tweets_data['lang'].append(tweet.lang)
        if tweet.geo != None:
            raw_tweets_data['geo'].append(json.dumps(tweet.geo))
            raw_tweets_data['coordinates'].append(json.dumps(tweet.coordinates))
            # raw_tweets_data['place'].append(json.dumps(tweet.place.__dict__))
        else:
            raw_tweets_data['geo'].append(None)
            raw_tweets_data['coordinates'].append(None)
            # raw_tweets_data['place'].append(None)

tweets_df = pd.DataFrame(raw_tweets_data)

#Now go get the user information from the unique users being referenced here
#Then merge that with the users dataset
#Then get the metrics for each of the users and split that into a new dataset that will be appended to the metrics dataset
tweets_df.to_csv("data/test.csv", index=False)
print(tweet.__dict__.keys())