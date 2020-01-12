import json
import tweepy
import os
import pandas as pd
from collections import defaultdict
from datetime import datetime

current_wd = os.getcwd()
script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

tweets_data_exists = True
accounts_data_exists = True

if not os.path.exists('data/'):
    os.mkdir("data")

if not os.path.exists('data/tweets.csv'):
    tweets_data_exists = False

if not os.path.exists('data/accounts.csv'):
    accounts_data_exists = False

with open("config.json") as json_file:
    config = json.load(json_file)

auth = tweepy.OAuthHandler(config['twitter-oauth']['consumer-keys']['api-key'], config['twitter-oauth']['consumer-keys']['secret-key'])

api = tweepy.API(auth)

raw_tweets_data = defaultdict(list)
print("Starting Tweets Data Processing")
for follow_account in config['twitter-accounts-following']:
    count = 0
    print("Extracting tweet data for %s" % follow_account)
    for tweet in tweepy.Cursor(api.user_timeline, screen_name=follow_account, tweet_mode='extended').items():
        count+=1
        if 'retweeted_status' in dir(tweet):
            text=tweet.retweeted_status.full_text
            retweet_status = True
            rt = True
        else:
            text=tweet.full_text
            retweet_status = False
            rt = False
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
        raw_tweets_data['is_retweet'].append(rt)
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
    print("%s tweets extracted and stored in database" % count)

tweets_cols = [
    'id',
    'created_at',
    'text',
    'source',
    'in_reply_to_status_id',
    'in_reply_to_user_id',
    'author_id',
    'is_quote_status',
    'is_retweet',
    'lang',
    'geo',
    'coordinates'
]

tweet_metrics_cols = [
    'id',
    'pulled_at',
    'favorite_count',
    'retweet_count'
    # TODO: Get the response count somehow??
]
raw_tweets_df = pd.DataFrame(raw_tweets_data)
raw_tweets_df['pulled_at'] = datetime.utcnow()
raw_clipped_tweets = raw_tweets_df[tweets_cols]
raw_clipped_metrics = raw_tweets_df[tweet_metrics_cols]
if tweets_data_exists:
    historical_tweets_df = pd.read_csv('data/tweets.csv')
    tweet_metrics_df = pd.read_csv('data/tweet_metrics.csv')
    full_tweets = pd.concat([historical_tweets_df, raw_clipped_tweets]).drop_duplicates()
    tweet_metrics_df = pd.concat([tweet_metrics_df, raw_clipped_metrics])
else:
    full_tweets = raw_clipped_tweets
    tweet_metrics_df = raw_clipped_metrics

full_tweets.to_csv("data/tweets.csv", index=False)
tweet_metrics_df.to_csv("data/tweet_metrics.csv", index=False)

raw_accounts_data = defaultdict(list)
unique_account_ids = list(set(list(full_tweets[~full_tweets['in_reply_to_user_id'].isna()]['in_reply_to_user_id']) + list(full_tweets['author_id'])))
print("Starting twitter account processing: %s accounts" % len(unique_account_ids))
for user_id in unique_account_ids:
    try:
        user = api.get_user(int(user_id))
    except:
        print("user %s doesn't exist anymore!" % user_id)
        continue
    raw_accounts_data['user_id'].append(user_id)
    raw_accounts_data['name'].append(user.name)
    raw_accounts_data['screen_name'].append(user.screen_name)
    raw_accounts_data['location'].append(user.location)
    raw_accounts_data['description'].append(user.description)
    raw_accounts_data['url'].append(user.url)
    raw_accounts_data['protected'].append(user.protected)
    raw_accounts_data['followers_count'].append(user.followers_count)
    raw_accounts_data['friends_count'].append(user.friends_count)
    raw_accounts_data['listed_count'].append(user.listed_count)
    raw_accounts_data['created_at'].append(user.created_at)
    raw_accounts_data['favourites_count'].append(user.favourites_count)
    raw_accounts_data['geo_enabled'].append(user.geo_enabled)
    raw_accounts_data['verified'].append(user.verified)
    raw_accounts_data['statuses_count'].append(user.statuses_count)
    raw_accounts_data['lang'].append(user.lang)
    raw_accounts_data['profile_background_color'].append(user.profile_background_color)
    raw_accounts_data['profile_background_image_url'].append(user.profile_background_image_url_https)
    raw_accounts_data['profile_background_tile'].append(user.profile_background_tile)
    raw_accounts_data['profile_image_url'].append(user.profile_background_image_url_https)
    # raw_accounts_data['profile_banner_url'].append(user.profile_banner_url)
    raw_accounts_data['profile_link_color'].append(user.profile_link_color)
    raw_accounts_data['profile_sidebar_border_color'].append(user.profile_sidebar_border_color)
    raw_accounts_data['profile_sidebar_fill_color'].append(user.profile_sidebar_fill_color)
    raw_accounts_data['profile_text_color'].append(user.profile_text_color)
    raw_accounts_data['profile_use_background_image'].append(user.profile_use_background_image)

raw_accounts_df = pd.DataFrame(raw_accounts_data)
raw_accounts_df['pulled_at'] = datetime.utcnow()

accounts_cols = [
    'user_id',
    'name',
    'screen_name',
    'location',
    'description',
    'url',
    'protected',
    'created_at',
    'geo_enabled',
    'verified',
    'lang',
    'profile_background_color',
    'profile_background_image_url',
    'profile_background_tile',
    'profile_image_url',
    # 'profile_banner_url',
    'profile_link_color',
    'profile_sidebar_border_color',
    'profile_sidebar_fill_color',
    'profile_text_color',
    'profile_use_background_image'
]

account_metrics_cols = [
    'user_id',
    'pulled_at',
    'followers_count',
    'friends_count',
    'listed_count',
    'favourites_count',
    'statuses_count'
]

if accounts_data_exists:
    old_accounts_df = pd.read_csv('data/accounts.csv')
    keep_old_accounts = old_accounts_df[~old_accounts_df['user_id'].isin(raw_accounts_df['user_id'])]
    full_accounts_df = pd.concat([raw_accounts_df[accounts_cols], keep_old_accounts])
    old_metrics_df = pd.read_csv('data/account_metrics.csv')
    account_metrics_df = pd.concat([old_metrics_df, raw_accounts_df[account_metrics_cols]])
else:
    full_accounts_df = raw_accounts_df[accounts_cols]
    account_metrics_df = raw_accounts_df[account_metrics_cols]

print("Data processing complete, writing to data files!")
full_accounts_df.to_csv('data/accounts.csv', index=False)
account_metrics_df.to_csv('data/account_metrics.csv', index=False)