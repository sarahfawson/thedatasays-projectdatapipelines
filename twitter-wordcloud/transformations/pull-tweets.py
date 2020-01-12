import pandas as pd 

accounts_df = pd.read_csv("../../tools/twitter-helper/data/accounts.csv")
tweets_df = pd.read_csv("../../tools/twitter-helper/data/tweets.csv")
# accountmetrics_df = pd.read_csv("../../tools/twitter-helper/data/account_metrics.csv")
# tweetmetrics_df = pd.read_csv("../../tools/twitter-helper/data/tweet_metrics.csv")

master_df = pd.merge(accounts_df, tweets_df, left_on="user_id", right_on="author_id")

# filter for trump's tweets and save for analysis
trump_tweets = master_df[master_df['screen_name'] == 'realDonaldTrump']
trump_tweets.to_csv("../data/wordcloud-trump-tweets.csv", index=False)
