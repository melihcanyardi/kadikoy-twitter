# Library imports
import json
import gzip
import tweepy
import datetime
import numpy as np
import os
import sys

batch_no = sys.argv[1]
with open("bearer_tokens.txt", "r") as f:
    bearer_tokens = [x.strip() for x in f.readlines()]
bearer_token = bearer_tokens[int(sys.argv[1])%8-1]

## tweepy.Client
client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)


# Read User IDs
with open(f"kadikoy_user_ids/kadikoy_user_ids-{batch_no}.txt", 'r') as f:
    user_ids = f.read().split("\n")

def add_metadata(user_id):
    """Get users_following, users_followers, tweets (original, reply,
    quote, retweet), follower_count following_count of a user from user_id."""

    # Get user data
    user_data = client.get_user(id=user_id, user_fields=["id", "name", "username", "created_at", "description", "location"]).data.data

    user_object = {}
    try:
        user_object["location"] = user_data["location"]
    except:
        user_object["location"] = ""
        
    try:
        user_object["description"] = user_data["description"]
    except:
        user_object["description"] = ""
        
    try:
        user_object["name"] = user_data["name"]
    except:
        user_object["name"] = ""

    try:
        user_object["screen_name"] = user_data["username"]
    except:
        user_object["screen_name"] = ""

    try:
        user_object["id_str"] = user_data["id"]
    except:
        user_object["id_str"] = ""
        
    try:
        user_object["created_at"] = user_data["created_at"]
    except:
        user_object["created_at"] = ""
    
    # users_following
    users_following = client.get_users_following(id=user_id)
    if users_following.data == None:
        following = []
    else:
        following = [str(user.id) for user in users_following.data]
        
    # users_followers
    users_followers = client.get_users_followers(id=user_id)
    if users_followers.data == None:
        followers = []
    else:
        followers = [str(user.id) for user in users_followers.data]
        
    # users_tweets
    users_tweets = []
    tweet_lists = []

    for tweet_list in tweepy.Paginator(client.get_users_tweets, user_id,
                                tweet_fields=["referenced_tweets", "created_at"],
                                expansions=["referenced_tweets.id", "referenced_tweets.id.author_id"],
                                max_results=100,
                                limit=32):
        tweet_lists.append(tweet_list)

    for tweet_list in tweet_lists:
        tweets = [tweet.data for tweet in tweet_list.data]
        referenced_tweets = [tweet.data for tweet in tweet_lists[0].includes["tweets"]]
        referenced_tweets_id_to_tweet = {str(tweet["id"]): tweet for tweet in referenced_tweets}
        
        for tweet in tweets:
            tweet.pop("author_id")

            tweet["twt_id_str"] = tweet.pop("id")
            tweet["twt_txt"] = tweet.pop("text")
            tweet["twt_date"] = tweet["created_at"][2:4] + tweet["created_at"][5:7] + tweet["created_at"][8:10]
            if "referenced_tweets" in tweet.keys():
                if tweet["referenced_tweets"][0]["type"] == "replied_to":
                    tweet["type"] = "reply"
                elif tweet["referenced_tweets"][0]["type"] == "quoted":
                    tweet["type"] = "quote"
                elif tweet["referenced_tweets"][0]["type"] == "retweeted":
                    tweet["type"] = "retweet"
                    tweet["ref_twt_id_str"] = tweet.pop("twt_id_str")
                    tweet["ref_twt_txt"] = tweet.pop("twt_txt")

                referenced_tweet_id = tweet["referenced_tweets"][0]["id"]
                tweet["ref_twt_id_str"] = str(referenced_tweet_id)

                if tweet["type"] == "reply" or tweet["type"] == "quote":
                    try:
                        tweet["ref_twt_txt"] = referenced_tweets_id_to_tweet[referenced_tweet_id]["text"]
                    except KeyError:
                        tweet["ref_twt_txt"] = ""

                    try:
                        tweet["ref_usr_id_str"] = referenced_tweets_id_to_tweet[referenced_tweet_id]["author_id"]
                    except:
                        tweet["ref_usr_id_str"] = ""

                    try:
                        referenced_tweet_date = referenced_tweets_id_to_tweet[referenced_tweet_id]["created_at"]
                        tweet["ref_twt_date"] = referenced_tweet_date[2:4] + referenced_tweet_date[5:7] + referenced_tweet_date[8:10]
                    except:
                        tweet["ref_twt_date"] = ""

                if tweet["type"] == "retweet":
                    tweet["ref_twt_date"] = tweet.pop("twt_date")
                    try:
                        tweet["ref_usr_id_str"] = referenced_tweets_id_to_tweet[referenced_tweet_id]["author_id"]
                    except:
                        tweet["ref_usr_id_str"] = ""

                tweet.pop("referenced_tweets")

            else:
                tweet["type"] = "original"

            tweet.pop("created_at")
            
            if "edit_history_tweet_ids" in tweet.keys():
                tweet.pop("edit_history_tweet_ids")
        
        users_tweets.extend(tweets)
                
    # likes
    liked_tweets = client.get_liked_tweets(id=user_id, expansions=["author_id"], tweet_fields=["created_at"])
    if liked_tweets.data != None:
        users_tweets.extend([{"ref_twt_date": str(tweet.created_at)[2:4] + str(tweet.created_at)[5:7] + str(tweet.created_at)[8:10],
                              "ref_twt_id_str": str(tweet.id),
                              "ref_twt_txt": tweet.text,
                              "ref_usr_id_str": str(tweet.author_id),
                              "type": "fav"} for tweet in liked_tweets.data])

    
    # append new key-value pairs
    user_object["following"] = following
    user_object["followers"] = followers
    user_object["tweets"] = users_tweets
                
    # get "profile_image_url", "followers_count" and "following_count" from user_info
    user_info = client.get_user(id=user_id, user_fields=["profile_image_url", "public_metrics"])
    
    if user_info.data:
        # add "followers_count" and "following_count"
        user_object["followers_count"] = user_info.data.public_metrics["followers_count"]
        user_object["following_count"] = user_info.data.public_metrics["following_count"]
        # add profile image URL
        pp_url = user_info.data.profile_image_url
        if pp_url.split("/")[2] == 'pbs.twimg.com':
            return_pp_url = '/'.join(pp_url.split("/")[-2:])
        elif pp_url.split("/")[2] == 'abs.twimg.com':
            return_pp_url = ""
        user_object["pp"] = return_pp_url
    else:
        user_object["followers_count"] = ""
        user_object["following_count"] = ""
        return_pp_url = ""
        user_object["pp"] = return_pp_url

        
    # add downloaded date
    user_object["downloaded"] = datetime.datetime.now().strftime("%y%m%d")
    
    return user_object

# Create "daily_downloads" folder if not exists
if not os.path.exists("daily_downloads_kadikoy"):
    os.mkdir("daily_downloads_kadikoy")
    
# Create empty txt file if not exists
if f"users_downloaded-batch_{batch_no}.txt" not in os.listdir("daily_downloads_kadikoy"):
    with open(f'daily_downloads_kadikoy/users_downloaded-batch_{batch_no}.txt', 'w') as f:
        pass

# Download user objects
print(f"Batch {batch_no} Progress:")
while True:
    user_id = user_ids[np.random.randint(low=0, high=len(user_ids))]
    try:
        # Add metadata to user objects & write to daily gzip files
        # Check if user has already been downloaded
        with open(f"daily_downloads_kadikoy/users_downloaded-batch_{batch_no}.txt", "r") as f:
            users_downloaded = f.read().split("\n")
        if user_id not in users_downloaded:
            # Write metadata-added user object to gzip file
            with open(f"daily_downloads_kadikoy/kadikoy_users-{datetime.datetime.now().strftime('%y%m%d')}_{batch_no}.txt.gz", 'ab') as f:
                f.write(gzip.compress(f"{json.dumps(add_metadata(user_id))}\n".encode('utf-8')))
            # Write User ID to txt file
            with open(f"daily_downloads_kadikoy/users_downloaded-batch_{batch_no}.txt", "a") as f:
                f.write(f'{user_id}\n')
        print(f"{len(users_downloaded)}/{len(user_ids)} | {len(users_downloaded)/len(user_ids)*100:.2f}%")
    except:
        continue