# -*- coding: utf-8 -*-

import oauth2 as oauth
import urllib.request as urllib
import ujson
import re
import reverse_geocoder as rg
import us
from pymongo import MongoClient

access_token_key = ''
access_token_secret = ''

consumer_key = ''
consumer_secret = ''

_debug = 0

oauth_token = oauth.Token(key=access_token_key, secret=access_token_secret)
oauth_consumer = oauth.Consumer(key=consumer_key, secret=consumer_secret)

signature_method_hmac_sha1 = oauth.SignatureMethod_HMAC_SHA1()

http_method = "GET"


http_handler = urllib.HTTPHandler(debuglevel=_debug)
https_handler = urllib.HTTPSHandler(debuglevel=_debug)


'''
Construct, sign, and open a twitter request
using the hard-coded credentials above.
'''
def twitterreq(url, method, parameters):
    req = oauth.Request.from_consumer_and_token(oauth_consumer,
                                             token=oauth_token,
                                             http_method=http_method,
                                             http_url=url,
                                             parameters=parameters)

    req.sign_request(signature_method_hmac_sha1, oauth_consumer, oauth_token)

    headers = req.to_header()

    if http_method == "POST":
        encoded_post_data = req.to_postdata()
    else:
        encoded_post_data = None
        url = req.to_url()

    opener = urllib.OpenerDirector()
    opener.add_handler(http_handler)
    opener.add_handler(https_handler)

    response = opener.open(url, encoded_post_data)

    return response


'''
Remove unused fields
'''
def remove_fields(tweet_dict):
    keys_to_delete = [
                      'contributors',
                      'filter_level',
                      'quoted_status',
                      'source',
                      'retweeted',
                      'in_reply_to_screen_name',
                      'id_str',
                      'favorited',
                      'in_reply_to_status_id',
                      'quoted_status',
                      'entities',
                      'in_reply_to_user_id',
                      'in_reply_to_user_id_str',
                      'display_text_range',
                      'is_quote_status',
                      'truncated',
                      'in_reply_to_status_id_str',
                      'quoted_status_id_str'
                      ]

    for key in keys_to_delete:
        if key in tweet_dict:
            del tweet_dict[key]

    if 'user' in tweet_dict:
        user_keys = list(tweet_dict['user'].keys())
        user_keys.remove('name')
        for key in user_keys:
            del tweet_dict['user'][key]

    return tweet_dict

'''
Prints tweet and inserts it into the db
'''
def print_tweet(tweet_json, db):
    user_name = tweet_json['user']['name']
    tweet_id = str(tweet_json['id'])

    tweet_json['tweet_link'] = 'https://twitter.com/' + user_name + '/status/' + tweet_id

    # now let's print the tweet
    print(ujson.dumps(tweet_json))

    # now let's save the tweet
    db.tweets_collection.insert_one(tweet_json)

'''
This is the method which actually retrieves tweets and filters them only getting those that were posted in English and from USA.
'''
def fetchsamples(db):
    query_coords_string = '-155.6811,18.91,-66.9470,44.81'

    url = 'https://stream.twitter.com/1.1/statuses/filter.json?lang=en&locations=' + query_coords_string

    response = twitterreq(url, "POST", [])

    for line in response:
        tweet_json = ujson.loads(line.strip().decode('utf8'))

        # We only process geolocated tweets and tweets in English
        if tweet_json['lang'] == 'en':
            if 'geo' in tweet_json and tweet_json['geo'] is not None:
                latitude = tweet_json['geo']['coordinates'][0]
                longitude = tweet_json['geo']['coordinates'][1]

                result_json = rg.search((latitude, longitude), verbose=False)[0]

                if result_json is not None:
                    if 'cc' in result_json and result_json['cc'] == 'US':
                        tweet_json['usa_state'] = result_json['admin1']
                        print_tweet(remove_fields(tweet_json), db)
            elif 'place' in tweet_json and tweet_json['place'] == 'United States':
                full_name = tweet_json['place']['full_name']

                matches = re.findall('([\w+\s]+)', full_name)

                if len(matches) is 2:
                    if matches[1] == 'USA':
                        tweet_json['usa_state'] = matches[0]
                    else:
                        tweet_json['usa_state'] = str(us.states.lookup(matches[1][1:]))  # Find state full name for abbr

                    print_tweet(remove_fields(tweet_json), db)


if __name__ == '__main__':
    client = MongoClient('localhost', 27017)

    db = client.tweets

    # DANGEROUS! IT DELETES ALL TWEETS EVERY TIME THE SCRIPT IS EXECUTED
    db.tweets_collection.delete_many({})

    while True:
        try:
            fetchsamples(db)
        except:
            continue
