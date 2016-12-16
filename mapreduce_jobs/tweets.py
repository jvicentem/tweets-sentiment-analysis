from mrjob.job import MRJob
from mrjob.step import MRStep
import ujson
import logging
from operator import itemgetter
import word_utils
import ssl

#The following two lines fix S3 bucket name problems
if hasattr(ssl, '_create_unverified_context'):
    ssl._create_default_https_context = ssl._create_unverified_context


class Tweets(MRJob):
    MRJob.SORT_VALUES = True

    WORDS_DICT = {}

    def mapper_init(self):
        self.WORDS_DICT = word_utils.words_score_dict()

    '''
    Returns a value for a word.
    If the word is not included in the dictionary, then the value is 0.
    '''
    def _eval_word(self, word):
        if word in self.WORDS_DICT:
            return self.WORDS_DICT[word]
        else:
            return 0

    '''
    Returns true if field is a key of dictionary and the value of that key is not None.
    '''
    @staticmethod
    def _field_in_dict(dictionary, field):
        return dictionary is not None and field in dictionary and dictionary[field] is not None

    '''
    Given a string, returns a filtered object that represents a Tweet.

    It will return None unless the tweet is written in English and its location
    belongs to the United States (there must be info about the state in tweet['place']['full_name']
    or tweet['user']['location']).
    '''
    @staticmethod
    def _filter_tweets(line):
        try:
            tweet_object = ujson.loads(line.strip())
        except ValueError:
            logging.warning('JSON malformed')
            return None

        if Tweets._field_in_dict(tweet_object, 'lang') and tweet_object['lang'] == 'en':
            if 'usa_state' in tweet_object:
                return tweet_object
            else:
                place_exists = Tweets._field_in_dict(tweet_object, 'place')
                country_code_exists = Tweets._field_in_dict(tweet_object['place'], 'country_code')

                if place_exists and country_code_exists and tweet_object['place']['country_code'] == 'US':
                    place_full_name = tweet_object['place']['full_name']

                elif (Tweets._field_in_dict(tweet_object, 'user') and
                        Tweets._field_in_dict(tweet_object['user'], 'location')):

                    place_full_name = tweet_object['user']['location']
                else:
                    return None

                possible_state = word_utils.find_usa_state(place_full_name)

                if possible_state != 'None':
                    tweet_object['usa_state'] = possible_state
                    return tweet_object
                else:
                    return None
        else:
            return None

    def mapper(self, _, line):
        tweet_object = Tweets._filter_tweets(line)

        if tweet_object is not None:
            text = tweet_object['text']
            usa_state = tweet_object['usa_state']

            for word in text.split():
                cleaned_word = word_utils.clean_word(word)

                yield(usa_state, self._eval_word(cleaned_word))

                if word_utils.is_hashtag(cleaned_word):
                    yield(cleaned_word, 1)

    def combiner(self, key, value):
        yield(key, sum(value))

    def reducer(self, key, value):
        value_key_tuple = (sum(value), key)

        if word_utils.is_hashtag(key):
            yield('hashtag', value_key_tuple)
        else:
            yield('state', value_key_tuple)

    '''
    This method will act as a second reducer and it returns the top 10 hashtags
    and the happiest state in USA.
    '''
    def happiest_state_and_top_10_hashtags(self, state_or_hashtag_string_key, value_key_tuples):
        tuples_list = list(value_key_tuples)

        if state_or_hashtag_string_key == 'hashtag':
            hashtags = []

            for tup in tuples_list:
                if tup is not None and word_utils.is_hashtag(tup[1]):
                    hashtags.append(tup)

            hashtags.sort(key=itemgetter(0), reverse=True)

            for hashtag in hashtags[:10]:
                yield(hashtag[1], hashtag[0])
        elif state_or_hashtag_string_key == 'state':
            states = []

            for tup in tuples_list:
                if tup is not None and not word_utils.is_hashtag(tup[1]):
                    states.append(tup)
                    yield(tup[1], tup[0])

            states.sort(key=itemgetter(0), reverse=True)

            yield(states[0][1], states[0][0])

    def steps(self):
        return [MRStep(mapper_init=self.mapper_init,
                       mapper=self.mapper,
                       combiner=self.combiner,
                       reducer=self.reducer
                       ),
                MRStep(reducer=self.happiest_state_and_top_10_hashtags)
                ]

if __name__ == '__main__':
    Tweets.run()
