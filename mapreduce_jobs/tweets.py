from mrjob.job import MRJob
from mrjob.step import MRStep
import ujson
import sys
import os
import logging
from operator import itemgetter
import re
import us

sys.path.append(os.path.abspath('..'))


class Tweets(MRJob):
    MRJob.SORT_VALUES = True

    WORDS_LIST = {}

    def mapper_init(self):
        words_file = open(sys.path[-1] + os.path.sep + './assets/AFINN-en-165.txt')

        for line in words_file:
            word, score = line.split('\t')
            self.WORDS_LIST[word] = int(score)

    def _eval_word(self, word):
        if word in self.WORDS_LIST:
            return self.WORDS_LIST[word]
        else:
            return 0

    @staticmethod
    def _is_hashtag(word):
        return word.startswith('#')

    @staticmethod
    def _filter_tweets(line):
        try:
            tweet_object = ujson.loads(line.strip())
        except ValueError:
            logging.warning('JSON malformed')
            return

        if tweet_object['lang'] == 'en':
            if 'usa_state' not in tweet_object:
                if 'place' in tweet_object and tweet_object['place']['country_code'] == 'US':
                    full_name = tweet_object['place']['full_name']

                    matches = re.findall('([\w+\s]+)', full_name)

                    if len(matches) is 2:
                        if matches[1] == 'USA':
                            tweet_object['usa_state'] = matches[0]
                        else:
                            # Find state full name for abbreviation:
                            tweet_object['usa_state'] = str(us.states.lookup(matches[1][1:]))
                else:
                    tweet_object = None

            return tweet_object

    @staticmethod
    def _clean_word(word):
        results = re.findall('^#?[a-zA-Z_]*', word)

        if len(results) > 0:
            word = results[0].lower()

        return word

    def mapper(self, _, line):
        tweet_object = Tweets._filter_tweets(line)

        if tweet_object is not None:
            text = tweet_object['text']
            usa_state = tweet_object['usa_state']

            for word in text.split():
                cleaned_word = Tweets._clean_word(word)

                yield(usa_state, self._eval_word(cleaned_word))

                if Tweets._is_hashtag(cleaned_word):
                    yield(cleaned_word, 1)

    def combiner(self, key, value):
        yield(key, sum(value))

    def reducer(self, key, value):
        value_key_tuple = (sum(value), key)

        if not Tweets._is_hashtag(key):
            print(value_key_tuple)

        yield(None, value_key_tuple)

    def happiest_state(self, _, value_key_tuples):
        tuples_list = list(value_key_tuples)

        states = []

        for tuple in tuples_list:
            if not Tweets._is_hashtag(tuple[1]):
                states.append(tuple)

        states.sort(key=itemgetter(0), reverse=True)

        print(states[0])

        for value_key_tuple in tuples_list:
            yield(None, value_key_tuple)

    def top_10_hashtags(self, _, value_key_tuples):
        tuples_list = list(value_key_tuples)

        hashtags = []

        for tuple in tuples_list:
            if Tweets._is_hashtag(tuple[1]):
                hashtags.append(tuple)

        hashtags.sort(key=itemgetter(0), reverse=True)

        for hashtag in hashtags[:10]:
            print(hashtag)

    def steps(self):
        return [MRStep(mapper_init=self.mapper_init,
                       mapper=self.mapper,
                       combiner=self.combiner,
                       reducer=self.reducer
                       ),
                MRStep(reducer=self.happiest_state),
                MRStep(reducer=self.top_10_hashtags)
                ]

if __name__ == '__main__':
    Tweets.run()
