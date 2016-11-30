from mrjob.job import MRJob
from mrjob.step import MRStep
import ujson
import sys
import os
import logging
from operator import itemgetter

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

    def mapper(self, _, line):
        try:
            tweet_object = ujson.loads(line.strip())

            text = tweet_object['text']
            usa_state = tweet_object['usa_state']

            for word in text.split():
                yield(usa_state, self._eval_word(word))

            if word.startswith('#'):
                yield(word, 1)
        except ValueError:
            logging.warning('JSON malformed')

    def combiner(self, key, value):
        yield(key, sum(value))

    def reducer(self, key, value):
        value_key_tuple = (sum(value), key)

        if not key.startswith('#'):
            print(value_key_tuple)

        yield(None, value_key_tuple)

    def happiest_state(self, _, value_key_tuples):
        tuples_list = list(value_key_tuples)

        original_tuples_list_copy = list(tuples_list)

        for tuple in tuples_list:
            if tuple[1].startswith('#'):
                tuples_list.remove(tuple)

        tuples_list.sort(key=itemgetter(0), reverse=True)

        print(tuples_list[0])

        for value_key_tuple in original_tuples_list_copy:
            yield(None, value_key_tuple)

    def top_10_hashtags(self, _, value_key_tuples):
        tuples_list = list(value_key_tuples)

        hashtags = []

        for tuple in tuples_list:
            if tuple[1].startswith('#'):
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
