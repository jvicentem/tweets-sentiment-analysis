from mrjob.job import MRJob
from mrjob.step import MRStep
import ujson
import sys
import os
import logging

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

            #    if word.startswith('#'):
            #        yield(word, 1)
        except ValueError:
            logging.warning('JSON malformed')

    def combiner(self, usa_state, word_score):
        yield(usa_state, sum(word_score))

    def reducer(self, usa_state, state_score):
        state_score_tuple = (sum(state_score), usa_state)
        print(state_score_tuple)
        yield(None, state_score_tuple)

    def happiest_state(self, _, state_score_tuple):
        yield max(state_score_tuple)

    def steps(self):
        return [MRStep(mapper_init=self.mapper_init,
                       mapper=self.mapper,
                       combiner=self.combiner,
                       reducer=self.reducer
                       ),
                MRStep(reducer=self.happiest_state)
                ]

if __name__ == '__main__':
    print(Tweets.run())
