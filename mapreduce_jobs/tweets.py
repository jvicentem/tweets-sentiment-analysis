from mrjob.job import MRJob
import ujson
import sys
import os

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
        tweet_object = ujson.loads(line)

        text = tweet_object['text']
        usa_state = tweet_object['usa_state']

        for word in text.split():
            yield(usa_state, self._eval_word(word))

    def combiner(self, usa_state, word_score):
        yield(usa_state, sum(word_score))

    def reducer(self, usa_state, state_score):
        yield(usa_state, sum(state_score))

if __name__ == '__main__':
    print(Tweets.run())
