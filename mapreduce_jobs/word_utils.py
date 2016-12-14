import re
import us

'''
Returns a dictionary with words as keys and sentiment rates as values.
'''
def words_score_dict():
    words_file = open('./AFINN-en-165.txt')

    words_dict = {}

    for line in words_file:
        word, score = line.split('\t')
        words_dict[word] = int(score)

    words_file.close()

    return words_dict

'''
Returns true if word is a hashtag. Otherwise, returns false.
'''
def is_hashtag(word):
    return word.startswith('#')

'''
Returns a clean word (only letters).
'''
def clean_word(word):
    results = re.findall('^#?[a-zA-Z_]*', word)

    if len(results) > 0:
        word = results[0].lower()

    return word

'''
Returns the USA state given a text that follows the following templates:
- Texas, USA
- Austin, TX
'''
def find_usa_state(text):
    matches = re.findall('([\w+\s]+)', text)

    if len(matches) is 2:
        # e.g.: Texas, USA
        if matches[1] == 'USA':
            return matches[0]
        # e.g.: Austin, TX
        else:
            # Find state full name from abbreviation:
            return str(us.states.lookup(matches[1][1:]))

