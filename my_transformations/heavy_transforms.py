import apache_beam as beam
import re


# ### Computazioni pesanti
#
# Andranno messe dentro i **ParDo**,
# che verranno eseguiti in parallelo su worker diversi
# al fine di ridurre l'overhead computazionale.


class ComputeCleanLineFn(beam.DoFn):
    def process(self, element, **kwargs):
        """
        Splits a text line into a list of tuples (word, next_word)
        :param element: a text line, typed as string e.g. "Hi, I am Gabriele!"
        :param kwargs: additional key-value pairs params that could be added
        :return: a list of string tuples e.g. [("hi", "i"), ("i", "am"), ("am", "gabriele")]
        """
        regex = r'[a-zA-Z]+'  # r'\w+' to include numbers as well
        line_words = re.findall(regex, element.lower())  # clean punctuation: get a list of (re)
        words_to_tuples = [(line_words[i], line_words[i+1]) for i in range(len(line_words)-1)]
        return words_to_tuples


class ExtractMostLikelyNextWordFn(beam.DoFn):
    def process(self, element, **kwargs):
        """
        Pairs a word with its most likely successor.
        :param element: a tuple, encoded as (word, [(next_word_i, count_i), ..., (next_word_n, count_n)])
        :param kwargs: additional key-value pairs params that could be added
        :return: the tuple (word, most_likely_next_word)
        """
        word, next_list = element
        next_list = list(next_list)  # per eseguire funzioni su liste su cloud (eg: sort) forzare i tipi!
        next_list.sort(key=lambda wc_tuple: (-wc_tuple[1], wc_tuple[0]))
        most_likely_successor = next_list[0][0]
        return [(word, most_likely_successor)]
