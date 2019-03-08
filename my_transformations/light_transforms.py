# ### Computazioni leggere
#
# Basta definirle come semplici funzioni,
# e passarle nella pipeline in una **lambda expression (Map, FlatMap, Reduce, ecc)**.
# Non verranno eseguite in parallelo.


def format_result(word_next_word):
    """
    Converts to string a tuple (word, most_likely_successor)
    :param word_next_word: tuple (word, most_likely_successor)
    :return: string "word: most_likely_successor"
    """
    (word, most_likely_successor) = word_next_word
    return "{}: {}".format(word, most_likely_successor)


def count_ones(word_ones):
    """
    Counts the ones associated with a (word, next) pair
    :param word_ones: tuple (word, [1,1,..,1])
    :return: tuple (word, count)
    """
    (word, ones) = word_ones
    return (word, sum(ones))
