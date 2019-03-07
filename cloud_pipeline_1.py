#!/usr/bin/env python
# coding: utf-8

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import re
import time


# ### Pipeline Options
#
# Per semplicità, vengono per il momento settate da codice. Per casi concreti, inserirle da terminale.
#
# Alcune sono obbligatorie: project, job_name, staging_location, temp_location e runner vanno **sempre** specificate.
#
# Le altre opzioni possono essere: facoltative di GCP (eg save_main_session, num_workers, ...) o custom dell'utente (input_location, output_location, ...).

PROJECT = "claudia-assistant"  # GCP project
JOB_NAME = "alice-job"  # Dataflow Job name
BUCKET = "claudia-bucket/data-pipelines"  # cloud storage file repository
RUNNER = "DataflowRunner"  # per lanciare la pipeline su GCP
argv = [
    '--project={}'.format(PROJECT),
    '--job_name={}'.format(JOB_NAME),
    '--staging_location={}'.format("gs://{}/temp".format(BUCKET)),
    '--temp_location={}'.format("gs://{}/temp".format(BUCKET)),
    '--runner={}'.format(RUNNER),
    '--num_workers=8',  # numero di worker su cui parallelizzare il calcolo
    '--max_num_workers=10',  # massimi numero di worker in caso di autoscaling
    '--save_main_session',  # per salvare e caricare lo stato globale del namespace in ogni worker. obbligatorio per poter usare eventuali librerie importate distribuendo la computazione su più worker.
    '--region=europe-west1'  # regione di deployment del job
]

# ### Computazioni pesanti
#
# Andranno messe dentro i **ParDo**, che verranno eseguiti in parallelo su worker diversi al fine di ridurre l'overhead computazionale.

class ComputeCleanLineFn(beam.DoFn):
    def process(self, element):
        """
        Splits a text line into a list of tuples (word, next_word)
        element: a text line, typed as string e.g. "Hi, I am Gabriele!"
        return: a list of string tuples e.g. [("hi", "i"), ("i", "am"), ("am", "gabriele")]
        """
        regex = r'[a-zA-Z]+'  # r'\w+' to include numbers as well
        line_words = re.findall(regex, element.lower())  # clean punctuation: get a list of (re)
        words_to_tuples = [(line_words[i], line_words[i+1]) for i in range(len(line_words)-1)]
        return words_to_tuples



class ExtractMostLikelyNextWordFn(beam.DoFn):
    def process(self, element):
        """
        Pairs a word with its most likely successor.
        element: a tuple, encoded as (word, [(next_word_i, count_i), ..., (next_word_n, count_n)])
        return: the tuple (word, most_likely_next_word)
        """
        word, next_list = element
        next_list = list(next_list)  # per eseguire funzioni su liste su cloud (eg: sort) forzare i tipi!
        next_list.sort(key=lambda wc_tuple: (-wc_tuple[1], wc_tuple[0]))
        most_likely_successor = next_list[0][0]
        return [(word, most_likely_successor)]


# ### Computazioni leggere
#
# Basta definirle come semplici funzioni e passarle nella pipeline in una **lambda expression (Map, FlatMap, Reduce, ecc)**. Non verranno eseguite in parallelo.

def format_result(word_next_word):
    """
    Converts to string a tuple (word, most_likely_successor)
    word_next_word: tuple (word, most_likely_successor)
    return: string "word: most_likely_successor"
    """
    (word, most_likely_successor) = word_next_word
    return "{}: {}".format(word, most_likely_successor)

def count_ones(word_ones):
    """
    Counts the ones associated with a (word, next) pair
    word_ones: tuple (word, [1,1,..,1])
    return: tuple (word, count)
    """
    (word, ones) = word_ones
    return (word, sum(ones))


INPUT_FILE = "gs://{}/datasets/alice.txt".format(BUCKET)  # 'alice.txt'
OUTPUT_FILE = "gs://{}/output/alice_processed".format(BUCKET)  # 'alice_processed.txt'

start_time = time.time()
with beam.Pipeline(argv=argv) as p:
    output = (p
            | 'ReadInputFile'     >> beam.io.ReadFromText(INPUT_FILE)
                                  # -> list lines
            | 'CleanLines'        >> beam.ParDo(ComputeCleanLineFn())
                                  # -> list tuple(word, next)
            | 'WordNextWithOne'   >> beam.Map(lambda x: (x, 1))
                                  # -> tuple((word, next), 1)
            | 'GroupByWordNext'   >> beam.GroupByKey()
                                  # -> tuple((word, next), [1,1,..1])
            | 'WordNextWithCount' >> beam.Map(count_ones)
                                  # -> tuple((word, next), count))
            | 'Reshape'           >> beam.Map(lambda x: (x[0][0], (x[0][1], x[1])))
                                  # -> tuple(word, (next, count))
            | 'GroupByWord'       >> beam.GroupByKey()
                                  # -> tuple(word, [(next_i, count_i), .., (next_n, count_n)])
            | 'ChooseSuccessor'   >> beam.ParDo(ExtractMostLikelyNextWordFn())
                                  # -> list tuple(word, next_k)
            | 'FormatResult'      >> beam.Map(format_result)
                                  # string "word: next_k"
            | 'WriteResult'       >> beam.io.WriteToText(OUTPUT_FILE)
                                  # output file written
    )
    # result = p.run()
    # result.wait_until_finish()

print("elapsed time: {}s".format(time.time() - start_time))
