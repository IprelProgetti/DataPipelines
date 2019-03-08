import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import time
import argparse
# Le trasformazioni sono importate da un package specifico
from my_transformations.heavy_transforms import ComputeCleanLineFn, ExtractMostLikelyNextWordFn
from my_transformations.light_transforms import count_ones, format_result


BUCKET = "claudia-bucket/data-pipelines"
DEFAULT_INPUT_FILE = "data/alice.txt"  # "gs://{}/datasets/alice.txt".format(BUCKET)
DEFAULT_OUTPUT_FILE = 'data/alice_processed'  # "gs://{}/output/alice_processed".format(BUCKET)


def run(argv=None):
    # ### Pipeline Options
    #
    # Vengono inserite da terminale ogni volta che si lancia lo script.
    #
    # Il parser degli argv conosce solamente le impostazioni "extra" stabilite dall'utente,
    # in questo caso il path ai file di input e output. Questi vengono inseriti nei "known_args".
    #
    # Tutti gli altri parametri sono gestiti direttamente dalla pipeline di Apache Beam (validazione inclusa).
    # Non conoscendoli, il parser li mettera' tra gli "unknown_args", qui rinominati in "pipeline_args".

    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        default=DEFAULT_INPUT_FILE,
                        help='Path completo al file in input alla pipeline')
    parser.add_argument('--output',
                        dest='output',
                        default=DEFAULT_OUTPUT_FILE,  # required=True
                        help='Path completo al file su cui andra scritto il risultato della pipeline')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # preparare le opzioni di esecuzione
    pipeline_options = PipelineOptions(pipeline_args)
    # forzare save_main_session = True a prescindere dai settaggi utente
    pipeline_options.view_as(SetupOptions).save_main_session = True

    print("pipeline started")
    start_time = time.time()
    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | 'ReadInputFile' >> beam.io.ReadFromText(known_args.input)
         # -> list lines
         | 'CleanLines' >> beam.ParDo(ComputeCleanLineFn())
         # -> list tuple(word, next)
         | 'WordNextWithOne' >> beam.Map(lambda x: (x, 1))
         # -> tuple((word, next), 1)
         | 'GroupByWordNext' >> beam.GroupByKey()
         # -> tuple((word, next), [1,1,..1])
         | 'WordNextWithCount' >> beam.Map(count_ones)
         # -> tuple((word, next), count))
         | 'Reshape' >> beam.Map(lambda x: (x[0][0], (x[0][1], x[1])))
         # -> tuple(word, (next, count))
         | 'GroupByWord' >> beam.GroupByKey()
         # -> tuple(word, [(next_i, count_i), .., (next_n, count_n)])
         | 'ChooseSuccessor' >> beam.ParDo(ExtractMostLikelyNextWordFn())
         # -> list tuple(word, next_k)
         | 'FormatResult' >> beam.Map(format_result)
         # string "word: next_k"
         | 'WriteResult' >> beam.io.WriteToText(known_args.output)
         # output file written
         )

    print("elapsed time: {}s".format(time.time() - start_time))


if __name__ == '__main__':
    run()
