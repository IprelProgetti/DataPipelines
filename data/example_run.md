# Esempi di lancio della pipeline

## Prerequisiti

    cd DataPipelines

## In locale

Per eseguire la pipeline in locale (--runner DirectRunner):

* con input e output di default (risp: `data/alice.txt` e `data/alice_processed`):

        python cloud_pipeline_2.py
        
* specificando i file di input e/o output:

        python cloud_pipeline_2.py \
            --input path/to/my-input-file.ext \
            --output path/to/my-output-file.ext
            
            
## Su Google Cloud Dataflow

Per eseguire la pipeline su Cloud specificare anche le opzioni di Apache Beam:

        python cloud_pipeline_2.py \
            --project claudia-assistant \
            --staging_location gs://claudia-bucket/data-pipelines/temp \
            --temp_location gs://claudia-bucket/data-pipelines/temp \
            --runner DataflowRunner \
            --region europe-west1 \
            --num_workers 8 \
            --input gs://claudia-bucket/data-pipelines/datasets/alice.txt \
            --output gs://claudia-bucket/data-pipelines/output/alice_processed \
            --job_name alice-job \
            --setup_file ./setup.py

Per monitorare l'esecuzione della pipeline, collegarsi alla dashboard web di Dataflow.
