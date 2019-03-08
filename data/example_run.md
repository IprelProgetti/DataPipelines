# Esempi di lancio della pipeline

## Prerequisiti per l'utilizzo

    git clone https://github.com/horns-g/DataPipelines.git
    cd DataPipelines

## Parte 1

Un notebook Jupyter consultabile (o eseguibile) per prendere confidenza con le APIs di Apache Beam in Python2.

    cd Pt1
    jupyter notebook

Sulla home page di Jupyter che si aprirà sul browser, selezionare il notebook `BeamPipelines.ipynb` ed eseguirlo cella per cella.


## Parte 2

Uno script Python2 tramite cui eseguire la pipeline su *Google Cloud Dataflow*, con le opzioni di esecuzione hard-coded nel sorgente.

## Contenuto principale

Progetto Python2 che mostra come strutturare una pipeline Apache Beam in un contesto reale.

Con questa struttura la pipeline può essere eseguita sia in locale sia in remoto in modo del tutto trasparente.

#### Esecuzione in locale

Per eseguire la pipeline in locale (--runner DirectRunner):

* con input e output di default (risp: `data/alice.txt` e `data/alice_processed`):

        python cloud_pipeline_2.py
        
* specificando i file di input e/o output:

        python cloud_pipeline_2.py \
            --input path/to/my-input-file.ext \
            --output path/to/my-output-file.ext
            
            
#### Esecuzione su Google Cloud Dataflow

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
