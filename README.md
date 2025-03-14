## Link utili

### Buckets
https://console.cloud.google.com/storage

### Dataproc
https://console.cloud.google.com/dataproc

### Crediti
https://console.cloud.google.com/billing/credits


## Steps esecuzione codice

1) installare gcloud

2) inizializzare gcloud &rarr; gcloud init

3) gcloud auth login <!-- evitabile? -->

4) creazione venv &rarr; python -m venv myvenv

5) attivazione venv &rarr; source myvenv/venv/activate

6) installare google-cloud-storage per python &rarr; pip install google-cloud-storage

7) eseguire lo script pyton per la creazione dei bucket
    ``` 
        pyhton  script_cloud/inizializzazione.py

    ```
8) eseguire lo script pyton per l'esecuzione dei cluster

    ```
        pyhton  script_cloud/master.py
    ```

9) eseguire lo script pyton per eliminare i bucket (attenzione eliminerà TUTTI i bucket)

    ```
        pyhton  script_cloud/master.py
    ```

## comandi utili

### creazione cluster (single-node)
```
gcloud dataproc clusters create cluster-scp \
    --region europe-west1 \
    --single-node \
    --master-boot-disk-size 340 \
    --master-machine-type n2-standard-4 \
    --bucket co-purchase-bucket
```

### creazione cluster (no single-node)
```
    gcloud dataproc clusters create cluster-scp \
    --region europe-west1 \
    --num-workers 4 \
    --master-boot-disk-size 100 \
    --worker-boot-disk-size 100 \
    --master-machine-type n2-standard-4 \
    --worker-machine-type n2-standard-2 \
    --bucket co-purchase-bucket
```

### run del job
```
    gcloud dataproc jobs submit spark \
    --cluster cluster-scp \
    --region europe-west1 \
    --jar gs://co-purchase-bucket/ProgettoEsame.jar \
    --properties spark.executor.instances=4,spark.driver.memory=4g,spark.executor.memory=4g,spark.executor.cores=2

```