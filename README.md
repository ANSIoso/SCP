## Link utili

### Buckets
https://console.cloud.google.com/storage/browser?hl=it&inv=1&invt=AbqQqQ&project=progettoscalable-451511&prefix=&forceOnBucketsSortingFiltering=true

### Dataproc
https://console.cloud.google.com/dataproc/clusters?hl=it&inv=1&invt=AbqXBA&project=progettoscalable-451511

### Crediti
https://console.cloud.google.com/billing/018DD1-33FF02-FA1923/credits/all?hl=it&inv=1&invt=AbqQ1g&organizationId=0&walkthrough_id=billing_tour&walkthrough_tutorial_id=billing_tour

## comandi utili

### creazione cluster
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
### aumento numero worker per un cluster




## Steps esecuzione codice

1) installare &rarr; gcloud

2) inizializzare gcloud &rarr; gcloud init

3) gcloud auth login <!-- evitabile? -->

4) creazione venv &rarr; python -m venv myvenv

5) attivazione venv &rarr; source myvenv/venv/activate

6) installare &rarr; pip install google-cloud-storage

7) eseguire lo script pyton per la creazione dei bucket
    ``` 
        pyhton  script_cloud/inizializzazione.py

    ```
8) eseguire lo script pyton per l'esecuzione dei cluster

    ```
        pyhton  script_cloud/master.py
    ```

9) eseguire lo script pyton per eliminare i bucket

    ```
        pyhton  script_cloud/master.py
    ```

gs://bucket_test_fusillo/order_products.csv