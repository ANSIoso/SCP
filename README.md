## Link utili

### Bukets
https://console.cloud.google.com/storage/browser?hl=it&inv=1&invt=AbqQqQ&project=progettoscalable-451511&prefix=&forceOnBucketsSortingFiltering=true

### Dataproc
https://console.cloud.google.com/dataproc/clusters?hl=it&inv=1&invt=AbqXBA&project=progettoscalable-451511

### Crediti
https://console.cloud.google.com/billing/018DD1-33FF02-FA1923/credits/all?hl=it&inv=1&invt=AbqQ1g&organizationId=0&walkthrough_id=billing_tour&walkthrough_tutorial_id=billing_tour



## Comandi shell utili

### ===== CREAZIONE CLUSTERS =====
gcloud dataproc clusters create <nome> --region europe-central2 --single-node  --master-boot-disk-size 240
gcloud dataproc clusters create <nome> --region=<regione> --num-workers <n> --master-boot-disk-size 240 --worker-boot-disk-size 240

### ===== SUBMIT JOB =====
gcloud dataproc jobs submit spark --cluster=<nome> --region=<regione> --jar=gs://<bucket>/<nome-jar>.jar

### ===== CANCELLAZIONE CLUSTERS =====
gcloud dataproc clusters delete <nome> --region <regione>


## Steps esecuzione codice
1) installare -> pip install google-cloud-storage
2) installare -> gcloud
3) gcloud auth application-default login


gs://bucket_test_fusillo/order_products.csv