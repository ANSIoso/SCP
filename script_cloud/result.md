# Risultati dei run

## con 1 nodo


## con 2 nodi
============================
TEMPO TOTALE: 349.259 secondi
============================


## con 3 nodi
============================
TEMPO TOTALE: 313.122 secondi
============================


## con 4 nodi
============================
TEMPO TOTALE: 271.832 secondi
============================

gcloud dataproc jobs submit spark \
    --cluster prova \
    --region europe-central2 \
    --jar gs://co-purchase-bucket/ProgettoEsame.jar \
    --properties=spark.driver.memory=6g,spark.executor.memory=6g,spark.executor.instances=1,spark.executor.cores=4

gcloud dataproc clusters delete prova --region europe-central2 --quiet
