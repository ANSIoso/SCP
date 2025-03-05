import subprocess

nomeCluster = "prova"
region = "europe-central2"
bucket_name = "co-purchase-bucket"


# ===========================================================================================================

# ================================ RUN CON UN NODO ================================

# CREAZIONE
process = subprocess.run(["gcloud", "dataproc", "clusters", "create", nomeCluster, 
                "--region", region,
                "--single-node", 
                "--master-boot-disk-size", "240"])

# RUN
process = subprocess.run([
    "gcloud", "dataproc", "jobs", "submit", "spark",
    "--cluster", nomeCluster,
    "--region", region,
    "--jar", "gs://" + bucket_name + "/ProgettoEsame.jar"
    # ,"--properties=spark.ui.showConsoleProgress=false,spark.eventLog.enabled=false,spark.logConf=false"
])

# ================================ RUN CON DUE NODI ================================

# PULIZIA
subprocess.run(["gcloud", "dataproc", "clusters", "delete", nomeCluster, "--region", region, "--quiet"])
 
# # CREAZIONE
# num_workers = 2
# print(f"Creazione del nuovo cluster con {num_workers} worker...")

# subprocess.run([
#     "gcloud", "dataproc", "clusters", "create", nomeCluster,
#     "--region", region,
#     "--num-workers", str(num_workers),
#     "--master-boot-disk-size", "240",
#     "--worker-boot-disk-size", "240"
# ])

# # RUN
# process = subprocess.run([
#     "gcloud", "dataproc", "jobs", "submit", "spark",
#     "--cluster", nomeCluster,
#     "--region", region,
#     "--jar", "gs://" + bucket_name + "/ProgettoEsame.jar"
# ])

# # ================================ RUN CON TRE NODI ================================

# num_workers = 3

# # AGGIORNAMENTO CLUSTER (AUMENTO NODI)
# print(f"Aggiornamento del cluster {nomeCluster}: aumento dei worker a {num_workers}...")
# subprocess.run([
#     "gcloud", "dataproc", "clusters", "update", nomeCluster,
#     "--region", region,
#     "--num-workers", num_workers
# ])

# # RUN
# process = subprocess.run([
#     "gcloud", "dataproc", "jobs", "submit", "spark",
#     "--cluster", nomeCluster,"gs://" + bucket_name + "/ProgettoEsame.jar"
#     "--region", region,
#     "--jar", "gs://" + bucket_name + "/ProgettoEsame.jar"
# ])
