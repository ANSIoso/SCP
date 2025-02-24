import subprocess

nomeCluster = "prova"
region = "europe-central2"

def c(num_workers):
    print(f"Creazione del nuovo cluster con {num_workers} worker...")

    nuovoNomeCluster = nomeCluster + "_" + num_workers

    subprocess.run([
        "gcloud", "dataproc", "clusters", "create", nuovoNomeCluster,
        "--region", region,
        "--num-workers", num_workers,
        "--master-boot-disk-size", "240",
        "--worker-boot-disk-size", "240"
    ])


# ===========================================================================================================

# ================================ RUN CON UN NODO ================================

# CREAZIONE
process = subprocess.run(["gcloud", "dataproc","clusters", "create", nomeCluster, 
                "--region", region,
                "--single-node", 
                "--master-boot-disk-size", "240"])

# RUN
process = subprocess.run([
    "gcloud", "dataproc", "jobs", "submit", "spark",
    "--cluster", nomeCluster,
    "--region", region,
    "--jar", "gs://bucket_test_fusillo/ProgettoEsame.jar"
])

# ================================ RUN CON DUE NODI ================================

# PULIZIA
subprocess.run(["gcloud", "dataproc", "clusters", "delete", nomeCluster, "--region", region, "--quiet"])

# CREAZIONE
num_workers = 2
print(f"Creazione del nuovo cluster con {num_workers} worker...")

nuovoNomeCluster = nomeCluster + "_" + str(num_workers)

subprocess.run([
    "gcloud", "dataproc", "clusters", "create", nuovoNomeCluster,
    "--region", region,
    "--num-workers", num_workers,
    "--master-boot-disk-size", "240",
    "--worker-boot-disk-size", "240"
])

# RUN
process = subprocess.run([
    "gcloud", "dataproc", "jobs", "submit", "spark",
    "--cluster", nuovoNomeCluster,
    "--region", region,
    "--jar", "gs://bucket_test_fusillo/ProgettoEsame.jar"
])

# ================================ RUN CON TRE NODI ================================

nuovo_num_workers = 3

print(f"Aggiornamento del cluster {nuovoNomeCluster}: aumento dei worker a {nuovo_num_workers}...")
subprocess.run([
    "gcloud", "dataproc", "clusters", "update", nuovoNomeCluster,
    "--region", region,
    "--num-workers", nuovo_num_workers
])

# RUN
process = subprocess.run([
    "gcloud", "dataproc", "jobs", "submit", "spark",
    "--cluster", nuovoNomeCluster,
    "--region", region,
    "--jar", "gs://bucket_test_fusillo/ProgettoEsame.jar"
])
