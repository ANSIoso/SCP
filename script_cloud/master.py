import subprocess

nomeCluster = "prova"
region = "europe-west1"
bucket_name = "co-purchase-bucket"

def crea(singolo):
    # CREAZIONE

    if(singolo):
        subprocess.run([
            "gcloud", "dataproc", "clusters", "create", nomeCluster, 
            "--region", region,
            "--zone", "europe-west1-b",
            "--single-node", 
            "--master-boot-disk-size", "340",
            "--master-machine-type", "n2-standard-4",
            ])
    else:
        subprocess.run([
            "gcloud", "dataproc", "clusters", "create", nomeCluster,
            "--region", region,
            "--zone", "europe-west1-b",
            "--num-workers", str(2),
            "--master-boot-disk-size", "100",
            "--worker-boot-disk-size", "100",
            "--master-machine-type", "n2-standard-4",
            "--worker-machine-type", "n2-standard-2"
        ])

def ingrandisci(workers):
    subprocess.run([
        "gcloud", "dataproc", "clusters", "update", nomeCluster,
        "--region", region,
        "--num-workers", str(workers)
    ])


def start(ramD, ramE, workers, cores):
    subprocess.run([
        "gcloud", "dataproc", "jobs", "submit", "spark",
        "--cluster", nomeCluster,
        "--region", region,
        "--jar", f"gs://{bucket_name}/ProgettoEsame.jar",
        "--properties", f"spark.executor.instances={workers},spark.driver.memory={ramD}g,spark.executor.memory={ramE}g,spark.executor.cores={cores}"
    ])

def pulisci():
    subprocess.run(["gcloud", "dataproc", "clusters", "delete", nomeCluster, "--region", region, "--quiet"])


# ===========================================================================================================

# ================================ RUN CON UN NODO ================================
print(f"---- Creazione del cluster con 1 worker ----")

# CREAZIONE
crea(True)

# RUN
start(6, 4, 1, 2)

# PULIZIA
pulisci()

# ================================ RUN CON DUE NODI ================================

print(f"---- Creazione del cluster con 2 worker ----")

# CREAZIONE
crea(False)

# RUN
start(4, 4, 2, 2)

# ================================ RUN CON TRE NODI ================================

new_num_workers = 3
print(f"---- Aggiornamento del cluster {nomeCluster}: aumento dei worker a {new_num_workers} ----")

# AGGIORNAMENTO CLUSTER (AUMENTO NODI)
ingrandisci(new_num_workers)

# RUN
start(4, 4, 3, 2)

# # ================================ RUN CON QUATRO NODI ================================

new_num_workers = 4
print(f"---- Aggiornamento del cluster {nomeCluster}: aumento dei worker a {new_num_workers} ----")

# AGGIORNAMENTO CLUSTER (AUMENTO NODI)
ingrandisci(new_num_workers)

# RUN
start(4, 4, 4, 2)

# PULIZIA
pulisci()