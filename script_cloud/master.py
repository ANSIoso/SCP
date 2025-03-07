import subprocess

nomeCluster = "prova"
region = "europe-central2"
bucket_name = "co-purchase-bucket"

def crea(singolo):
    # CREAZIONE

    if(singolo):
        subprocess.run([
            "gcloud", "dataproc", "clusters", "create", nomeCluster, 
            "--region", region,
            "--single-node", 
            "--master-boot-disk-size", "240",
            "--master-machine-type", "n2-standard-2"
            ])
    else:
        subprocess.run([
            "gcloud", "dataproc", "clusters", "create", nomeCluster,
            "--region", region,
            "--num-workers", str(2),
            "--master-boot-disk-size", "240",
            "--worker-boot-disk-size", "240",
            "--master-machine-type", "n2-standard-2",
            "--worker-machine-type", "n2-standard-2"
        ])

def ingrandisci(workers):
    subprocess.run([
        "gcloud", "dataproc", "clusters", "update", nomeCluster,
        "--region", region,
        "--num-workers", str(workers)
    ])


def start(workers, ramD, ramE, cores):
    subprocess.run([
        "gcloud", "dataproc", "jobs", "submit", "spark",
        "--cluster", nomeCluster,
        "--region", region,
        "--jar", f"gs://{bucket_name}/ProgettoEsame.jar",
        "--properties", f"spark.driver.memory={ramD}g,spark.executor.memory={ramE}g,spark.executor.instances={workers},spark.executor.cores={cores}"
    ])

def pulisci():
    subprocess.run(["gcloud", "dataproc", "clusters", "delete", nomeCluster, "--region", region, "--quiet"])


# ===========================================================================================================

# ================================ RUN CON UN NODO ================================
print(f"---- Creazione del cluster con 1 worker ----")

# CREAZIONE
crea(True)

# RUN
start(1, 6, 5, 2)

# PULIZIA
pulisci()

# ================================ RUN CON DUE NODI ================================

print(f"---- Creazione del cluster con 2 worker ----")

# CREAZIONE
crea(False)

# RUN
start(2, 5, 4, 2)

# ================================ RUN CON TRE NODI ================================

new_num_workers = 2
print(f"---- Aggiornamento del cluster {nomeCluster}: aumento dei worker a {new_num_workers} ----")

# AGGIORNAMENTO CLUSTER (AUMENTO NODI)
ingrandisci(new_num_workers)

# RUN
start(3, 4, 2, 2)

# # ================================ RUN CON QUATRO NODI ================================

new_num_workers = 3
print(f"---- Aggiornamento del cluster {nomeCluster}: aumento dei worker a {new_num_workers} ----")

# AGGIORNAMENTO CLUSTER (AUMENTO NODI)
ingrandisci(new_num_workers)

# RUN
start(4, 2, 2, 2)

# PULIZIA
pulisci()