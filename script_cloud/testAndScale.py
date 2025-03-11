import subprocess

clusterName = "cluster-scp"
region = "europe-west1"
bucket_name = "co-purchase-bucket"

def createCluster(isSingleNode):
    if(isSingleNode):
        subprocess.run([
            "gcloud", "dataproc", "clusters", "create", clusterName, 
            "--region", region,
            "--single-node", 
            "--master-boot-disk-size", "340",
            "--master-machine-type", "n2-standard-4",
            ])
    else:
        subprocess.run([
            "gcloud", "dataproc", "clusters", "create", clusterName,
            "--region", region,
            "--num-workers", str(2),
            "--master-boot-disk-size", "100",
            "--worker-boot-disk-size", "100",
            "--master-machine-type", "n2-standard-4",
            "--worker-machine-type", "n2-standard-2"
        ])

def updateWorkerNumber(workers):
    subprocess.run([
        "gcloud", "dataproc", "clusters", "update", clusterName,
        "--region", region,
        "--num-workers", str(workers)
    ])


def start(ramD, ramE, workers, cores):
    subprocess.run([
        "gcloud", "dataproc", "jobs", "submit", "spark",
        "--cluster", clusterName,
        "--region", region,
        "--jar", f"gs://{bucket_name}/ProgettoEsame.jar",
        "--properties", f"spark.executor.instances={workers},spark.driver.memory={ramD}g,spark.executor.memory={ramE}g,spark.executor.cores={cores}"
    ])

def deleteCluster():
    subprocess.run(["gcloud", "dataproc", "clusters", "delete", clusterName, "--region", region, "--quiet"])


# ===========================================================================================================

# ================================ RUN CON UN NODO ================================
print(f"---- Creazione del cluster con 1 worker ----")

# CREAZIONE
createCluster(True)

# RUN
start(6, 4, 1, 2)

# PULIZIA
deleteCluster()

# ================================ RUN CON DUE NODI ================================

print(f"---- Creazione del cluster con 2 worker ----")

# CREAZIONE
createCluster(False)

# RUN
start(4, 4, 2, 2)

# ================================ RUN CON TRE NODI ================================

new_num_workers = 3
print(f"---- Aggiornamento del cluster {clusterName}: aumento dei worker a {new_num_workers} ----")

# AGGIORNAMENTO CLUSTER (AUMENTO NODI)
updateWorkerNumber(new_num_workers)

# RUN
start(4, 4, 3, 2)

# # ================================ RUN CON QUATRO NODI ================================

new_num_workers = 4
print(f"---- Aggiornamento del cluster {clusterName}: aumento dei worker a {new_num_workers} ----")

# AGGIORNAMENTO CLUSTER (AUMENTO NODI)
updateWorkerNumber(new_num_workers)

# RUN
start(4, 4, 4, 2)

# PULIZIA
deleteCluster()