from google.cloud import storage

def delete_all_buckets():
    client = storage.Client()
    buckets = list(client.list_buckets())

    if not buckets:
        print("Nessun bucket trovato.")
        return

    for bucket in buckets:
        try:
            print(f"Eliminando il bucket: {bucket.name}")
            bucket = client.get_bucket(bucket.name)

            # Eliminare tutti gli oggetti nel bucket
            blobs = list(bucket.list_blobs())
            for blob in blobs:
                blob.delete()

            # Eliminare il bucket
            bucket.delete()
            print(f"Bucket {bucket.name} eliminato con successo.")

        except Exception as e:
            print(f"Errore nell'eliminazione del bucket {bucket.name}: {e}")

delete_all_buckets()
