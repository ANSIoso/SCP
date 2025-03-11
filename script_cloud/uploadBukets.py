from google.cloud import storage

bucket_name = "co-purchase-bucket"

# creazione del bucket
def create_bucket(bucket_name, location="EU"):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    bucket = storage_client.create_bucket(bucket, location=location)
    print(f"Bucket {bucket.name} cretato in {bucket.location}")

# caricamento di un singolo file nel bucket
def upload_file(bucket_name, source_file_path, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    blob.upload_from_filename(source_file_path)
    print(f"File {source_file_path} caticato in {destination_blob_name}.")


create_bucket(bucket_name)

upload_file(bucket_name, "../order_products.csv", "order_products.csv")
upload_file(bucket_name, "../ProgettoEsame.jar", "ProgettoEsame.jar")

print("creazione del bucket completata")