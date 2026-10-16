from google.cloud import storage
from utils import getProjectId

def upload_batch_file_to_gcs(bucket_name, local_file_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    # Define o caminho exato exigido pelo seu Workflow
    destination_blob_name = "batch-incoming/transactions_batch.parquet"
    
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_file_path)
    
    print(f"Arquivo batch enviado com sucesso para: gs://{bucket_name}/{destination_blob_name}")

if __name__ == "__main__":
    projectId = getProjectId()
    upload_batch_file_to_gcs(f"{projectId}-storage-bucket", "batch_data.parquet")