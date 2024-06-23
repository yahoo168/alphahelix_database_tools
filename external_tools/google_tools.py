import os, re
from datetime import datetime
from google.cloud import storage
import concurrent.futures

class GoogleCloudStorageTools():
    def __init__(self, credential_file_path=None):
        if credential_file_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_file_path
        
    # 待調用函數: 用於上傳file or file_name物件類型的檔案
    def _upload_to_bucket(self, bucket_name, blob_data):
        """Uploads a file to the bucket and returns the public URL."""
        blob_name = blob_data["blob_name"]
        file_type = blob_data["file_type"]  # "file" or "file_name"
        src_file = blob_data["file"]
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Set custom metadata if present
        if "metadata" in blob_data:
            blob.metadata = blob_data["metadata"]

        # Upload the file based on the file_type
        if file_type == "file":
            blob.upload_from_file(src_file)
        elif file_type == "file_name":
            blob.upload_from_filename(src_file)

        blob.make_public()
        return blob_name, blob.public_url
        
    # 將檔案上傳到google_cloud_storage，file可為file_name或file物件
    def upload_to_google_cloud_storage(self, bucket_name, blob_meta_list):
        blob_url_dict = {}
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_blob_name = {
                executor.submit(self._upload_to_bucket, bucket_name, blob_meta): blob_meta["blob_name"]
                for blob_meta in blob_meta_list
            }

            for future in concurrent.futures.as_completed(future_to_blob_name):
                blob_name = future_to_blob_name[future]
                try:
                    name, url = future.result()
                    blob_url_dict[name] = url
                except Exception as e:
                    print(f"Error uploading file {blob_name}: {e}")

        return blob_url_dict

    def get_blob(self, bucket_name, blob_name):
        """Get a blob from the bucket."""
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        return bucket.get_blob(blob_name)

    def get_blob_list_in_folder(self, bucket_name, folder_name):
        """Lists all the files in the specified folder in the bucket."""
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # 加上斜杠以確保只查詢資料夾內的文件
        # 如果 folder_name 本來就沒有斜杠，這樣做可以確保它有一個斜杠，如果 folder_name 已經有斜杠，rstrip('/') 會先移除它，然後再添加一個斜杠。
        prefix = folder_name.rstrip('/') + '/'
        blob_list = list()
        for blob in bucket.list_blobs(prefix=prefix):
            if not blob.name.endswith('/'):
                blob_list.append(blob)
        return blob_list

    def set_blob_metadata(self, bucket_name, blob_name, metadata):
        """Set a blob's metadata."""
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.get_blob(blob_name)
        blob.metadata = metadata
        blob.patch()

    # 从 Google Cloud Storage 下载文件
    def download_blob(self, bucket_name, blob_name, file_path):
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.download_to_filename(file_path)
        print(f"Blob {blob_name} is downloaded to {file_path}.")

    def make_blob_public(self, bucket_name, blob_name):
        """将 GCS 上的 blob 设置为公开访问"""
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.make_public()
        return blob.public_url

    def generate_signed_url(self, bucket_name, blob_name, expiration=3600):
        """生成一个带有过期时间的签名 URL"""
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        url = blob.generate_signed_url(expiration=expiration)
        return url