import os
from google.cloud import storage
import concurrent.futures

class GoogleCloudStorageTools():
    def __init__(self, credential_file_path):
        # 設定google cloud storage的認證檔案路徑
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_file_path
        self.self.storage_client = storage.Client()
    
    # 給定bucket名稱和blob名稱，返回blib物件
    def get_blob(self, bucket_name, blob_name):
        """Get a blob from the bucket."""
        bucket = self.storage_client.bucket(bucket_name)
        return bucket.get_blob(blob_name)
    
    # 指定folder，取得其中的blob列表
    def get_blob_list_in_folder(self, bucket_name, folder_name):
        """Lists all the files in the specified folder in the bucket."""
        bucket = self.storage_client.bucket(bucket_name)
        # 加上斜杠以確保只查詢資料夾內的文件
        # 若folder_name 本來沒有/，以下函數可確保它有斜杠，若原本已有，rstrip('/') 會先移除然後再添加一個斜杠。
        prefix = folder_name.rstrip('/') + '/'
        blob_list = list()
        # 透過prefix參數，只列出指定資料夾下的blob
        for blob in bucket.list_blobs(prefix=prefix):
            # 若blob不是資料夾，則加入blob_list（GCS的資料夾是以/結尾的blob）
            if not blob.name.endswith('/'):
                blob_list.append(blob)
        return blob_list
     
    # 待調用函數: 用於上傳file/file_name物件類型的檔案，前者是實際的檔案物件，後者是檔案的路徑
    def _upload_to_bucket(self, bucket_name, blob_data):
        """Uploads a file to the bucket and returns the public URL."""
        blob_name, file_type, src_file = blob_data["blob_name"], blob_data["file_type"], blob_data["file"]
        blob = self.get_blob(bucket_name, blob_name)
        # 若存在metadata，則將metadata加入blob（GCS稱為中繼資料）
        if "metadata" in blob_data:
            blob.metadata = blob_data["metadata"]
        # 依照檔案類型選擇不同上傳方式
        if file_type == "file":
            blob.upload_from_file(src_file)
        elif file_type == "file_name":
            blob.upload_from_filename(src_file)
        # 設定blob為公開，並返回blob名稱和公開的url
        blob.make_public()
        return blob_name, blob.public_url
        
    # 使用multi-thread，將檔案上傳到google_cloud_storage，檔案類型可為file_name / file物件
    # 須傳入bucket名稱和blob_meta_list，blob_meta_list是一個list
    # 每個元素是一個dict，包含blob_name, file_type, file, metadata等key（metadata為可選，是1個dict）
    # file_type若為file_name，則file是檔案路徑；若為file，則file是檔案物件
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
    
    # 設置blob的metadata
    def set_blob_metadata(self, bucket_name, blob_name, metadata):
        """Set a blob's metadata."""
        blob = self.get_blob(bucket_name, blob_name)
        # 會針對已經有的metadata進行更新，而非直接覆蓋
        blob.metadata = metadata
        blob.patch()

    # 從Google Cloud Storage下载文件到本地
    def download_blob(self, bucket_name, blob_name, file_path):
        blob = self.get_blob(bucket_name, blob_name)
        blob.download_to_filename(file_path)
        print(f"Blob {blob_name} is downloaded to {file_path}.")
        
    # 將blob設為公開
    def make_blob_public(self, bucket_name, blob_name):
        blob = self.get_blob(bucket_name, blob_name)
        blob.make_public()
        return blob.public_url
    
    # 生成一個預設過期時間1小時（3600秒）的簽名URL，可確保用戶以任何方式訪問（目前用於取得memo文字）
    def generate_signed_url(self, bucket_name, blob_name, expiration=3600):
        blob = self.get_blob(bucket_name, blob_name)
        url = blob.generate_signed_url(expiration=expiration)
        return url