from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import storage
from googleapiclient.errors import HttpError

import concurrent.futures
import mimetypes
import os, logging
import pandas as pd

class GoogleCloudStorageTools():
    def __init__(self, credential_file_path):
        # 設定google cloud storage的認證檔案路徑
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_file_path
        self.storage_client = storage.Client()
    
    # 給定bucket名稱和blob名稱，返回blib物件
    def get_blob(self, bucket_name, blob_name):
        """Get a blob from the bucket."""
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.get_blob(blob_name)
        # 若blob不存在，則創建一個新的空blob
        if blob is None:
            blob = bucket.blob(blob_name)
        return blob
    
    def get_blob_size(self, bucket_name, blob_name):
        """取得指定 bucket 中 blob_name 的檔案大小"""
        blob = self.get_blob(bucket_name, blob_name)
        if blob.exists():  # 檢查 blob 是否存在
            return blob.size  # 返回 blob 的大小
        else:
            print(f"Blob {blob_name} 不存在於 {bucket_name} bucket 中")
            return None
    
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
        logging.info(blob_name)
        logging.info(blob)
        # 若存在metadata，則將metadata加入blob（GCS稱為中繼資料）
        if "metadata" in blob_data:
            blob.metadata = blob_data["metadata"]
        
        # 設定Content-Type（可盡量依照原始檔案的Content-Type設定在GCS的Content-Type）
        # Content-Type若為pdf，則在瀏覽器中會直接顯示，而非下載
        content_type, _ = mimetypes.guess_type(blob_name)
        if content_type:
            blob.content_type = content_type
        else:
            # 默认设置为二进制文件类型
            blob.content_type = "application/octet-stream"
            
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
                    logging.info(f"Error uploading file {blob_name}: {e}")

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
        logging.info(f"Blob {blob_name} is downloaded to {file_path}.")
        
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
 
class GoogleDriveTools():
    def __init__(self, credential_file_path):
        # 設定google drive的讀取權限
        SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets.readonly"]
        # 建立Google Drive的credentials
        credentials = service_account.Credentials.from_service_account_file(credential_file_path, scopes=SCOPES)
        # 給定根目錄id：與我共用的資料夾/質化報告_標的/
        self.ROOT_FOLDER_ID = "1-h_98q7snKNXpCcdIG4Wg9HYHqkp6Oxm" 
        # 建立 Google Drive 和 Google Sheets 服務
        self.drive_service = build('drive', 'v3', credentials=credentials)
        self.sheets_service = build('sheets', 'v4', credentials=credentials)

    # 根據資料夾名稱查找資料夾 ID，須給定上層資料夾ID
    def _get_folder_id(self, parent_id, folder_name):
        query = f"'{parent_id}' in parents and name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
        results = self.drive_service.files().list(q=query, fields="files(id, name)").execute()
        items = results.get('files', [])
        if not items:
            return None
        # 假設名稱唯一，返回第一個匹配的資料夾 ID
        else:
            return items[0]['id']
    
    def get_folder_id_by_path(self, folder_path, parent_id=None):
        # 解析巢狀資料夾路徑
        folder_name_list = folder_path.split('/')
        # 若未指定上層資料夾ID，則自根目錄開始查找
        if not parent_id:
            parent_id = self.ROOT_FOLDER_ID
        for folder_name in folder_name_list:
            # 迭代查詢指定的資料夾路徑，若中途找不到資料夾則返回None
            folder_id = self._get_folder_id(parent_id, folder_name)
            if not folder_id:
                return None
            parent_id = folder_id
        return folder_id
    
    # 創建新資料夾，並返回資料夾 ID
    def _create_folder(self, parent_folder_id, folder_name):
        # 定义新資料夾的元数据
        folder_metadata = {
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_folder_id],  # 设置父資料夾
            'name': folder_name,  # 新資料夾的名称
        }
        
        # 创建資料夾
        file = self.drive_service.files().create(body=folder_metadata, fields='id').execute()
        file_id = file.get('id')
        logging.info('[SERVER][Google Drive] Create Folder ID: %s' % file.get('id'))
        return file_id
    
    def _is_folder(self, item):
        return item['mimeType'] == 'application/vnd.google-apps.folder'
    
    # 列出資料夾中的所有物件（資料夾和文件）
    def list_items_in_folder_by_id(self, folder_id):
        query = f"'{folder_id}' in parents and trashed=false"
        results = self.drive_service.files().list(q=query, fields="files(id, name, mimeType)").execute()
        items = results.get('files', [])
        # 添加物件的下载链接和查看链接
        for item in items:
            item['download_url'] = f"https://drive.google.com/uc?export=download&id={item['id']}"
            item['view_url'] = f"https://drive.google.com/file/d/{item['id']}/view"
            
        return items
    
    # 取得特定資料夾的網址，以及其包含的檔案meta list
    def list_items_in_folder_by_path(self, folder_path):
        # 根據最終資料夾，列出其中的檔案
        folder_id = self.get_folder_id_by_path(folder_path)
        item_meta_list = self.list_items_in_folder_by_id(folder_id)
             
        # 包裝為dict
        result_dict = {
            "folder_url": f"https://drive.google.com/drive/folders/{folder_id}",
            "item_meta_list": item_meta_list
        }
        return result_dict
    
    # 複製資料夾（包含資料夾中的所有文件和資料夾）
    def copy_folder(self, src_folder_id, des_folder_name, des_parent_folder_id):
        try:
            # 在目标資料夾中创建新的資料夾
            des_folder_id = self._create_folder(des_parent_folder_id, des_folder_name)
            # 获取源資料夾中的所有项目
            items = self.list_items_in_folder_by_id(src_folder_id)
            for item in items:
                # 判断是否为資料夾
                if self._is_folder(item):
                    # 若是資料夾，遞歸複製
                    self.copy_folder(item['id'], item['name'], des_folder_id)
                    logging.info(f"Copied Folder '{item['name']}' to folder ID: {des_folder_id}")
                else:
                    # 若是文件，直接複製到指定資料夾
                    self.copy_file(item['id'], des_folder_id)
                
        except HttpError as error:
            logging.info(f'An error occurred: {error}')
    
    # 複製文件到指定資料夾
    def copy_file(self, file_id, folder_id):
        try:
            # 获取原文件元数据
            file_metadata = self.drive_service.files().get(fileId=file_id, fields='name').execute()
            new_file_metadata = {
                'name': file_metadata['name'],
                'parents': [folder_id]
            }
            # 复制文件
            self.drive_service.files().copy(fileId=file_id, body=new_file_metadata).execute()
            logging.info(f"Copied File '{file_metadata['name']}' to folder ID: {folder_id}")
        except HttpError as error:
            logging.info(f'An error occurred: {error}')
        
    # 讀取Google Sheets的檔案內容（須指定sheet name），並返回DataFrame
    def get_spreadsheet_data(self, file_id, sheet_name):
        # Call the Sheets API
        result = self.sheets_service.spreadsheets().values().get(spreadsheetId=file_id, range=sheet_name).execute()
        values = result.get('values', [])
        
        if not values:
            logging.warn('No data found.')
            return None
        else:
            return pd.DataFrame(values[1:], columns=values[0])