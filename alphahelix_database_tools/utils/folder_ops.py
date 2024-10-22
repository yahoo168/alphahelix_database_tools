import os
import shutil

def delete_folder_files(folder_path):
  # 確保資料夾存在
  if os.path.exists(folder_path):
      # 刪除資料夾中的所有檔案
      for file_name in os.listdir(folder_path):
          file_path = os.path.join(folder_path, file_name)
          
          try:
              if os.path.isfile(file_path) or os.path.islink(file_path):
                  os.unlink(file_path)  # 刪除檔案或符號連結
              elif os.path.isdir(file_path):
                  shutil.rmtree(file_path)  # 刪除資料夾及其內容
                  
          except Exception as e:
              print(f"刪除 {file_path} 時發生錯誤: {e}")

      print(f"已刪除資料夾: {folder_path}")

  else:
      print(f"資料夾 {folder_path} 不存在")

def make_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)