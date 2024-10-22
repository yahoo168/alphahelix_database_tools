import os, tempfile, requests
import requests
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from PIL import Image, ImageFile #type: ignore

from alphahelix_database_tools.utils.folder_ops import delete_folder_files
from alphahelix_database_tools.external_tools.pdf_tools import clean_gibberish_text, count_text_length, _extract_images_from_pdf

# 創建 OCR reader（只初始化一次，默認使用 CPU）
import easyocr #type: ignore

# 定義全域變數
_reader = None
# 避免每次用戶載入模組時都重新初始化 easyocr.Reader 而導致長時間的載入
def get_ocr_reader(language_list=['ch_tra', 'en']):
    global _reader
    if _reader is None:
        _reader = easyocr.Reader(language_list)
    return _reader

# 忽略警告
import warnings
warnings.filterwarnings("ignore")

# 忽略截斷的圖片
ImageFile.LOAD_TRUNCATED_IMAGES = True

# 識別圖片中的文字
def extract_text_from_image(image_path):
    try:
        # 將圖片轉為灰階，並轉為 numpy array
        reader = get_ocr_reader()
        image = Image.open(image_path).convert('L')
        result = reader.readtext(np.array(image))
        # 將結果轉為單一字串
        text_content = ' '.join([text for (_, text, _) in result])
        return text_content
    except Exception as e:
        print(f"處理圖片 {image_path} 時發生錯誤: {e}")
        return ""

# 過濾文字量低於30字的圖片
def filter_images_by_text_length(input_folder, output_folder, min_text_length=15, max_text_length=150):
    # 處理每張圖片
    def process_image(img_file_name):
        try:
            img_file_path = os.path.join(input_folder, img_file_name)
            img_text = extract_text_from_image(img_file_path)
            # 去除辨識後的亂碼
            img_text = clean_gibberish_text(img_text)
            # 計算文字數量
            img_text_length = count_text_length(img_text)

            print(f"圖片 {img_file_name} 的文字長度: {img_text_length}")
            if max_text_length >= img_text_length >= min_text_length:
                output_path = os.path.join(output_folder, img_file_name)
                Image.open(img_file_path).save(output_path)
                print(f"保存圖片: {img_file_name}")
                print(img_text_length)
                print(img_text[:300])
                print("\n")
            else:
                print(f"過濾圖片: {img_file_name}")
            print("\n")
        except Exception as e:
          print(f"處理圖片 {img_file_name} 時發生錯誤: {e}")
            
    # 確保輸出文件夾存在
    os.makedirs(output_folder, exist_ok=True)
    # 列出所有圖片文件
    file_name_list = os.listdir(input_folder)
    # 使用 ThreadPoolExecutor 並行處理圖片
    delete_folder_files(output_folder)
    with ThreadPoolExecutor() as executor:
        executor.map(process_image, file_name_list)
    # 計算總耗時
    print(f"處理完成，過濾後的圖片保存在: {output_folder}")
    
def get_pdf_filtered_images(url, filtered_image_folder_path):
    # 確保目標資料夾存在
    os.makedirs(filtered_image_folder_path, exist_ok=True)
    # 使用 tempfile.TemporaryDirectory() 來管理臨時資料夾
    with tempfile.TemporaryDirectory() as temp_folder_path:
        # 透過 URL 下載 PDF 文件
        response = requests.get(url)
        if response.status_code == 200:
            # 將下載的 PDF 文件保存到臨時資料夾中
            temp_file_path = os.path.join(temp_folder_path, "temp_pdf_file.pdf")
            with open(temp_file_path, 'wb') as file:
                file.write(response.content)
            print(f"PDF 文件已下載並保存至：{temp_file_path}")

            # 使用另一個臨時資料夾來保存提取的圖片
            with tempfile.TemporaryDirectory() as images_folder_path:
                # 清空資料夾內容（如果需要）
                delete_folder_files(images_folder_path)
                
                # 從 PDF 中提取圖片到臨時圖片資料夾
                _extract_images_from_pdf(temp_file_path, images_folder_path)
                print(f"提取圖片共：{len(os.listdir(images_folder_path))}")

                # 過濾圖片，僅保留文字量在範圍內的圖片，並保存到指定的資料夾
                filter_images_by_text_length(images_folder_path, filtered_image_folder_path, min_text_length=15, max_text_length=200)
                print(f"篩選後的圖片共：{len(os.listdir(filtered_image_folder_path))}")

                # 取得篩選後的圖片路徑列表
                filtered_image_path_list = [
                    os.path.join(filtered_image_folder_path, f)
                    for f in os.listdir(filtered_image_folder_path)
                ]
                print(f"篩選後的圖片路徑列表：{filtered_image_path_list}")

        else:
            print(f"無法下載 PDF 文件，狀態碼：{response.status_code}")

    # 返回篩選後的圖片路徑列表
    return filtered_image_path_list
