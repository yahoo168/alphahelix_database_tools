import os, tempfile, requests

import requests
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import cv2  #opencv-python #type: ignore
import fitz  # fitz（PyMuPDF）擷取PDF文字 &圖片
from PIL import Image, ImageFile #type: ignore

from alphahelix_database_tools.utils.folder_ops import delete_folder_files
from alphahelix_database_tools.external_tools.pdf_tools import clean_gibberish_text, count_text_length

# 創建 OCR reader（只初始化一次，默認使用 CPU）
import easyocr #type: ignore

# 忽略警告
import warnings
warnings.filterwarnings("ignore")

# 忽略截斷的圖片
ImageFile.LOAD_TRUNCATED_IMAGES = True

# 定義全域變數
_reader = None
# 避免每次用戶載入模組時都重新初始化 easyocr.Reader 而導致長時間的載入
def get_ocr_reader(language_list=['ch_tra', 'en']):
    global _reader
    if _reader is None:
        _reader = easyocr.Reader(language_list)
    return _reader

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
def filter_images_by_text(input_folder, output_folder, min_text_length=15, max_text_length=150, keyword_list=[]):
    # 處理每張圖片
    def process_image(img_file_name):
        try:
            img_file_path = os.path.join(input_folder, img_file_name)
            img_text = extract_text_from_image(img_file_path)
            # 去除辨識後的亂碼
            img_text = clean_gibberish_text(img_text)
            
            # 檢查文字是否包含任何關鍵字
            contains_keyword = any(keyword in img_text for keyword in keyword_list)
            # 計算文字數量
            img_text_length = count_text_length(img_text)
            print(f"圖片 {img_file_name} 的文字長度: {img_text_length}, 包含關鍵字: {contains_keyword}")
            print(img_text[:150])
            if contains_keyword and (max_text_length >= img_text_length >= min_text_length):
                output_path = os.path.join(output_folder, img_file_name)
                Image.open(img_file_path).save(output_path)
                print(f"保存圖片: {img_file_name}")
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
    with ThreadPoolExecutor() as executor:
        executor.map(process_image, file_name_list)
    # 計算總耗時
    print(f"處理完成，過濾後的圖片保存在: {output_folder}")

def _extract_images_from_pdf(pdf_file_path, output_folder_path, del_image_folder_path=None, min_size=100 * 1024, max_size=1 * 1024 * 1024, dpi=150, 
                            segment_threshold=300, binary_threshold=100, kernel_size=10, dilation_iterations=4):
    processed_xrefs = set()
    # 逐頁提取頁面中的所有圖像
    def process_page(page_num):
        with fitz.open(pdf_file_path) as pdf_document:
            page = pdf_document.load_page(page_num)
            image_list = page.get_images(full=True)
            
            # 1. 處理一般的嵌入式圖像
            for img_index, img in enumerate(image_list):
                xref = img[0]
                # 如果這個 xref 已經處理過，就跳過(避免圖片重複提取)
                if xref in processed_xrefs:
                    continue
                # 標記 xref 為已處理
                processed_xrefs.add(xref)
                
                base_image = pdf_document.extract_image(xref)
                image_bytes = base_image["image"]
                image_ext = base_image["ext"]
                image_size = len(image_bytes)
                
                # 檢查圖像大小，是否在限定的檔案大小範圍內
                if min_size <= image_size <= max_size:
                    # 保存圖像
                    image_filename = f"page_{page_num + 1}_img_{img_index + 1}.{image_ext}"
                    image_filepath = os.path.join(output_folder_path, image_filename)
                    with open(image_filepath, "wb") as image_file:
                        image_file.write(image_bytes)

            # 2. 渲染頁面為圖像，以提取矢量圖和文本組成的圖表（無法直接提取）
            pix = page.get_pixmap(dpi=dpi)
            rendered_image_path = os.path.join(output_folder_path, f"page_{page_num + 1}_rendered.png")
            pix.save(rendered_image_path)
            
            # 3. 使用OpenCV對渲染的圖像進行分割
            image = cv2.imread(rendered_image_path)
            if image is not None:
                # 將圖像轉為灰度圖
                gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
                # 將圖像二值化，調整閾值
                _, binary = cv2.threshold(gray, binary_threshold, 255, cv2.THRESH_BINARY_INV)
                # 使用膨脹操作合併相近的區域
                kernel = np.ones((kernel_size, kernel_size), np.uint8)
                dilated = cv2.dilate(binary, kernel, iterations=dilation_iterations)
                
                # 找到輪廓
                contours, _ = cv2.findContours(dilated, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

                # 根據輪廓進行切割
                for idx, contour in enumerate(contours):
                    segment_image_file_name = f"page_{page_num + 1}_segment_{idx + 1}.png"
                    x, y, w, h = cv2.boundingRect(contour)
                    
                    # 濾除過小的區域
                    if (w * h >= segment_threshold**2) and (w >= segment_threshold/2) and (h > segment_threshold/2):
                        segmented_image = image[y:y+h, x:x+w]
                        segmented_image_path = os.path.join(output_folder_path, segment_image_file_name)
                        #print(segment_image_file_name, w, h)
                        #print("Yes")
                        # image_size = os.path.getsize(segmented_image_path) if os.path.exists(segmented_image_path) else 0
                        # print(image_size)
                        # if min_size <= image_size <= max_size:  # 檔案大小介於100KB和1MB之間
                        cv2.imwrite(segmented_image_path, segmented_image)
                        
                    else:
                        if del_image_folder_path is None:
                            continue
                        #segmented_image_path = os.path.join(del_image_folder_path, segment_image_file_name)
                        #segmented_image = image[y:y+h, x:x+w]
                        #print(segment_image_file_name, w, h)
                        #print("No")
                    
                    #print()
                    #cv2.imwrite(segmented_image_path, segmented_image)
                    
                    image_size = os.path.getsize(segmented_image_path)
                    if not (min_size <= image_size <= max_size):  # 檢查檔案大小是否在範圍內
                        os.remove(segmented_image_path)  # 刪除不符合大小的檔案
                            
                # 刪除原始渲染的圖像
                os.remove(rendered_image_path)

    # 確保輸出文件夾存在
    os.makedirs(output_folder_path, exist_ok=True)
    # 確保del_image_folder_path存在
    if del_image_folder_path is not None:
      os.makedirs(del_image_folder_path, exist_ok=True)

    # 使用線程池來並發處理各頁
    with ThreadPoolExecutor() as executor:
        executor.map(process_page, range(fitz.open(pdf_file_path).page_count))

    print(f"提取完成，圖片保存在文件夾: {output_folder_path}")

def get_pdf_filtered_images(url, filtered_image_folder_path, min_text_length=15, max_text_length=200, keyword_list=["Exhibit", "圖"]):
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
                # 從 PDF 中提取圖片到臨時圖片資料夾
                _extract_images_from_pdf(temp_file_path, images_folder_path)
                print(f"提取圖片共：{len(os.listdir(images_folder_path))}")
                
                # 過濾圖片，僅保留文字量在範圍內的圖片，並保存到指定的資料夾
                filter_images_by_text(images_folder_path, filtered_image_folder_path, min_text_length=min_text_length, max_text_length=max_text_length, keyword_list=keyword_list)
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
