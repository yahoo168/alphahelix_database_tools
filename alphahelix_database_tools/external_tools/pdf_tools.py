import fitz  # fitz（PyMuPDF）擷取PDF文字 &圖片
import re, os, tempfile, requests, logging
import cv2  #type: ignore #opencv-python 
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from PIL import Image, ImageFile #type: ignore

# import nltk
# from nltk.tokenize import sent_tokenize, word_tokenize
# import statistics
# 確保已經下載必要的nltk資源
# nltk.download('punkt')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_gibberish_text(text):
    # 定義正則表達式來匹配亂碼或無意義的符號組合
    # [^\w\s]{1}：匹配單個非單詞字符或空白符號（如 %, ;, -）
    # \b\d+\b：匹配孤立的數字
    # \b[a-zA-Z]{1,2}\b：匹配孤立的1-2位英文字母
    pattern = r"[^\w\s]{1}|\b\d+\b|\b[a-zA-Z]{1,2}\b"

    # 使用正則表達式替換匹配到的部分
    cleaned_text = re.sub(pattern, '', text)
    # 去除多餘的空白
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
    
    return cleaned_text

def count_text_length(text):
    # 匹配所有中文字符
    chinese_chars = re.findall(r'[\u4e00-\u9fff]', text)
    # 匹配所有英文單詞
    english_words = re.findall(r'\b[a-zA-Z]+\b', text)
    # 計算中文字符數量（每個字算 1）
    chinese_count = len(chinese_chars)
    # 計算英文單詞數量（每個單詞算 1）
    english_count = len(english_words)
    
    # 總數量
    total_count = chinese_count + english_count
    return total_count
    
# 待改：可選擇是否清除零碎段落
def get_pdf_text_from_url(url):
    # 使用临时文件夹，每个线程都有一个独立的临时文件夹
    with tempfile.TemporaryDirectory() as temp_folder_path:
        # 透过url取得报告PDF文件
        response = requests.get(url)
        # 将下载的 PDF 文件保存到本地
        if response.status_code == 200:
            temp_file_path = os.path.join(temp_folder_path, "temp_pdf_file.pdf")
            with open(temp_file_path, 'wb') as file:
                file.write(response.content)
            
            # 從PDF中提取出段落文字
            pdf_text = _extract_raw_text_from_pdf(temp_file_path)
            return pdf_text
        
        else:
            logging.warning(f"[SERVER][PDF][Error {response.status_code}]")
            return []

def _extract_raw_text_from_pdf(file_path):
    raw_text = ""
    PDF = fitz.open(file_path)
    for page_num in range(len(PDF)):
        page = PDF.load_page(page_num)  # 加載頁面
        _text = page.get_text()  # 提取頁面上的所有文字
        raw_text += (_text)
    return raw_text
    
def _extract_images_from_pdf(pdf_file_path, output_folder_path, del_image_folder_path=None, min_size=100 * 1024, max_size=1 * 1024 * 1024, dpi=150, 
                            segment_threshold=300, binary_threshold=100, kernel_size=10, dilation_iterations=4):
    # 逐頁提取頁面中的所有圖像
    def process_page(page_num):
        with fitz.open(pdf_file_path) as pdf_document:
            page = pdf_document.load_page(page_num)
            image_list = page.get_images(full=True)
            
            # 1. 處理一般的嵌入式圖像
            for img_index, img in enumerate(image_list):
                xref = img[0]
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
                        #     cv2.imwrite(segmented_image_path, segmented_image)
                        
                    else:
                        if del_image_folder_path is None:
                            continue
                        segmented_image_path = os.path.join(del_image_folder_path, segment_image_file_name)
                        segmented_image = image[y:y+h, x:x+w]
                        #print(segment_image_file_name, w, h)
                        #print("No")
                    
                    #print()
                    cv2.imwrite(segmented_image_path, segmented_image)
                    
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

def delete_disclosure_section(text, window_size=20, keyword_density=0.2):
    keywords = [
        "disclosures", "important disclosure", "analyst certification",
        "disclosure appendix", "refer to page", "research certification",
        "reg ac certification", "analyst disclosures",
        "Monetary Authority of Singapore", "Australian financial services license",
        "Securities and Exchange Board of India (SEBI)", "Compliance Officer",
        "Grievance Officer", "Regulatory Disclosures", "Conflict Management Policy",
        "Investment Banking Services", "Non-Investment Banking Services",
        "Regulatory Requirements"
    ]
    # 將所有關鍵字組合成一個正則表達式模式
    pattern = re.compile(r"\b(" + "|".join(re.escape(keyword) for keyword in keywords) + r")\b", re.IGNORECASE)

    # 滑動窗口逐步檢查每一段的關鍵字密度
    for i in range(len(text) - window_size + 1):
        window = text[i:i + window_size]
        # 計算該窗口中出現的關鍵字數量
        keyword_count = sum(1 for line in window if pattern.search(line))
        
        # 如果關鍵字密度達到標準，則認為這是風險聲明段落的起點
        if keyword_count / window_size >= keyword_density:
            # 裁剪文本至該段落之前
            return text[:i]
    
    # 若未找到符合條件的段落，則返回全文
    return text

# # 計算被刪除的字數 & 節省的成本
# def show_filtered_paragraph_part(raw_text, paragraph_list):
#     print("original word num: ", len(raw_text))
#     print("remain word num: ", len("\n".join(paragraph_list)))
#     print("retention ratio: ", round(100 * len("\n".join(paragraph_list)) / len(raw_text),2))
#     print("expense: ", cal_GPT_API_expense(text="\n".join(paragraph_list), model="gpt-4o"))
#     print("\n")

# def _clean_raw_text(text):
#     # 移除多餘的空格和換行符
#     text = re.sub(r'\s+', ' ', text)
#     # 移除連結
#     text = re.sub(r'https?://\S+|www\.\S+|\b\S+\.\S+\b', ' ', text)
#     # 使用正則表達式去除頁眉和頁腳
#     text = re.sub(r'(\n86 2 [0-9-]+)|(\n[A-Z][a-z]+[ \t]+[A-Z][a-z]+)', '', text)
#     return text.strip()

# def _extract_paragraph_list(text, show_deleted_part=False):
    # 分割段落
    #paragraph_list = nltk.tokenize.sent_tokenize(text)
    # # 計算每段落字數，以設定段落字數的上下限
    # paragraph_len_list = sorted([len(paragraph) for paragraph in paragraph_list])
    # # 計算平均數
    # paragraph_len_mean = statistics.mean(paragraph_len_list)
    # # 計算標準差
    # paragraph_len_std = statistics.stdev(paragraph_len_list)
    # word_num_upper_limit = paragraph_len_mean + 2.5 * paragraph_len_std
    # word_num_lower_limit = 10
    
    # # 保留較長的段落，過濾掉短段落
    # paragraph_list = [para for para in paragraph_list if (len(para) > word_num_lower_limit) and (len(para) < word_num_upper_limit)]
    # # 顯示被刪除的段落
    # if show_deleted_part:
    #     print(word_num_lower_limit, word_num_upper_limit)
    #     count = 0
    #     for para in paragraph_list:
    #         if (len(para) < word_num_lower_limit) or (len(para) > word_num_upper_limit):
    #             count += 1
    #             print(para, len(para), "\n\n\n")
    #     print(count)
    return text
    #return paragraph_list

# def _get_cleaned_paragraph_list_from_pdf(file_path):
#     raw_text = _extract_raw_text_from_pdf(file_path)
#     cleaned_text = _clean_raw_text(raw_text)
#     paragraph_list = _extract_paragraph_list(cleaned_text)
#     return paragraph_list
