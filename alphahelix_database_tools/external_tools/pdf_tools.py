import fitz  # fitz（PyMuPDF）擷取PDF文字 &圖片
import re, os, tempfile, requests, logging
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import tiktoken

# import nltk
# from nltk.tokenize import sent_tokenize, word_tokenize
# import statistics
# 確保已經下載必要的nltk資源
# nltk.download('punkt')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

import re

def clean_gibberish_text(raw_text):
    """
    Cleans gibberish text by:
    - Removing standalone numbers
    - Removing standalone single characters (except 'a' and 'I')
    - Removing isolated 1-2 character English words
    - Removing non-word characters except ':' and '：'
    - Removing excessive or fragmented punctuation (e.g., '...', ', , ,')
    - Normalizing whitespace
    
    Args:
        raw_text (str): The text to clean.

    Returns:
        str: The cleaned text.
    """
    # Remove standalone numbers
    cleaned_text = re.sub(r'\b\d+\b', '', raw_text)
    
    # Remove standalone single characters (excluding 'a' and 'I' for grammar)
    cleaned_text = re.sub(r'\b(?!a\b|I\b)[a-zA-Z]\b', '', cleaned_text)
    
    # Remove isolated 1-2 character English words
    cleaned_text = re.sub(r'\b[a-zA-Z]{1,2}\b', '', cleaned_text)
    
    # Remove non-word characters except ':' and '：'
    cleaned_text = re.sub(r'[^\w\s:：.,?!]', '', cleaned_text)
    
    # Remove excessive or fragmented punctuation (e.g., '. . . .', ', , ,')
    cleaned_text = re.sub(r'([.,?!])(?:\s*\1\s*)+', r'\1', cleaned_text)
    
    # Normalize whitespace (remove extra spaces, tabs, newlines)
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

# def get_pdf_page_length(file_path):
#     PDF = fitz.open(file_path)
#     return len(PDF)

def _extract_raw_text_from_pdf(file_path):
    raw_text = ""
    PDF = fitz.open(file_path)
    for page_num in range(len(PDF)):
        page = PDF.load_page(page_num)  # 加載頁面
        _text = page.get_text()  # 提取頁面上的所有文字
        raw_text += (_text)
    return raw_text

def truncate_text_to_token_limit(text, token_limit=8000, encoding_name="cl100k_base"):
    """
    Truncate the text to ensure it does not exceed the specified token limit.
    
    Args:
        text (str): The input text to be truncated.
        token_limit (int): The maximum number of tokens allowed. Default is 8000.
        encoding_name (str): The name of the encoding to use with tiktoken.
        
    Returns:
        str: The truncated text, ensuring it is within the token limit.
    """
    # Get the specified encoding
    enc = tiktoken.get_encoding(encoding_name)
    
    # Encode the text into tokens
    tokens = enc.encode(text)
    
    # Truncate tokens if they exceed the limit
    if len(tokens) > token_limit:
        tokens = tokens[:token_limit]
    
    # Decode the truncated tokens back into text
    truncated_text = enc.decode(tokens)
    
    return truncated_text

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
