import fitz  # PyMuPDF
import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
import re, os
import statistics

def _extract_raw_text_from_pdf(file_path):
    raw_text = ""
    PDF = fitz.open(file_path)
    for page_num in range(len(PDF)):
        page = PDF.load_page(page_num)  # 加載頁面
        _text = page.get_text()  # 提取頁面上的所有文字
        #_text = _text.replace('\n', ' ').strip()
        raw_text += (_text)
    return raw_text
    
def _clean_raw_text(text):
    # 移除多餘的空格和換行符
    text = re.sub(r'\s+', ' ', text)
    # 移除連結
    text = re.sub(r'https?://\S+|www\.\S+|\b\S+\.\S+\b', ' ', text)
    # 使用正則表達式去除頁眉和頁腳
    text = re.sub(r'(\n86 2 [0-9-]+)|(\n[A-Z][a-z]+[ \t]+[A-Z][a-z]+)', '', text)
    return text.strip()

def _extract_paragraph_list(text, show_deleted_part=False):
    # 確保已經下載必要的nltk資源
    nltk.download('punkt')
    # 分割段落
    paragraph_list = nltk.tokenize.sent_tokenize(text)
    # 計算每段落字數，以設定段落字數的上下限
    paragraph_len_list = sorted([len(paragraph) for paragraph in paragraph_list])
    # 計算平均數
    paragraph_len_mean = statistics.mean(paragraph_len_list)
    # 計算標準差
    paragraph_len_std = statistics.stdev(paragraph_len_list)
    word_num_upper_limit = paragraph_len_mean + 2.5 * paragraph_len_std
    word_num_lower_limit = 10
    
    # 保留較長的段落，過濾掉短段落
    paragraph_list = [para for para in paragraph_list if (len(para) > word_num_lower_limit) and (len(para) < word_num_upper_limit)]
    # 顯示被刪除的段落
    if show_deleted_part:
        print(word_num_lower_limit, word_num_upper_limit)
        count = 0
        for para in paragraph_list:
            if (len(para) < word_num_lower_limit) or (len(para) > word_num_upper_limit):
                count += 1
                print(para, len(para), "\n\n\n")
        print(count)
    
    return paragraph_list

def get_cleaned_paragraph_list_from_pdf(file_path):
    raw_text = _extract_raw_text_from_pdf(file_path)
    cleaned_text = _clean_raw_text(raw_text)
    paragraph_list = _extract_paragraph_list(cleaned_text)
    return paragraph_list


def extract_images_from_pdf(pdf_path, output_folder):
    # 打开PDF文件
    pdf_document = fitz.open(pdf_path)
    
    # 确保输出文件夹存在
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # 遍历每一页
    for page_num in range(len(pdf_document)):
        page = pdf_document.load_page(page_num)
        image_list = page.get_images(full=True)
        
        # 遍历页面中的每个图像
        for img_index, img in enumerate(image_list):
            xref = img[0]
            base_image = pdf_document.extract_image(xref)
            image_bytes = base_image["image"]
            image_ext = base_image["ext"]
            
            # 设置图像文件名并保存
            image_filename = f"page_{page_num + 1}_img_{img_index + 1}.{image_ext}"
            image_filepath = os.path.join(output_folder, image_filename)
            
            with open(image_filepath, "wb") as image_file:
                image_file.write(image_bytes)
    
    print(f"提取完成，图片保存在文件夹: {output_folder}")

# 計算被刪除的字數 & 節省的成本
def show_filtered_paragraph_part(raw_text, paragraph_list):
    print("original word num: ", len(raw_text))
    print("remain word num: ", len("\n".join(paragraph_list)))
    print("retention ratio: ", round(100 * len("\n".join(paragraph_list)) / len(raw_text),2))
    print("expense: ", cal_GPT_API_expense(text="\n".join(paragraph_list), model="gpt-4o"))
    print("\n")
    
if __name__ == "main":
    from PIL import Image
    # 打开JPEG图像
    image_path = '/Users/yahoo168/Desktop/pdf_output/page_31_img_3.jpeg'
    image = Image.open(image_path)

    # 获取图像的尺寸（宽度和高度）
    width, height = image.size
    print(width)
    print(height)
    # 计算像素数量
    pixel_count = width * height

    print(f'图像的像素数量: {pixel_count}')