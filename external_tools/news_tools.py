from datetime import datetime, timezone, timedelta
import os, re, json, time, base64
import logging, time
import pandas as pd
import requests

import google.auth
import google.auth.transport.requests
import google.oauth2.credentials
import google_auth_oauthlib.flow
#import googleapiclient.discovery
from googleapiclient.discovery import build

from utils import str2datetime, str2unix_timestamp

# 用途 : 抓取seakingalpha的個股分析（analysis）
def get_stock_report_from_seekingalpha(API_key, ticker, start_date=None, size=40):
    url = "https://seeking-alpha.p.rapidapi.com/analysis/v2/list"

    headers = {
        "X-RapidAPI-Key": API_key,
        "X-RapidAPI-Host": "seeking-alpha.p.rapidapi.com"
    }
    
    query_string = {"id":ticker,"size":str(size),"number":"1"}
    if start_date:
        start_date_unix = str2unix_timestamp(start_date)
        query_string.update({"since": str(start_date_unix)})
        
    response = requests.get(url, headers=headers, params=query_string)
    raw_data_list = dict(response.json())["data"]
    
    # 處理數據格式
    article_meta_list = list()
    for i in range(len(raw_data_list)):
        data_dict = raw_data_list[i]
        # _timestamp的前十位為日期，格式範例: "2024-01-01"
        date_str = data_dict["attributes"]["publishOn"]
        # datetime.fromisoformat可解析包含時區偏移的ISO 8601格式的日期字符（包含時區資訊）
        date_datetime = datetime.fromisoformat(date_str)
        # 轉換為台北時間（UTC+8）後，移除附帶的時區資訊
        date_datetime = date_datetime.astimezone(timezone(timedelta(hours=8))).replace(tzinfo=None)
        
        article_meta = {"date": date_datetime,
                        "title": data_dict["attributes"]["title"],
                        "url": "https://seekingalpha.com" + data_dict["links"]["self"],
                        "ticker": ticker,
                        "source":"seekingalpha", 
                        "id": data_dict["id"]
                       }
        article_meta_list.append(article_meta)

    return pd.DataFrame(article_meta_list)

"""
用途 : 抓取seakingalpha的新聞（news)
輸入 : 股號(ticker)
輸出 : list，內容為多個dict ，內含: id, date, title, url
"""
def get_stock_news_from_seekingalpha(API_key, ticker, start_date=None, size=40):
    url = "https://seeking-alpha.p.rapidapi.com/news/v2/list-by-symbol"
    
    headers = {
        "X-RapidAPI-Key": API_key,
        "X-RapidAPI-Host": "seeking-alpha.p.rapidapi.com"
    }

    querystring = {"id":ticker, "size":str(size)}
    if start_date:
        # rapid API讀取的日期格式為unix
        start_date_unix = str2unix_timestamp(start_date)
        querystring.update({"since": str(start_date_unix)})

    response = requests.get(url, headers=headers, params=querystring)
    raw_data_list = dict(response.json())["data"]

    # 處理數據格式
    article_meta_list = list()
    for data_dict in raw_data_list:
        # 原始日期格式：'2024-05-24T12:33:59-04:00'
        date_str = data_dict["attributes"]["publishOn"]
        # datetime.fromisoformat可解析包含時區偏移的ISO 8601格式的日期字符（包含時區資訊）
        date_datetime = datetime.fromisoformat(date_str)
        # 轉換為台北時間（UTC+8）後，移除附帶的時區資訊
        date_datetime = date_datetime.astimezone(timezone(timedelta(hours=8))).replace(tzinfo=None)
    
        article_meta = {"date": date_datetime, 
                        "title": data_dict["attributes"]["title"], 
                        "url": "https://seekingalpha.com" + data_dict["links"]["self"],
                        "ticker": ticker,
                        "source":"seekingalpha", 
                        "id":data_dict["id"]}

        article_meta_list.append(article_meta)
        
    return pd.DataFrame(article_meta_list)

### news API 測試
def get_stock_news_from_news_API(API_key, ticker):
    url = f"https://newsapi.org/v2/everything?q={ticker}&language=en&zh&apiKey={API_key}"
    res = requests.get(url)
    raw_data_list = res.json()["articles"]

    article_meta_list = list()
    for data_dict in raw_data_list:
        # 日期格式範例：'2024-05-01T11:25:57Z'，原先沒有時區資訊，補上預設時區
        date_str = data_dict["publishedAt"]
        date_datetime = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')
        # 轉換為台北時間（UTC+8）後，移除附帶的時區資訊
        date_datetime = date_datetime.astimezone(timezone(timedelta(hours=8))).replace(tzinfo=None)

        article_meta = {"date": date_datetime,
                        "title": data_dict["title"], 
                        "url": data_dict["url"],
                        "ticker": ticker,
                        "source": data_dict["source"]["name"],
                    }

        article_meta_list.append(article_meta)
    
    df = pd.DataFrame(article_meta_list)
    return df.sort_values(by="date")

"""
用途 : 抓reuters的文章
輸入 : 輸入(keyword)需為公司名，股號無法查詢
輸出 : list，內容為多個dict ，內含: id, date, title, url
"""
def get_articles_from_reuters(API_key, keyword, num=10):
    url = f'https://reuters-business-and-financial-news.p.rapidapi.com/get-articles-by-keyword-name/{keyword}/0/{num}'

    headers = {
        'X-RapidAPI-Key': API_key,
        'X-RapidAPI-Host': "reuters-business-and-financial-news.p.rapidapi.com"
    }
    response = requests.get(url, headers=headers)
    raw_data_list = dict(response.json())['articles']

    article_meta_list = list()
    for raw_data_dict in raw_data_list:
        id = raw_data_dict["articlesId"]
        date = raw_data_dict['publishedAt']['date'][:10]
        title = raw_data_dict["articlesName"]
        url = "https://www.reuters.com" + raw_data_dict['urlSupplier']

        article_meta = {"source":"reuters", "type":"news", "id":id, "date":date, "title":title, "url":url}
        article_meta_list.append(article_meta)

    return pd.DataFrame(article_meta_list).drop_duplicates("id")

class GMAIL_NEWS_SCRAP():
    def __init__(self):
        # 待改：應該外部給定
        self.credentials_file_path = "/Users/yahoo168/Desktop/資料庫_測試功能/gmail_acess/gmail_api_credentials.json"
        self.token_file_path = "/Users/yahoo168/Desktop/資料庫_測試功能/gmail_acess/token.json"        
        self._build_service()


    def _build_service(self):
        # 需要访问的 OAuth 2.0 范围
        SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

        creds = None
        if os.path.exists(self.token_file_path):
            creds = google.oauth2.credentials.Credentials.from_authorized_user_file(self.token_file_path, SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(google.auth.transport.requests.Request())
            else:
                flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(self.credentials_file_path, SCOPES)
                creds = flow.run_local_server(port=0)
            with open(self.token_file_path, 'w') as token:
                token.write(creds.to_json())
        # 建立 Gmail API 服務
        self.service = build('gmail', 'v1', credentials=creds)

    def _get_raw_mail_list_by_label(self, start_date, label="BBG_news"):
        if isinstance(start_date, str):
            start_date = str2datetime(start_date)
            
        # 將指定的日期和時間轉換為Unix時間戳記格式
        query_timestamp = int(time.mktime(start_date.timetuple()))
        # 設定查詢條件，只查詢指定時間之後，label為BBG_news的信件
        query = f"after:{query_timestamp} label:{label}"
        # 初始化變數以儲存所有信件
        raw_mail_list = []
        page_token = None
        # Gmail API在一次請求中最多返回100個結果，需要使用nextPageToken進行分頁以取得所有信件
        while True:
            # 執行查詢請求
            results = self.service.users().messages().list(userId='me', q=query, pageToken=page_token).execute()
            raw_mail = results.get('messages', [])
            # 添加查詢結果到信件列表
            raw_mail_list.extend(raw_mail)

            # 檢查是否有下一頁
            page_token = results.get('nextPageToken')
            if not page_token:
                break

        return raw_mail_list

    def _build_non_US_ticker_re_pattern(self, ticker_list):
        ticker_pattern_list = list()
        for ticker in ticker_list:
            #如：2330 TT中的TT
            stock_code = ticker.split(" ")[0]
            #如：2330 TT中的TT
            identifier = ticker.split(" ")[1]
            # r和 f-string 不能直接结合使用，故使用format
            _ticker_pattern = r'\b{stock_code}\s{identifier}\b'.format(stock_code=stock_code, identifier=identifier)
            ticker_pattern_list.append(_ticker_pattern)
        # 依照patthern_list，建立re pattern
        ticker_re_pattern =  re.compile('|'.join(ticker_pattern_list))
        return ticker_re_pattern

    # 如TSM，在BBG news中的ticker是2330 TT，故須另外指定
    def _parse_message_from_raw_mail(self, raw_mail_list, non_US_ticker_list=["2330 TT"]):
        # 正則表達式（搜索字串中的"Tickers\n"或"代碼\n"後面帶有"US"的字串）
        US_ticker_re_pattern = re.compile(r'\b([A-Z0-9]+)\s+US\b')
        non_US_ticker_pattern = self._build_non_US_ticker_re_pattern(non_US_ticker_list)
        # 使用正则表达式匹配 "Source:" 或 "來源:" 后面的字
        source_re_pattern = re.compile(r'(Source|來源):\s*(.*)')

        # BBG_news ticker與資料庫ticker的轉換參照
        BBG_news_ticker_trans_dict = {
            "2330 TT": "TSM",
            "GOOGL": "GOOG",
        }

        #透過mail id獲得信件完整資訊
        logging.info(f"[GMAIL][parse_raw_mail] 共{len(raw_mail_list)}則")
        message_meta_list = list()
        for index, raw_message in enumerate(raw_mail_list, 1):
            # 顯示當前解析進度
            current_progess_percentage = round(100*index/len(raw_mail_list), 2)
            logging.info(f"[GMAIL][parse_raw_mail] current progress: {str(current_progess_percentage)}%")
            msg = self.service.users().messages().get(userId='me', id=raw_message['id']).execute()
            # 取出信件的tile & date
            title, date = self._get_message_subject_and_date(msg["payload"])
            # 取出信件本文
            msg_body = self._get_message_body(msg["payload"])
            # 轉換日期格式（原格式為'Wed, 22 May 2024 16:06:38 -0000'）
            date_datetime = datetime.strptime(date, "%a, %d %b %Y %H:%M:%S %z")
            # 轉換為台北時間（UTC+8）後，移除附帶的時區資訊
            date_datetime = date_datetime.astimezone(timezone(timedelta(hours=8))).replace(tzinfo=None)
            # 取出內文中的ticker（可能有多個），find_all可返回多個
            US_ticker_list = US_ticker_re_pattern.findall(msg_body)
            non_US_ticker_list = non_US_ticker_pattern.findall(msg_body)
            # 將US ticker和non US ticker合併，並轉換為資料庫格式的ticker(如GOOGL -> GOOG, 2330 TT -> TSM)
            raw_mentioned_ticker_list = US_ticker_list + non_US_ticker_list
            mentioned_ticker_list = [BBG_news_ticker_trans_dict.get(ticker, ticker) for ticker in raw_mentioned_ticker_list]
            # 取出內文中的source（僅有一個），search為返回第一个Match object
            match = source_re_pattern.search(msg_body)
            source_ticker = "unknown"
            if match:
                # Ex: DJ (Dow Jones Institutional News Feed)
                source_fullname = match.group(2)
                # Ex: DJ
                source_ticker = source_fullname.split(" ")[0]
            
            message_meta = {
                "date": date_datetime,
                "title": title,
                "ticker": mentioned_ticker_list,
                # 待改：若未來有其他來源，不能直接設置BBG
                "source": "BBG_" + source_ticker,
                #live:僅有標題，無內文與連結
            }
            message_meta_list.append(message_meta)
        return message_meta_list
    
    # 当信件的 mimeType 是 multipart/alternative 时，信件正文可以包含多个版本（例如纯文本和 HTML）。
    # 需要遍历这些部分并选择合适的内容来读取。通常，我们会选择纯文本部分（text/plain），如果没有纯文本部分则选择 HTML 部分（text/html）。
    def _get_message_body(self, payload):
        parts = payload.get("parts", [])
        for part in parts:
            mimeType = part.get('mimeType')
            if mimeType == 'text/plain':
                body = part.get('body')
                data = body.get('data')
                if data:
                    return base64.urlsafe_b64decode(data).decode('utf-8')
            else:
                return self._get_message_body(part)
        return ""

    def _get_message_subject_and_date(self, payload):
        headers = payload["headers"]
        for header in headers:
            if header['name'] == 'Subject':
                subject = header['value']
            elif header['name'] == 'Date':
                date = header['value']
        return subject, date