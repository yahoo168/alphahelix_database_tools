from collections import defaultdict
from datetime import datetime
import os, json
from pymongo import DESCENDING
from jinja2 import Template #type: ignore

from dotenv import load_dotenv #type: ignore

# 避免採用相對導入，在雲端伺服器容易報錯
# from alphahelix_database_tools.cloud_database.config.notification_template import _all_notification_template_dict

from alphahelix_database_tools.external_tools.news_tools import *
from alphahelix_database_tools.external_tools.openai_tools import call_OpenAI_API
from alphahelix_database_tools.external_tools.pdf_tools import get_paragraph_list_from_pdf
from alphahelix_database_tools.external_tools.google_tools import *
from alphahelix_database_tools.utils.format_utils import standardize_dict, standardize_key, remove_duplicates_by_key
from alphahelix_database_tools.utils.notification_template import _all_notification_template_dict

from alphahelix_database_tools.cloud_database.CloudPoolListDatabase import CloudPoolListDatabase
from .AbstractCloudDatabase import *

"""
### 都是台幣
- shorts_summary: 0.1/ticker/日
"""

"""
在 MongoDB 中，datetime 对象默认是不带时区信息的（即offset-naive）。
当你将 Python 的 datetime 对象插入到 MongoDB 中时，如果该对象包含时区信息，
MongoDB 会将其转换为UTC时间并存储为一个没有时区偏移的时间戳。在查询时，返回的 datetime 对象也不会包含时区信息。
"""

# 應該放在這？
load_dotenv()

class CloudArticlesDatabase(AbstractCloudDatabase):
    def __init__(self, config_folder_path=None):
        super().__init__(config_folder_path=config_folder_path)  #調用AbstractCloudDatabase 的__init__方法
        self._load_api_keys()
        self.MDB_client = MongoClient(self.cluster_uri_dict["articles"], server_api=ServerApi('1'))
        # 待確認：這樣合適嗎？
        self.pool_list_db = CloudPoolListDatabase()
        
    def _load_api_keys(self):
        # 載入 .env 文件(用於本地測試)，並取用環境變數
        self.polygon_API_key = os.getenv('polygon_API_key')
        self.rapid_API_key = os.getenv('rapid_API_key')
        self.news_API_key = os.getenv('news_API_key')
        self.OpenAI_API_key = os.getenv('OpenAI_API_key')        

    def update_stock_news_summary(self):
        self.save_shorts()
        ticker_list = self.pool_list_db.get_tracking_ticker_list()
        for ticker in ticker_list:
            self.save_shorts_summary(ticker)
            self.save_stock_news(ticker, source="News_API")
            self.save_stock_news(ticker, source="polygon_io")
            self.save_stock_news_summary(ticker)
        
        # 取得所有用戶id列表
        user_id_list = self.get_active_user_id_list()
        # 測試用：只寄送給特定用戶
        user_id_list = [ObjectId("66601790f20eb424a340acd3")]
        
        # 用於render通知模板的變數字典
        notification_variables_dict = {
                "date": datetime2str(datetime.now() - timedelta(days=1)),
                "page_url": f"/main/ticker_news_overviews",
            }
        
        # 寄送用戶通知
        self.create_notification(user_id_list=user_id_list, 
                                        priority=2,
                                        notification_type="update", # "system"、"update"、"todo" 或 "alert"。
                                        notification_sub_type="stock_news_summary_update",
                                        variables_dict=notification_variables_dict)
    
    # Source: News_API, polygon_io
    def save_stock_news(self, ticker, source):
        mongdoDB_collection = self.MDB_client["raw_content"]["raw_stock_news"]  
        last_news_meta = mongdoDB_collection.find_one({"tickers": ticker, "data_source": source}, sort=[("updated_timestamp", -1)])
        if last_news_meta is not None:
            start_timestamp = last_news_meta["updated_timestamp"]
        else:
            start_timestamp = None
        
        # 根據不同的數據源，調用不同的函數
        if source == "News_API":
            news_meta_list = get_stock_news_from_news_API(self.news_API_key, ticker, start_timestamp)
        elif source == "polygon_io":
            news_meta_list = save_stock_news_from_Polygon(self.polygon_API_key, ticker, start_timestamp)
        else:
            logging.error(f"[SERVER][NEWS][{source}]: No such data source.")
            return
        
        logging.info(f"[SERVER][NEWS][{source}][{ticker}] from: {datetime2str(start_timestamp)}, Total: {len(news_meta_list)} news.")
        if len(news_meta_list) > 0:
            mongdoDB_collection.insert_many(news_meta_list)

    def save_shorts(self, start_date=None, source="BBG_news"):
        if start_date == None:
            start_date = self.get_latest_data_date(item="raw_shorts")

        GMAIL_NEWS = GMAIL_NEWS_SCRAP()
        raw_mail_list = GMAIL_NEWS._get_raw_mail_list_by_label(start_date=start_date, label="BBG_news")
        data_list = GMAIL_NEWS._parse_message_from_raw_mail(raw_mail_list)

        if len(data_list) > 0:
            self.save_data_to_MDB(item="raw_shorts", data_list=data_list, upsert=True, key="title")
    
    # 1. 取出最新的新聞資料日期，小於該日期即不取，大於等於的方可（仍會有部分重複）取出後再作整理
    # 2. 同個pool list的重疊新聞，若有重複，透過tickers_tag重合
    def _group_news_df_by_ticker(self, df):
        grouped_df = df.groupby("url").agg({
            "source": 'first',
            "id": "first", 
            "date": "first",
            "title": "first",
            "ticker": list
        })
        # 因group後url會變成index，須reset_index
        return grouped_df.reset_index()
        
    def save_shorts_summary(self, ticker,  words_per_shorts=8):
        def _call_shorts_summary_LLM(shorts_content_list, ticker, word_number):
            prompt = (
                        f"以下是一些可能與「股票代號為{ticker}的公司」有關的新聞消息，"
                        f"請挑選其中與公司相關性較高的消息，組合成一篇中文新聞，切勿包含任何不在以下消息中的內容"
                        f"並在各個段落用標題敘述段落重點，不含標題的總字數約{word_number}字，標題使用markdown語法中的###表示\n"
                    )
            
            prompt += "\n".join(shorts_content_list)
            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format="text")
        
        lastest_doc_meta = self.MDB_client["preprocessed_content"]["shorts_summary"].find_one({"ticker": ticker}, sort=[("data_timestamp", -1)])
        current_timestamp = datetime.now(timezone.utc)
        current_date_timestamp = current_timestamp.replace(hour=0, minute=0, second=0, microsecond=0) #不使用.date()，會導致MDB儲存錯誤
        
        # 如果有上次的摘要時間，則從上次時間開始摘要，否則從當前的一日前開始摘要
        if lastest_doc_meta is not None:
            logging.info(f"[SERVER][NEWS][shorts_summary]{ticker}: from: {lastest_doc_meta['data_timestamp']}")
            latest_data_timestamp = lastest_doc_meta["data_timestamp"].replace(tzinfo=timezone.utc)
            end_timestamp = current_date_timestamp - timedelta(days=1)
            # 自上次摘要時間的下一天開始，生成以一天為間隔的日期列表
            data_timestamp_list = pd.date_range(start=(latest_data_timestamp+timedelta(days=1)), end=end_timestamp, freq='D').to_list()
        else:
            logging.info(f"[SERVER][NEWS][shorts_summary]{ticker}: No lastest data timestamp")
            data_timestamp_list = [current_date_timestamp]
            
        for data_timestamp in data_timestamp_list:
            start_timestamp = data_timestamp - timedelta(days=0.5)
            end_timestamp = data_timestamp + timedelta(days=1)
            shorts_meta_list = list(self.MDB_client["raw_content"]["raw_shorts"].find(
                                                {"ticker": ticker, "date":
                                                {"$gte": start_timestamp, "$lte": end_timestamp}}))
            
            # 取得shorts的標題列表（待改：應該是content）
            shorts_content_list = [shorts_meta["title"] for shorts_meta in shorts_meta_list]
            num_of_raw_shorts = len(shorts_content_list)
            
            if num_of_raw_shorts == 0:
                logging.info(f"[SERVER][NEWS][shorts_summary][{ticker}][no shorts exist in the selected period]")
                shorts_summary_content = ''
            
            else:
                # 設定字數上限，避免因新聞過多導致摘要過長
                summary_word_num = min(num_of_raw_shorts * words_per_shorts, 800)
                logging.info(f"[SERVER][NEWS][shorts_summary][{ticker}][{datetime2str(data_timestamp)}] 共{num_of_raw_shorts}則shorts，預計摘要字數約{summary_word_num}")
                # 每則新聞以8個字摘要（因含無關的新聞，實際約為10字）
                shorts_summary_content = _call_shorts_summary_LLM(shorts_content_list, ticker=ticker, word_number= words_per_shorts*num_of_raw_shorts)
            
            doc_meta = {
                    "data_timestamp": datetime(data_timestamp.year, data_timestamp.month, data_timestamp.day),
                    "updated_timestamp": datetime.now((timezone.utc)),
                    "timestamp_range": {"start": start_timestamp, "end": end_timestamp},
                    "ticker": ticker,
                    "content": shorts_summary_content
                }
            
            self.MDB_client["preprocessed_content"]["shorts_summary"].insert_one(doc_meta)
    
    # 摘要每日個股新聞（所有新聞源）並儲存，（多儲存一天避免時區問題）
    def save_stock_news_summary(self, ticker):
        def _call_stock_news_summary_LLM(ticker, shorts_summary_content, news_meta_list):
            prompt = f"""
                    從以下的「彭博新聞」以及「其他新聞」中，挑選出對股票ticker為{ticker}的公司重要的新資訊，依照相近的主題歸納不同新聞，內容盡量完整，不要包含「股價變動」相關的新聞。 \n
                    請以「彭博新聞」的內容為主，「其他新聞」作為補充。
                    請標註新聞來源，「彭博新聞」的新聞來源直接寫「彭博新聞」，「其他新聞」的新聞來源則標註其來源連結，若有多個連結則換行呈現，若是其來源連結不存在，可保留資訊內容但來源留空。 \n
                    不要包含重複的新聞，並確保內容皆引用自以下文字，而非憑空捏造，若沒有相關且重要的新聞，可返回空字串('') \n
                    
                    格式：返回純text（不要包含markdown語法，例如###），且不要包含系統訊息，並翻譯為繁體中文，範例如下：
                    
                    主題：雲端業務增長
                    
                    內容：微軟（MSFT）儘管其雲業務增長放緩，仍計劃增加 AI 基礎設施的支出，表明巨額投資可能需要比華爾街預期更長的時間才能獲得回報。
                    Azure 雲業務預計在 2025 財年下半年加速增長。儘管過去一年 AI 樂觀情緒推動微軟股價上漲，但 Azure 增速低於預期，引發投資者失望。微軟表示，
                    其AI服務在收入增長中佔據重要部分，目前有超過 60,000 名客戶使用 Azure AI。
                    
                    來源：https://today.line.me/tw/v2/article/Kwjlrn0
                    """
            # 添加當日的BBG新聞快訊總結（若有）
            prompt += f"「彭博新聞」:\n {shorts_summary_content} \n\n"
            prompt += "「其他新聞」:\n"
            # 添加當日的新聞
            for news_meta in news_meta_list:
                prompt += news_meta["title"] + '\n'
                prompt += datetime2str(news_meta["data_timestamp"]) + '\n'
                prompt += news_meta["content"] + '\n'
                prompt += news_meta["url"] + '\n\n'
                
            summary_content = call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format="text")
            return summary_content
        
        lastest_doc_meta = self.MDB_client["preprocessed_content"]["stock_news_summary"].find_one({"ticker": ticker}, sort=[("data_timestamp", -1)])
        
        # 新聞摘要至昨天，以避免當天僅有部分新聞導致摘要不完整
        end_timestamp = datetime.now(timezone.utc) - timedelta(days=1)
        end_date_timestamp = end_timestamp.replace(hour=0, minute=0, second=0, microsecond=0) #不使用.date()，會導致MDB儲存錯誤
        
        # 如果有上次的摘要時間，則從上次時間開始摘要，否則從當前的一日前開始摘要
        if lastest_doc_meta is not None:
            latest_data_timestamp = lastest_doc_meta["data_timestamp"].replace(tzinfo=timezone.utc)
            # 自上次摘要時間的下一天開始，生成以一天為間隔的日期列表
            data_timestamp_list = pd.date_range(start=(latest_data_timestamp+timedelta(days=1)), end=end_date_timestamp, freq='D').to_list()
        
        # 如果沒有上次的摘要時間，則從當前的一日前開始摘要
        else:
            data_timestamp_list = [end_date_timestamp]
        
        # 逐日進行摘要(往前多取一部分，使新聞摘要更完整)
        for data_timestamp in data_timestamp_list:
            logging.info(f"[SERVER][NEWS][SUMMARY][{ticker}][{datetime2str(data_timestamp)}]")
            start_timestamp = data_timestamp - timedelta(days=0.25)
            end_timestamp = data_timestamp + timedelta(days=1)
            # 取得當日的新聞快訊總結（若有）(data_timestamp為整數日期，不含時分秒)
            shorts_meta_list = self.get_shorts_summary(ticker, data_timestamp)
            shorts_meta = shorts_meta_list[0] if shorts_meta_list else {}
            shorts_summary_content = shorts_meta.get("content", '')
            
            news_meta_list = self.get_stock_news(ticker=ticker, 
                                            start_timestamp=start_timestamp,
                                            end_timestamp=end_timestamp)
            
            # 如果沒有新聞，則直接回傳以下內容（不能為空，以避免後續自動化更新卡死在確實沒有新聞的時間區段中）
            if shorts_summary_content == '' and len(news_meta_list) == 0:
                summary_content = ''
                logging.info(f"[SERVER][NEWS][SUMMARY][{ticker}] Skip: No news exist in the selected period")
            
            else:
                summary_content = _call_stock_news_summary_LLM(ticker, shorts_summary_content, news_meta_list)
                
            doc_meta = {
                "ticker": ticker,
                "data_timestamp": data_timestamp,
                "timestamp_range": {"start": start_timestamp, "end": end_timestamp},
                "updated_timestamp": datetime.now(timezone.utc),
                "content": summary_content,
            }
            self.MDB_client["preprocessed_content"]["stock_news_summary"].insert_one(doc_meta)
            
    # 針對特定issue，找出尚未針對此issue進行摘要的document meta list（以ticker搜索），可限定最長回顧天數與最大報告數量
    def _get_unextracted_documents_of_certain_issue(self, issue_id, max_days, max_doc_num):
        # 依照issue_id，取得issue的meta資料
        issue_meta = self.MDB_client["users"]["following_issues"].find_one({"_id": issue_id})
        # 取得與issue相關的tickers
        ticker_list = issue_meta["tickers"]
        # 若有多個ticker，限制最大文件數量為100
        max_doc_num = min(len(ticker_list) * max_doc_num, 100)
        # 針對tickers搜索相關的文件（目前僅有stock report）
        document_meta_list = list(self.MDB_client["preprocessed_content"]["stock_report"]
                                    .find({"ticker": {"$in": ticker_list}, 
                                            "data_timestamp": {"$gte": datetime.now() - timedelta(days=max_days)}
                                        },
                                        {"ticker":1, "title":1, "url":1, "issue_summary.issue_id": 1, "data_timestamp": 1})  # projection
                                    .sort("data_timestamp", DESCENDING)
                                    .limit(max_doc_num))

        # for-loop目前已經摘要過的issue，過濾掉已經有此issue的document
        result_document_meta_list = [
            document_meta for document_meta in document_meta_list
            if not any(issue["issue_id"] == issue_id for issue in document_meta.get("issue_summary", []))
        ]

        return result_document_meta_list
    
    # 針對特定issue，在一定的期間內，找出尚未針對此issue進行摘要的document並進行摘要（以ticker搜索document）
    def extract_documents_of_certain_issue(self, issue_id, max_days=90, max_doc_num=30):
        # 依照issue_id，取得issue的meta資料
        issue_meta = self.MDB_client["users"]["following_issues"].find_one({"_id": issue_id})
        # 取得issue名稱（即issue）
        issue = issue_meta["issue"]
        # 找出尚未針對此issue進行摘要的document
        document_meta_list = self._get_unextracted_documents_of_certain_issue(issue_id=issue_id, max_days=max_days, max_doc_num=max_doc_num)
        logging.info(f"[SERVER][investment_issue][check][{issue}] Found {len(document_meta_list)} unextracted documents")
        for index, document_meta in enumerate(document_meta_list):
            logging.info(f"[SERVER][LLM] Summarizing({index}). Title: {document_meta['title']}")
            report_id, ticker, pdf_file_url = document_meta["_id"], document_meta["ticker"], document_meta["url"]
            
            # 針對單篇document與issue進行摘要，取得issue content（關閉全文摘要，僅針對issue摘要）
            result_data_dict = self._extract_info_from_document(ticker, pdf_file_url, issue_list=[issue], 
                                                                extract_summary=False, extract_issue_summary=True)
            
            # issue_content_dict為一個dict，key為issue，value為issue_content，但本處只有一個issue，故直接取第一個value
            issue_content_dict = result_data_dict["issue_content_dict"]
            issue_content = list(issue_content_dict.values())[0] if issue_content_dict else ''
            
            # 將issue content存入MongoDB（在原document的issue_summary中添加issue_content）
            issue_meta = {
                "issue_id": issue_id,
                "issue": issue,
                "issue_content": issue_content
            }
            self.MDB_client["preprocessed_content"]["stock_report"].update_one({"_id": report_id}, {"$push": {"issue_summary": issue_meta}})
            
    # 使用LLM抽取報告摘要（全文摘要 & 追蹤問題摘要）
    def _extract_info_from_document(self, ticker, pdf_file_url, issue_list=[], extract_summary=True, extract_issue_summary=True):
        def _creat_stock_report_summary(paragraph_list, ticker, output_format="text"):
            prompt = (f"以下是一篇「股票代號為{ticker}的公司」的研究報告，"
                    f"請以markdown的語法來整理這份報告，包括'全文摘要', '看多論點', '看空論點'三個段落，並翻譯為中文，"
                    f"回傳格式：全文僅包括'全文摘要', '看多論點', '看空論點'三個段落，不要有任何其他內容。"
                    f"markdown的段落標題統一使用'###'表示，段落內文不要有任何符號 \n")
            
            prompt += f"研究報告內容:\n{'n'.join(paragraph_list)}\n"
            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format=output_format)

        def _creat_issue_following_summary(paragraph_list, ticker, issue_list, word_number, output_format="json_object"):
            prompt = (f"以下是一篇與「股票代號為{ticker}的公司」有關的研究報告，"
                    f"依據以下不同的'issue'，各自挑選出相關的段落（盡量完整），整合成短文後翻譯為「繁體中文」，各個段落短文的字數約{word_number}字。\n"
                    f"切勿添加本篇研究報告以外的內容，僅依照本篇報告提供的內容整理即可，若沒有相關內容可返回空字串('')。")
            
            prompt += f"'issue':「{', '.join(issue_list)}」\n"
            
            prompt += f"格式請確保返回的 JSON 格式中只有以下key：{', '.join(issue_list)}, value則為對應的issue段落短文）\n"
            prompt += f"研究報告內容:\n{'\n'.join(paragraph_list)}\n"
            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format=output_format)
        
        # 初始化結果字典（避免出現key error）
        result_data_dict = {
            "report_summary": '',
            "issue_content_dict": {},  
        }
        # 從PDF中取得段落
        try:
            paragraph_list = get_paragraph_list_from_pdf(pdf_file_url=pdf_file_url)
        except Exception as e:
            logging.error(f"Error retrieving PDF from {pdf_file_url}: {e}")
            return result_data_dict
        
        if extract_summary:
            # 報告全文摘要 - 調用LLM
            report_summary = _creat_stock_report_summary(paragraph_list, ticker, output_format="text")
            result_data_dict["report_summary"] = report_summary
        
        # 取得特定issue的摘要，並存入issue_content_dict（key: issue, value: issue_content）
        if extract_issue_summary:
            # 報告追蹤問題摘要 - 調用LLM
            # 將issue_list的key進行standardize，降低LLM回傳的key(issue)變化的機率
            standardized_issue_list = [standardize_key(issue) for issue in issue_list]
            issue_content_json_text = _creat_issue_following_summary(paragraph_list, ticker, standardized_issue_list, word_number=500, output_format="json_object")
            # 預防少數情況可能因為LLM回傳的json格式有誤，導致dict解析錯誤而報錯
            try:
                standardized_issue_content_dict, reverse_key_mapping = standardize_dict(json.loads(issue_content_json_text))
            except Exception as e:
                logging.error(f"Error parsing issue content JSON: {e}")
                return result_data_dict
                            
            # 使用standardize_key过滤出原先给定的issue，以原始issue為key，issue_content為value，存入issue_content_dict
            # 处理摘要内容，确保issue_content若非string（有可能為list / dict）也被转换为字符串
            issue_content_dict = {
                issue: str(standardized_issue_content_dict.get(standardize_key(issue), ''))
                for issue in issue_list
            }
            
            # 將issue_content_dictd的standardize_key還原為原始key（issue）
            result_data_dict["issue_content_dict"] = issue_content_dict
            
        return result_data_dict
    
    # 自動找出尚未預處理的stock report並進行處理（全文摘要 & 追蹤問題摘要）
    def process_raw_stock_report(self):
        # 取出尚未處理的stock report meta
        stock_report_meta_list = list(self.MDB_client["raw_content"]["raw_stock_report_non_auto"].find({"is_processed": False}))
        stock_report_num = len(stock_report_meta_list)
        logging.info(f"[SERVER][Data Process][共{stock_report_num}篇non auto stock report待處理]")

        for index, stock_report_meta in enumerate(stock_report_meta_list):
            stock_report_id, ticker, pdf_file_url = stock_report_meta["_id"], stock_report_meta["ticker"], stock_report_meta["url"]
            logging.info(f"[SERVER][Data Process][{ticker}][開始處理第{index + 1}/{stock_report_num}篇non auto stock report]")

            # 取出與該標的相關的active issue（預設最多10個）
            stock_following_issue_meta_list = list(self.MDB_client["users"]["following_issues"].find({"tickers": ticker, "is_active": True}, limit=10))
            # 建立issue與issue_id的對應字典（因後續由LLM進行處理，返回的issue為str，須透過issue_id確認對應的資料庫物件
            issue_id_mapping = {item_meta['issue']: item_meta['_id'] for item_meta in stock_following_issue_meta_list}
            stock_following_issue_list = list(issue_id_mapping.keys())
            # 將文件由LLM進行抽取（全文摘要 / 追蹤問題摘要）
            extracted_info_data_dict = self._extract_info_from_document(
                ticker=ticker, 
                pdf_file_url=pdf_file_url, 
                issue_list=stock_following_issue_list, 
                extract_summary=True, 
                extract_issue_summary=True
            )
            
            issue_summary_list = []
            # 逐一取出LLM返回issue與issue_content，並進行解析後儲存
            for issue, issue_content in extracted_info_data_dict["issue_content_dict"].items():
                # 通過預先建立的issue_id_mapping，取得issue對應的issue_id
                issue_id = issue_id_mapping.get(issue, None)
                # 若issue_id為None，則不進行儲存
                if issue_id is not None:
                    issue_summary_meta = {
                            "issue": issue,
                            "issue_id": issue_id,
                            "issue_content": issue_content
                        }
                    issue_summary_list.append(issue_summary_meta)
                else:
                    logging.warning(f"[SERVER][Data Process][{ticker}][處理丟失][issue: {issue}][issue_id: {issue_id}]")

            # 將原始文件標識為已處理
            self.MDB_client["raw_content"]["raw_stock_report_non_auto"].update_one(
                {"_id": stock_report_id}, {"$set": {"is_processed": True}}
            )
            
            # 更新stock_report_meta的內容
            stock_report_meta.update({
                "summary": extracted_info_data_dict["report_summary"],
                "issue_summary": issue_summary_list,
                "processed_timestamp": datetime.now()
            })
            
            # 保存處理後的stock report meta到MongoDB
            result = self.MDB_client["preprocessed_content"]["stock_report"].insert_one(stock_report_meta)
            
            # 寄送用戶通知，获取插入后的 _id
            report_id = result.inserted_id
            following_user_id_list = self.MDB_client["research_admin"]["ticker_info"].find_one({"ticker": ticker}, {"following_users": 1, "_id": 0})["following_users"]

            # 用於render通知模板的變數字典
            variables_dict = {
                    "ticker": ticker,
                    # _external=True 参数确保生成的是绝对 URL
                    "page_url": f"/main/report_summary_page/{report_id}",
                    "title": stock_report_meta["title"].split('.')[0],
                }

            self.create_notification(user_id_list=following_user_id_list, 
                                     priority=2,
                                     notification_type="update", # "system"、"update"、"todo" 或 "alert"。
                                     notification_sub_type="stock_report_update",
                                     variables_dict=variables_dict)
            
    
    # 根據與特定個股相關的報告，製作看多/看空的論點摘要，可設定最大報告數量
    def save_stock_report_review(self, ticker, review_report_nums=10):
        def _create_raw_stock_report_review(summary_list, ticker, output_format="json_object"):
            prompt = (f"以下是一些「股票代號為{ticker}的公司」近期的多篇研究報告，"
                    f"挑選出其中的bullish_outlook以及bearish_outlook進行整理，2個面向各約300字，不包含對目標價的預測。")
            prompt += f"將回傳值以 JSON 格式提供，其中包含以下2️個key:'bullish_outlook'以及'bearish_outlook'。"
            prompt += f"除了上述的部分外，不要有任何其他內容\n\n"
            prompt += f"研究報告內容:\n{'\n'.join(summary_list)}\n"
            
            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format=output_format)

        def _adjust_stock_report_review_format(summary_text, ticker, output_format="text"):
            prompt = (f"以下是一些「股票代號為{ticker}的公司」近期的研究結論，"
                    f"以條列式的方式，整理其中的論點，每個論點以2~3句話表示內容，論點數量不高於10個。")
            prompt += f"回傳格式：每個論點以換行符('\n')分隔，論點前面不需要數字編號"
            prompt += f"除了上述的部分外，不要有任何其他內容\n\n"
            prompt += f"研究報告內容:\n{'\n'.join(summary_text)}\n"
            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format=output_format)
             
        # 將json格式的看多/看空論點解析成list
        def _parse_outlook_argument(argument_text):
            argument_list = argument_text.split("\n")
            argument_list = [argument for argument in argument_list if argument]
            return argument_list
        
        def _compare_outlook_argument(old_argument_list, new_argument_list):
            old_argument, new_argument = '\n'.join(old_argument_list), '\n'.join(new_argument_list)
            prompt =  f"以下是投資機構針對{ticker}的新、舊觀點，請針對新增與減少的部分寫成一篇約150字的短文。"
            prompt += f"回傳格式：以markdown語法顯示，段落標題為「新增的部分」與「減少的部分」，格式使用### \n"
            prompt += f"舊的論點: {old_argument}\n\n"
            prompt += f"新的論點: {new_argument}\n\n"
            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format="text")
            
        # 取得自上次總結後，新上傳報告的meta data，預設為最新的10篇
        # 查找stock_report_review中data_timestamp字段的最大值
        last_reviews_doc = self.MDB_client["published_content"]["stock_report_review"].find_one(sort=[("data_timestamp", -1)])

        last_processed_timestamp = None
        if last_reviews_doc:
            last_processed_timestamp = last_reviews_doc["data_timestamp"]
            
        stock_report_meta_list = list(self.MDB_client["preprocessed_content"]["stock_report"].find({"ticker": ticker, "processed_timestamp": {"$gt": last_processed_timestamp}})
                                    .sort([("processed_timestamp", -1)]).limit(review_report_nums))
        
        # 若無新的報告，則不進行總結
        if len(stock_report_meta_list) == 0:
            return
        logging.info(f"[SERVER][Data Process][{ticker}][stock_report_review]")
        # 逐篇進行摘要，取得看多/看空的論點（json格式）
        summary_list = [stock_report_meta["summary"] for stock_report_meta in stock_report_meta_list]
            
        # 調用LLM，取得看多/看空的論點（json格式）
        stock_report_review_json_text = _create_raw_stock_report_review(summary_list, ticker, output_format="json_object")
        stock_report_review = json.loads(stock_report_review_json_text)
        # 取得看多/看空的論點(dict)
        bullish_outlook_raw_text, bearish_outlook_raw_text = stock_report_review["bullish_outlook"], stock_report_review["bearish_outlook"]
        # 調整看多論點格式，原先為整段文字，改為條列式的論點列表
        bullish_argument_text = _adjust_stock_report_review_format(summary_text=bullish_outlook_raw_text, ticker=ticker, output_format="text")
        bullish_outlook_argument_list = _parse_outlook_argument(argument_text=bullish_argument_text)
        # 調整看空論點格式，原先為整段文字，改為條列式的論點列表
        bearish_argument_text = _adjust_stock_report_review_format(summary_text=bearish_outlook_raw_text, ticker=ticker, output_format="text")
        bearish_outlook_argument_list = _parse_outlook_argument(argument_text=bearish_argument_text)
        # 取得舊的看多/看空論點，進行比較
        old_stock_report_review_meta = self.MDB_client["published_content"]["stock_report_review"].find_one({"ticker": ticker}, sort=[("date", -1)])
        
        if old_stock_report_review_meta:
            old_bullish_argument_list = old_stock_report_review_meta["stock_report_review"]["bullish_outlook"]
            old_bearish_argument_list = old_stock_report_review_meta["stock_report_review"]["bearish_outlook"]
            
            bullish_outlook_diff = _compare_outlook_argument(old_argument_list=old_bullish_argument_list, new_argument_list=bullish_outlook_argument_list)
            bearish_outlook_diff = _compare_outlook_argument(old_argument_list=old_bearish_argument_list, new_argument_list=bearish_outlook_argument_list)
        # 若無舊的看多/看空論點，則不進行比較，填入空字串
        else:
            bullish_outlook_diff, bearish_outlook_diff = "", ""
            
        # 存入MongoDB
        stock_report_review_meta = {
            "ticker": ticker,
            "data_timestamp": datetime(datetime.now().year, datetime.now().month, datetime.now().day),
            "upload_timestamp": datetime.now(),
            "stock_report_review": {
                "bullish_outlook": bullish_outlook_argument_list,
                "bearish_outlook": bearish_outlook_argument_list,
                "bullish_outlook_diff": bullish_outlook_diff,
                "bearish_outlook_diff": bearish_outlook_diff,
            },
        }
        self.MDB_client["published_content"]["stock_report_review"].insert_one(stock_report_review_meta)
    
    def save_issue_review(self, issue_id, max_days=90, max_doc_num=30):
        def _get_issue_concensus_and_dissensus_LLM(issue, issue_review_text):
            prompt = f"根據以下的「市場看法」，具體整理出市場對「{issue}」議題的共識與差異點。\n"
            prompt += f"請將市場共識和差異分為兩個段落進行整理，每個部分以具體事例和數據進行說明。\n"
            prompt += f"要求：\n"
            prompt += f"1. 先列出市場共識，以「共識點」作為段落開頭，描述市場各方對此議題的共同看法，針對不同主題條列式整理(包含觀點 & 數據)。\n"
            prompt += f"2. 接下來列出市場差異，以「差異點」作為段落開頭，說明不同機構或來源對此議題的分歧，針對不同主題條列式整理(包含觀點 & 數據)。\n"
            prompt += f"3. 請使用純文字格式，不要有Markdown語法或額外的內容，除了條列式整理應以「-」開頭。\n"
            prompt += f"4. 不要包含任何其他內容，如系統回覆、開場白、問候語、結語等。\n\n"
            prompt += f"市場看法: {issue_review_text}\n"

            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format="text")

        # 調用LLM，將新的問題總結與舊的問題總結進行比較，找出差異
        def _get_issue_review_change_text_LLM(issue, new_issue_review, old_issue_review, output_format="text"):
            prompt = f"針對同一議題『{issue}』，比較「新的論述」和「舊的論述」中的觀點變化，重點分析「原有觀點」與「現有觀點」的具體轉變。\n"
            prompt += f"要求：\n"
            prompt += f"1. 若存在觀點轉變，請將具體的變化內容整合為一篇簡短的文章，清晰描述這些轉變的內容。著重指出新的論述是如何修正、補充或反駁舊的觀點。\n"
            prompt += f"2. 若無觀點轉變，請返回空字串（''）。\n"
            prompt += f"3. 回答必須是純文字格式，不要使用 Markdown 語法或包含其他系統回覆、問候語等無關內容。\n"
            prompt += f"\n新的論述: {new_issue_review}\n"
            prompt += f"舊的論述: {old_issue_review}\n"
            
            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format=output_format)
        
        issue_meta = self.MDB_client["users"]["following_issues"].find_one({"_id": issue_id})
        issue, ticker_list = issue_meta["issue"], issue_meta["tickers"]
        # 取得issue的追蹤者id（用於寄送用戶通知） 
        following_user_id_list = issue_meta.get("following_users", [])
        
        logging.info(f"[SERVER][issue_review][{issue}]")
        
        # 設定搜尋文件的時間範圍
        start_timestamp = datetime.now() - timedelta(days=max_days)
        end_timestamp = datetime.now()
        
        # 取得包含此issue id並且issue_content不為空的stock report
        market_stock_report_meta_list = list(self.MDB_client["preprocessed_content"]["stock_report"].find(
            {
                "issue_summary": {
                    "$elemMatch": {
                        "issue_id": issue_id,
                        "issue_content": {"$ne": ""}
                    }
                },
                "data_timestamp": {"$gte": start_timestamp, "$lte": end_timestamp}
            },
            projection={"issue_summary": {"$elemMatch": {"issue_id": issue_id, "issue_content": {"$ne": ""}}}, "data_timestamp": 1, "source": 1}
        ).limit(max_doc_num))
        
        logging.info(f"[SERVER][issue_review][{issue}][共找到{len(market_stock_report_meta_list)}篇參考報告]")
        # 取得參考的report id以留存紀錄
        ref_report_id_list = [
            report_meta["_id"] for report_meta in market_stock_report_meta_list
        ]
        
        # 取得搜尋時間範圍內的新聞（依照ticker_list搜尋）
        market_info_meta_list = list(self.MDB_client["preprocessed_content"]["stock_news_summary"].find(
            {"ticker": {"$in": ticker_list},
            "data_timestamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
        ))
        
        # 預先定義變數，避免後續出現未定義的情況        
        added_report_meta_list = []
        added_issue_review_text = ''
        issue_review_change_text = ''
        
        # 與前一次的issue_review，進行比較，判斷是否需要進行更新
        last_issue_review_meta = self.MDB_client["published_content"]["issue_review"].find_one({"issue_id": issue_id}, sort=[("upload_timestamp", DESCENDING)])
        min_report_num = 3
        
        # 如果有找到前次記錄，提取參考報告的 _id 列表；否則，初始化為空列表
        last_ref_report_id_list = last_issue_review_meta.get("ref_report_id", []) if last_issue_review_meta else []
        # 計算新增的報告 id 列表
        added_report_id_list = list(set(ref_report_id_list) - set(last_ref_report_id_list))

        # 檢查新增的參考報告數量是否足夠
        if len(added_report_id_list) < min_report_num:
            logging.info(f"[SERVER][issue_review][{issue}][新增報告數量小於{min_report_num}篇，本次不進行更新]")
            return
            
        # 生成市場觀點總結（issue_review）
        market_issue_review_text = self._get_issue_review_text_LLM(issue_meta, market_stock_report_meta_list, market_info_meta_list)
        # 依照市場觀點總結，列出市場共識與差異點
        market_issue_concensus_and_dissensus_text = _get_issue_concensus_and_dissensus_LLM(issue, market_issue_review_text)
        
        # 若此前已有issue_review，則進行以下，否則略過此部分
        if last_issue_review_meta:
            added_report_meta_list = list(self.MDB_client["preprocessed_content"]["stock_report"].find({"_id": {"$in": added_report_id_list}}))
            # 針對新增的報告，生成市場觀點總結
            added_issue_review_text = self._get_issue_review_text_LLM(issue, added_report_meta_list)
            # 對比新舊的市場觀點總結，找出市場轉向觀點
            issue_review_change_text = _get_issue_review_change_text_LLM(
                issue=issue,
                new_issue_review=added_issue_review_text, 
                old_issue_review=last_issue_review_meta["market_issue_review"]
                )
        
        # 存入MongoDB
        self.MDB_client["published_content"]["issue_review"].insert_one(
            {
                "issue_id": issue_meta["_id"],
                "issue": issue,
                "data_timestamp": datetime(datetime.now().year, datetime.now().month, datetime.now().day),
                "upload_timestamp": datetime.now(),
                "market_issue_review": market_issue_review_text,
                "added_issue_review": added_issue_review_text,
                "market_issue_concensus_and_dissensus": market_issue_concensus_and_dissensus_text,
                "issue_review_change": issue_review_change_text,
                "ref_report_id": ref_report_id_list,
                "added_report_id": added_report_id_list,
            }
        )
        # 紀錄issue的更新時間（updated_timestamp）
        self.MDB_client["users"]["following_issues"].update_one({"_id": issue_id}, {"$set": {"updated_timestamp": datetime.now(timezone.utc)}})
        
        # 用於render通知模板的變數字典
        variables_dict = {
                "issue": issue,
                # _external=True 参数确保生成的是绝对 URL
                "page_url": f"/main/investment_issue_review/{issue_id}",
            }
        # 寄送用戶通知
        self.create_notification(user_id_list=following_user_id_list, 
                                     priority=2,
                                     notification_type="update", # "system"、"update"、"todo" 或 "alert"。
                                     notification_sub_type="investment_issue_review_update",
                                     variables_dict=variables_dict)

        
    # 生成問題總結 (following issues review)，並儲存到資料庫（MongoDB）
    
    def _get_issue_review_text_LLM(self, issue, stock_report_meta_list, market_info_meta_list=None):
        # 調用LLM，生成問題總結
        def _create_issue_review_LLM(issue, issue_content_json, market_info_content, output_format="text"):
            prompt = f"針對「{issue}」議題，請整合以下不同機構的報告內容和新聞資訊，詳細說明市場對此議題的看法與近期資訊。\n"
            prompt += f"要求：\n"
            prompt += f"1. 不要生成開場白或總結\n"
            prompt += f"2. 每段內容要盡量詳細，盡量保留該報告中的內容，須包括來源機構的觀點以及發布日期：\n"
            prompt += f"3. 確保內容來自以下提供的素材，不要添加其他內容\n"
            prompt += f"4. 請使用純文字格式，不要有任何 Markdown 語法或其他額外的內容。\n\n"
            
            prompt += f"「報告內容」:\n {issue_content_json} \n\n"
            prompt += f"「新聞資訊」:\n {market_info_content} \n"

            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format=output_format)
        
        def _filter_market_info_LLM(market_info_content):
            prompt =  f"以下是針對市場的新聞資訊，請篩選出與「{issue}」高度相關的部分，過濾掉無關與低度相關的內容，整理成短文，標註日期但去除來源（url）。\n"
            prompt += f"「新聞資訊」:\n {market_info_content} \n"
            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format="text")
        
        # 將自MongoDB取得的issue資料進行整理，改為適合GPT解析的格式，dict (source: content)
        def _generate_issue_json_string(stock_report_meta_list):
            # 將stock_report_meta_list按照data_timestamp進行排序（可使AI摘要的內容顯示由新到舊）
            stock_report_meta_list = sorted(stock_report_meta_list, key=lambda x: x["data_timestamp"], reverse=True)
            issue_content_dict = defaultdict(list)
            for stock_report_meta in stock_report_meta_list:
                issue_summary_meta_list = stock_report_meta.get("issue_summary", [])
                # 先前已經使用pymongo的運算子過濾，不需再次過濾
                for issue_summary_meta in issue_summary_meta_list:
                    issue_content = issue_summary_meta.get("issue_content", '')
                
                # 假设 source_trans_dict 和 keys_to_convert 是全局变量或类属性
                # 待改：應集中管理
                source_trans_dict = {"gs": "Goldman Sachs", 
                                     "jpm": "J.P. Morgan", 
                                     "citi": "Citi", 
                                     "barclays": "Barclays",
                                     "seeking_alpha": "Seeking Alpha"}
                
                source = source_trans_dict.get(stock_report_meta["source"], "other")
                issue_content_dict[source].append(
                    {
                        "date": datetime2str(stock_report_meta.get("data_timestamp")),
                        "content": issue_content,
                    }
                )   
            # 使用 ensure_ascii=False 将 JSON 转换为 Unicode 编码格式
            issue_content_json_string = json.dumps(issue_content_dict, ensure_ascii=False, indent=4)
            return issue_content_json_string
        
        issue_json_string = _generate_issue_json_string(stock_report_meta_list)
        
        market_info_content = ''
        if market_info_meta_list is not None:
            for info_meta in market_info_meta_list:
                market_info_content += datetime2str(info_meta["data_timestamp"])
                market_info_content += f"內容: {info_meta["content"]}"
        
        filtered_market_info_content = _filter_market_info_LLM(market_info_content)
        issue_review_text = _create_issue_review_LLM(issue, issue_json_string, filtered_market_info_content)
        return issue_review_text
    
    def save_investment_assumption_review(self, assumption_id):
        # 使用正則表達式解析LLM回傳的review，取得風險分數，並刪除包含風險分數的那一行文字（若有）
        def _get_risk_score(text):
            match = re.search(r"風險分數[\s：]*\d+", text)

            if match:
                risk_score = int(re.search(r"\d+", match.group()).group())  # 提取並轉換為整數
                # 刪除包含風險分數的那一行
                clean_text = re.sub(r"風險分數[\s：]*\d+\s*\n?", "", text)
                return risk_score, clean_text
            
            return None, text
        
        # 從 MongoDB 中檢索啟用的投資假設資料
        investment_assumptions_meta = self.MDB_client["users"]["investment_assumptions"].find_one({"_id": assumption_id})
        # 獲取投資假設
        investment_assumption = investment_assumptions_meta["assumption"]
        related_ticker_list = investment_assumptions_meta["tickers"]
        
        # 設定 prompt 初始文字
        prompt = (
            f"以下是針對 {', '.join(related_ticker_list)} 的投資假設，"
            f"請根據以下的市場資訊，判斷投資假設是否已經被推翻，並計算風險分數（0～100），"
            f"越高分代表被推翻的機率越高。\n"
            f"回傳格式：「風險分數」、「總結」、「支持假設的資訊」、「反對假設的資訊」，"
            f"除此之外不需要包含系統訊息。此外不要使用 markdown 語法的符號，純文字即可。\n\n"
            f"投資假設：「{investment_assumption}」\n\n"
        )

        # 獲取與假設相關的市場資訊
        linked_issues_meta_list = investment_assumptions_meta.get("linked_issues", [])
        
        for linked_issues_meta in linked_issues_meta_list:
            issue = linked_issues_meta.get("issue")
            issue_id = linked_issues_meta.get("issue_id")
            
            # 查詢對應的市場資訊文本
            issue_review = self.MDB_client["published_content"]["issue_review"].find_one(
                {"issue_id": issue_id},
                sort=[("data_timestamp", -1)],
                projection={"_id": 0, "added_issue_review": 1}
            )
            
            # 如果查詢結果存在，則將其添加到 prompt 中
            if issue_review and "added_issue_review" in issue_review:
                issue_review_text = issue_review["added_issue_review"]
                prompt += f"市場資訊： {issue}\n{issue_review_text}\n\n"

        assumption_review_text = call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format="text")
        risk_score, assumption_review_text = _get_risk_score(text=assumption_review_text)
        
        # 存入MongoDB
        self.MDB_client["published_content"]["assumption_review"].insert_one(
            {
                "assumption_id": assumption_id,
                "investment_assumption": investment_assumption,
                "linked_issues": linked_issues_meta_list,
                "risk_score": risk_score,
                "data_timestamp": datetime(datetime.now().year, datetime.now().month, datetime.now().day),
                "upload_timestamp": datetime.now(),
                "assumption_review": assumption_review_text,
            }
        )
    
    def create_notification(self, user_id_list, priority, notification_type, notification_sub_type, variables_dict=None, meta_data_dict=None):
        # If using a mutable default in paramenter({}), it can lead to unexpected behavior if the function is called multiple times
        if meta_data_dict is None:
            meta_data_dict = {}
        
        # 依照通知類型（包含主類型與sub類型），取得通知模板
        template_dict = _all_notification_template_dict.get(notification_type, {}).get(notification_sub_type, {})
        if not template_dict:
            logging.error(f"[SERVER][NOTIFICATION][TEMPLATE][{notification_type}][{notification_sub_type}] 沒有對應的通知模板")
            return
        
        # 取得模板中的標題與內容，並進行變數渲染（render）
        rendered_title = Template(template_dict.get("title", '')).render(**variables_dict)
        rendered_message = Template(template_dict.get("message", '')).render(**variables_dict)
        
        notifications = []
        for user_id in user_id_list:
            notification_meta = {
                "user_id": user_id,
                "type": notification_type,
                "sub_type": notification_sub_type,
                "priority": priority,
                "title": rendered_title,
                "message": rendered_message,
                "upload_timestamp": datetime.now(timezone.utc),
                "is_read": False,
                "is_displayed": False,
                "meta_data": meta_data_dict
            }
            notifications.append(notification_meta)

        # Perform a bulk insert for efficiency
        if notifications:
            self.MDB_client["users"]["notifications"].insert_many(notifications)

    def get_shorts_summary(self, ticker, data_timestamp=None, start_timestamp=None, end_timestamp=None):
        query = {"ticker": ticker}
        if data_timestamp:
            query["data_timestamp"] = data_timestamp
            
        # 如果有時間範圍，則加入時間條件
        elif start_timestamp or end_timestamp:
            timestamp_range_dict = {}
            if start_timestamp:
                timestamp_range_dict["$gte"] = start_timestamp
            if end_timestamp:
                timestamp_range_dict["$lte"] = end_timestamp
            query["data_timestamp"] = timestamp_range_dict
        
        shorts_summary_list = list(self.MDB_client['preprocessed_content']['shorts_summary'].find(query, sort=[("data_timestamp", -1)]))
        return shorts_summary_list

    def get_stock_news(self, ticker, start_timestamp=None, end_timestamp=None, data_source=None):
        # 建立查詢條件字典，包含 tickers 條件
        query = {"tickers": ticker}
        # 如果有時間範圍，則加入時間條件
        if start_timestamp or end_timestamp:
            timestamp_range_dict = {}
            if start_timestamp:
                timestamp_range_dict["$gte"] = start_timestamp
            if end_timestamp:
                timestamp_range_dict["$lte"] = end_timestamp
            query["data_timestamp"] = timestamp_range_dict
            
        if data_source:
            query["data_source"] = data_source
        else:
            data_source = "All" # 用於顯示
        
        # 根據查詢條件查詢數據
        start_timestamp_str = datetime2str(start_timestamp)
        end_timestamp_str = datetime2str(end_timestamp)
        
        news_meta_list = list(self.MDB_client['raw_content']['raw_stock_news'].find(query, sort=[("data_timestamp", -1)]))
        logging.info(f"[SERVER][DATA][NEWS][{ticker}][{data_source}][{start_timestamp_str}][{end_timestamp_str}] 共{len(news_meta_list)}則新聞")
        news_meta_list = remove_duplicates_by_key(news_meta_list, "url")
        logging.info(f"[SERVER][DATA][NEWS][{ticker}][{data_source}][{start_timestamp_str}][{end_timestamp_str}] 去除重複後，剩餘{len(news_meta_list)}則新聞")
        return news_meta_list        
            
# def save_stock_reports_auto(self, ticker_list, start_date=None):
    #     if start_date == None:
    #         # MongoDB返回的datetime不帶時區資訊，此處進行轉換，以便與新聞utc對接
    #         start_date = self.get_latest_data_date(item="raw_stock_report_auto")
    #     if isinstance(start_date, str):
    #         start_date = str2datetime(start_date)

    #     df_list = list()
    #     for ticker in ticker_list:
    #         logging.info(f"[SAVE][stock_report_auto][{ticker}][{datetime2str(start_date)}~{TODAY_DATE_STR}]")
    #         # 從seekingalpha下載stock report
    #         logging.info(f"[SAVE][stock_report_auto][{ticker}][source: seekingalpha]")
    #         # 不該直接傳入start_date，因個別標的最新的報告收錄時間，不適用於其他標的
    #         _df = get_stock_report_from_seekingalpha(API_key=self.rapid_API_key, ticker=ticker)
    #         df_list.append(_df)
        
    #     df = pd.concat(df_list).sort_values(by="date")
    #     df = self._group_news_df_by_ticker(df)
    #     df = df[df["date"] >= start_date].reset_index(drop=True)
    
    #     data_list = df.to_dict("records")
    #     self.save_data_to_MDB(item="raw_stock_report_auto", data_list=data_list, upsert=True, key="url")
    
    # def get_raw_stock_news_df(self, ticker, start_date):
    #     all_news_df = self.get_item_df(item="raw_stock_news", method="by_date", start_date=start_date)
    #     # 篩選出包含此ticker的news
    #     news_df = all_news_df[all_news_df["ticker"].apply(lambda x: ticker in x)]
    #     # 因資料庫中可能存在重複的新聞，故刪除
    #     news_df = news_df.drop_duplicates("url")
    #     news_df = news_df.loc[:, ["title", "url", "source"]]
    #     news_df = news_df.reset_index()
    #     return news_df