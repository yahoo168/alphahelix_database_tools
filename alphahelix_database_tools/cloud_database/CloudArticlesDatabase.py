from collections import defaultdict
from datetime import datetime
import os, json
from pymongo import DESCENDING
from jinja2 import Template #type: ignore

from dotenv import load_dotenv #type: ignore

from alphahelix_database_tools.external_tools.news_tools import *
from alphahelix_database_tools.external_tools.openai_tools import call_OpenAI_API, truncate_text_to_token_limit, get_embedding
from alphahelix_database_tools.external_tools.pdf_tools import get_pdf_text_from_url, clean_gibberish_text
from alphahelix_database_tools.external_tools.google_tools import GoogleCloudStorageTools

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

# 之後移動到資料庫中
ticker_disambiguation_dict = {
        "GOOGL": "GOOG",
        "BRK.B": "BRK",
        "BRK.A": "BRK"
}

class CloudArticlesDatabase(AbstractCloudDatabase):
    def __init__(self, config_folder_path=None):
        super().__init__(config_folder_path=config_folder_path)  #調用AbstractCloudDatabase 的__init__方法
        self._load_api_keys()
        self.MDB_client = MongoClient(self.cluster_uri_dict["articles"], server_api=ServerApi('1'))
        
        # 待確認：這樣合適嗎？
        self.pool_list_db = CloudPoolListDatabase()
        
    # 載入 .env 文件(用於本地測試)，並取用環境變數
    def _load_api_keys(self):
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
        
        # # 取得所有用戶id列表
        user_id_list = self.pool_list_db.get_active_user_id_list()
        # 測試用：只寄送給特定用戶
        #user_id_list = [ObjectId("66601790f20eb424a340acd3")]
        
        # 用於render通知模板的變數字典
        notification_variables_dict = {
                "date": datetime2str(datetime.now() - timedelta(days=1)),
                "page_url": f"/main/ticker_news_overview",
            }
        
        # 寄送用戶通知
        self.create_notification(user_id_list=user_id_list, 
                                        priority=3,
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

    def save_shorts(self, start_date=None):
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
                prompt += (news_meta.get("title") or '') + '\n' # 若title為None，則取空字串（曾發生過value存在但為None，導致報錯）
                prompt += datetime2str(news_meta.get("data_timestamp", '')) + '\n'
                prompt += (news_meta.get("content") or '') + '\n'
                prompt += (news_meta.get("url") or '') + '\n\n'
                
            summary_content = call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="o1-mini", output_format="text")
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
            
    # 針對特定issue，在一定的期間內，找出尚未針對此issue進行摘要的document並進行摘要（以ticker搜索document）
    def extract_documents_of_certain_issue(self, issue_id, max_days=90, max_doc_num=30):
        # 針對特定issue，找出尚未針對此issue進行摘要的document meta list（以ticker搜索），可限定最長回顧天數與最大報告數量
        def _get_unextracted_documents_of_certain_issue(issue_id, max_days, max_doc_num):
            # 依照issue_id，取得issue的meta資料
            issue_meta = self.MDB_client["users"]["following_issues"].find_one({"_id": issue_id})
            # 取得與issue相關的tickers
            ticker_list = issue_meta["tickers"]
            # 若有多個ticker，限制最大文件數量為100
            max_doc_num = min(len(ticker_list) * max_doc_num, 100)
            # 待改：針對tickers搜索相關的文件（目前僅有stock report）
            document_meta_list = list(self.MDB_client["preprocessed_content"]["stock_report"]
                                        .find({"tickers": {"$in": ticker_list}, 
                                                "data_timestamp": {"$gte": datetime.now() - timedelta(days=max_days)}
                                            })
                                        .sort("data_timestamp", DESCENDING)
                                        .limit(max_doc_num))
            
            # for-loop目前已經摘要過的issue，過濾掉已經有此issue的document
            result_document_meta_list = []
            # 檢查 text_summaries 中的 issue_summaries 是否包含指定的 issue_id
            for document_meta in document_meta_list:
                if not any(issue["issue_id"] == issue_id for issue in document_meta.get("issue_summaries", [])):
                    result_document_meta_list.append(document_meta)
                
            return result_document_meta_list
        
        # 依照issue_id，取得issue的meta資料
        issue_meta = self.MDB_client["users"]["following_issues"].find_one({"_id": issue_id})
        # 取得issue名稱（即issue）
        issue = issue_meta["issue"]
        # 找出尚未針對此issue進行摘要的document
        document_meta_list = _get_unextracted_documents_of_certain_issue(issue_id=issue_id, max_days=max_days, max_doc_num=max_doc_num)
        logging.info(f"[SERVER][investment_issue][check][{issue}] Found {len(document_meta_list)} unextracted documents")
        
        for index, document_meta in enumerate(document_meta_list):
            logging.info(f"[SERVER][LLM] Summarizing({index}). Title: {document_meta['title']}")
            report_id, pdf_file_url = document_meta["_id"], document_meta["url"]
            content_text = get_pdf_text_from_url(pdf_file_url)
            # 進行issue摘要（只有一個issue，故直接取第一個元素）
            issue_summary_meta = self.create_issue_summary(content_text, [issue_meta])[0]
            
            # 將issue content存入MongoDB（在原document的issue_summary中添加issue_content）
            self.MDB_client["preprocessed_content"]["stock_report"].update_one({"_id": report_id}, {"$push": {"issue_summaries": issue_summary_meta}})
            
    def create_issue_summary(self, content_text, issue_meta_list):
        # 摘要投資議題相關內容（調用LLM）
        def _create_issue_summary_LLM(content_text, issue_list, word_number=500):
            prompt = (
                f"以下是一篇投資研究報告，依據以下不同的'issue'，各自挑選出相關的段落（盡量完整），"
                f"整合成短文後翻譯為「繁體中文」，各個段落短文的字數約{word_number}字。\n"
                f"切勿添加本篇報告以外的內容，僅依照本篇報告提供的內容整理即可，若沒有相關內容可返回空字串('')。\n"
                f"'issue':「{', '.join(issue_list)}」\n"
                
                f"格式請確保返回的 JSON 格式中只有以下key：{', '.join(issue_list)}，value則為對應的issue段落短文）\n"
                f"報告內容:\n\n{content_text}"
            )
            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o-mini", output_format="json_object")
        
        # 建立issue列表和issue_id映射表
        issue_id_mapping = {item_meta['issue']: item_meta['_id'] for item_meta in issue_meta_list}
        standardized_issue_list = [standardize_key(item_meta['issue']) for item_meta in issue_meta_list]
        
        try:
            # 呼叫 LLM 獲取摘要內容
            issue_content_json_text = _create_issue_summary_LLM(content_text, standardized_issue_list)
            issue_content_dict, _ = standardize_dict(json.loads(issue_content_json_text))
        
        except Exception as e:
            logging.error(f"Error parsing issue content JSON: {e}")
            return {}

        # 生成 issue_summary_list
        issue_summary_meta_list = []
        for item_meta in issue_meta_list:
            issue_content = str(issue_content_dict.get(standardize_key(item_meta["issue"]), '')) # 若無此issue，則取空字串
            
            # 限制issue_content最小長度，因LLM可能回傳「空字串」等無意義內容
            issue_content = '' if len(issue_content) <= 10 else issue_content
            
            issue_summary_meta = {
                "issue": item_meta["issue"],
                "issue_id": issue_id_mapping[item_meta["issue"]],
                "issue_content": issue_content
            }
            issue_summary_meta_list.append(issue_summary_meta)
            
        return issue_summary_meta_list
    
    def process_raw_stock_report(self, document_meta):
        def _get_stock_report_full_text_summary_LLM(content_text, ticker):
            prompt = (f"以下是一篇「股票代號為{ticker}的公司」的研究報告，請深入整理其中的重點，並提供詳細的摘要，要求如下：\n"
                      "1. 包括「投資建議與理由、全文摘要、看多論點、看空論點、產業趨勢」，共5個段落，並翻譯為繁體中文。\n"
                      "2. 回傳內容僅包含以上段落，不要包含任何內容，以及開場白或結語。\n"
                      "3. 不要以文章標題作為開頭，直接從「投資建議與理由」開始撰寫。\n"
                      "4. 格式：使用Markdown語法回傳。\n"
                      )
            
            prompt += f"研究報告內容:\n\n {content_text}"
            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o-mini", output_format="text")
        
        ticker, pdf_file_url = document_meta["tickers"][0], document_meta["url"]
        content_text = get_pdf_text_from_url(pdf_file_url)
        result_dict = {
            "text_summaries": {
                "full_text_summary": _get_stock_report_full_text_summary_LLM(content_text, ticker),
            }
        }
        return result_dict
    
    def process_raw_stock_transcript(self, document_meta, is_compare_last_one=True):
        def _create_stock_transcript_full_text_summary_LLM(ticker, content_text):
            prompt = (f"以下是一篇「股票代號為{ticker}的公司」財報會議的逐字稿，請整理其中的內容，盡可能保留細節，並翻譯為繁體中文（專有名詞、人名、業務項目保留原文）"
                      f"將內容分段呈現，並確保每段落的標題依照以下順序：「全文摘要、財務表現與Guidance、業務營收占比與驅動因素、未來展望與投資計劃、風險與挑戰、市場競爭和行業動態」。"
                      f"格式：使用markdown語法，不要包含任何的系統開場白與結語")
            
            prompt += f"財報會議逐字稿:\n\n {content_text}"
            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o-mini", output_format="text")
        
        def _create_stock_transcript_analyst_QA_summary_LLM(ticker, content_text):
            prompt = (f"以下是一篇「股票代號為{ticker}的公司」財報會議的逐字稿，請整理其中的分析師問答部分，並翻譯成繁體中文（專有名詞、人名、業務項目保留原文並加註）。"
                      f"將內容以 Markdown 格式整理，確保涵蓋所有問題與回答，不要遺漏。範例格式如下，不要包含任何的系統開場白與結語：\n\n"
                      
                      f"## Truist Securities (Will Stein) \n"
                      f"- 問題：市場上對於 Tesla 商業化的擔憂，特別是與 AI 有關。Tesla 如何在 Tesla 和 xAI 之間分配資源，以確保 Tesla 能夠受益？\n"
                      f"- Elon Musk（CEO）：關於 GPU 轉移到 xAI 的報導過時且被誤解了。當時 Tesla 沒有足夠的空間來安置這些 GPU，將它們分配到其他地方實際上是符合 Tesla 的利益。\n\n")
            
            prompt += f"財報會議逐字稿:\n\n {content_text}"
            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o-mini", output_format="text")
        
        def _create_prior_period_comparison(ticker, current_content_text, last_content_text):
            prompt = (f"請對股票代號為「{ticker}」的公司新一季與前一季的財報會議內容進行詳細對比分析，要求如下：\n"
                    f"1. 內容盡可能包含細節\n"
                    f"2. 以繁體中文書寫（專有名詞、業務部門、公司特定產品，保留英文名詞）。\n"
                    f"3. 請按照以下面向進行對比：「財務規劃、管理層態度、分析師提問主題、產品與技術進展、市場策略、行業趨勢」\n"
                    f"格式範例（使用 Markdown 語法）："
                    
                    f"### 產品與技術進展\n"
                    f"- 本季相較於上一季，凸顯了公司在矽碳化鎢領域的佈局和持續增長的市場需求，第二季ON宣布收購SWIR Vision Systems，藉此強化其在工業與國防領域的產品組合，同時展示其矽碳化鎢技術的領先地位，尤其在電動車（EV）市場擴展上取得突破。\n"
                    f"而在第三季，隨著中國BEV（電池電動車）市場需求增加，矽碳化鎢產品的收入顯著增長，同時公司在全球數據中心的電源管理解決方案上也獲得了設計訂單支持。\n"
                    f"此外公司還成功完成了200毫米M3e矽碳化鎢技術的資格認證，為未來的擴展奠定了技術基礎。\n\n"
                    
                    f"請勿包含任何系統開場白或結語"

                    f"新一季財報會議逐字稿：\n\n{current_content_text}\n\n"
                    f"前一季財報會議逐字稿：\n\n{last_content_text}"
                )
            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o-mini", output_format="text")
        
        ticker, pdf_file_url = document_meta["tickers"][0], document_meta["url"]
        content_text = get_pdf_text_from_url(pdf_file_url)
        
        if is_compare_last_one:
            # 取得上一篇財報會議逐字稿的內容
            last_document_meta = self.MDB_client["preprocessed_content"]["event_document"].find_one(
                        {"tickers": ticker, "data_timestamp": {"$lt": document_meta["data_timestamp"]}},
                        sort=[("data_timestamp", -1)]  # 按 `data_timestamp` 由大到小排序
                    )
            if last_document_meta:
                last_content_text = get_pdf_text_from_url(last_document_meta["url"])
                prior_period_comparison = _create_prior_period_comparison(ticker, content_text, last_content_text)
            else:
                prior_period_comparison = ''
        else:
            prior_period_comparison = ''
        
        result_dict = {
            "text_summaries": {
                "full_text_summary": _create_stock_transcript_full_text_summary_LLM(ticker, content_text),
                "analyst_QA_summary": _create_stock_transcript_analyst_QA_summary_LLM(ticker, content_text),
                "prior_period_comparison": prior_period_comparison
            }
        }
        return result_dict
        
    # 自動找出尚未預處理的stock report並進行處理（全文摘要 & 追蹤問題摘要）
    def process_raw_documents(self, document_type="stock_report"):
        # 根據document_type選擇不同的參數
        if document_type == "stock_report":
            source_collection = self.MDB_client["raw_content"]["raw_stock_report_non_auto"]
            des_collection = self.MDB_client["preprocessed_content"]["stock_report"]
            notification_template_type = "stock_report_update"
        
        elif document_type == "transcript":
            source_collection = self.MDB_client["raw_content"]["raw_event_document"]
            des_collection = self.MDB_client["preprocessed_content"]["event_document"]
            notification_template_type = "transcript_update"
                
        else:
            logging.error(f"[SERVER][Data Process][{document_type}]: No such document type.")
            return
        
        # 取出尚未處理的documents
        document_meta_list = list(source_collection.find({"is_processed": False, "tickers": {"$ne": []}}))
        
        logging.info(f"[SERVER][Data Process][共{len(document_meta_list)}篇個股文件待處理]")
        
        for index, document_meta in enumerate(document_meta_list):
            document_id, title, ticker_list = document_meta["_id"], document_meta["title"], document_meta["tickers"]
            logging.info(f"[SERVER][Data Process][{title}][開始處理第{index + 1}/{len(document_meta_list)}篇non auto stock report]")
            
            # 待改：取出第一個ticker（若有多個ticker，僅取第一個）
            ticker = ticker_list[0]
            
            if document_type == "stock_report":    
                extracted_data_dict = self.process_raw_stock_report(document_meta)
                following_user_id_list = self.pool_list_db.get_ticker_following_user_list(ticker)
                
            elif document_type == "transcript":
                extracted_data_dict = self.process_raw_stock_transcript(document_meta)
                # 財報會議逐字稿預設發送給所有用戶
                following_user_id_list = self.pool_list_db.get_active_user_id_list()
                #following_user_id_list = [ObjectId("66601790f20eb424a340acd3")]
            
            content_text = get_pdf_text_from_url(document_meta["url"])
            
            # 取得相關issue的meta資料
            issue_meta_list = list(self.MDB_client["users"]["following_issues"].find({"tickers": ticker, "is_active": True}, limit=10))
            issue_summaries = self.create_issue_summary(content_text, issue_meta_list) if len(issue_meta_list) > 0 else []
            
            # 更新document_meta的內容
            document_meta.update(extracted_data_dict)
            document_meta["issue_summaries"] = issue_summaries
            document_meta["processed_timestamp"] = datetime.now(timezone.utc)
            
            # 保存處理後的stock report meta到MongoDB，並取得inserted_id
            mongodb_ops_result = des_collection.insert_one(document_meta)
            document_id = mongodb_ops_result.inserted_id
            
            # 將原始文件標識為已處理
            source_collection.update_one(
                {"_id": document_id}, {"$set": {"is_processed": True}}
            )
            
            # 若本標的有追蹤用戶，則發送通知（依照文件類型發送不同的通知模板 / 用戶）
            if following_user_id_list:
                # 用於render通知模板的變數字典
                variables_dict = {
                        "ticker": ticker,
                        "page_url": f"/main/stock_document/US/{document_type}/{document_id}",
                        "title": os.path.splitext(title)[0], # 去除檔名的副檔名
                    }
                # 發送通知
                self.create_notification(user_id_list=following_user_id_list, 
                                        priority=2,
                                        notification_type="update", # "system"、"update"、"todo" 或 "alert"。
                                        notification_sub_type=notification_template_type,
                                        variables_dict=variables_dict)
                
    def save_issue_review(self, issue_id, max_days=90, max_doc_num=30, min_report_num=5, end_timestamp=None):
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
        
        # 取得issue基本資訊：issue內容、相關ticker、追蹤者id（用於寄送用戶通知） 
        issue_meta = self.MDB_client["users"]["following_issues"].find_one({"_id": issue_id})
        issue, ticker_list = issue_meta["issue"], issue_meta["tickers"]
        following_user_id_list = issue_meta.get("following_users", [])
        
        # 設定搜尋文件的時間範圍（預設為從當前回顧N日）
        if end_timestamp is None:
            end_timestamp = datetime.now()
        
        start_timestamp = end_timestamp - timedelta(days=max_days)
        logging.info(f"[SERVER][issue_review][{issue}][{datetime2str(start_timestamp)} ~ {datetime2str(end_timestamp)}]")
        
        # 取得包含此issue id並且issue_content不為空的stock report
        market_stock_report_meta_list = list(self.MDB_client["preprocessed_content"]["stock_report"].find(
            {
                "issue_summaries": {
                    "$elemMatch": {
                        "issue_id": issue_id,
                        "issue_content": {"$ne": ''}
                    }
                },
                "data_timestamp": {"$gte": start_timestamp, "$lte": end_timestamp}
            },
            projection={"issue_summaries": {"$elemMatch": {"issue_id": issue_id, "issue_content": {"$ne": ""}}}, "data_timestamp": 1, "source": 1},
        ).limit(max_doc_num))
        
        # 取得參考的report id以留存紀錄
        ref_report_id_list = [
            report_meta["_id"] for report_meta in market_stock_report_meta_list
        ]
        
        # 取得搜尋時間範圍內的新聞摘要（依照ticker_list搜尋）
        market_info_meta_list = self.get_stock_news_summary(ticker_list, start_timestamp, end_timestamp)
        
        # 預先定義變數，避免後續出現未定義的情況        
        added_report_meta_list = []
        added_issue_review_text = ''
        issue_review_change_text = ''
        
        # 與前一次的issue_review，進行比較，判斷是否需要進行更新
        last_issue_review_meta = self.MDB_client["published_content"]["issue_review"].find_one({"issue_id": issue_id}, sort=[("upload_timestamp", DESCENDING)])
        
        # 如果有找到前次記錄，提取參考報告的 _id 列表；否則，初始化為空列表
        last_ref_report_id_list = last_issue_review_meta.get("ref_report_id", []) if last_issue_review_meta else []
        # 計算新增的報告 id 列表
        added_report_id_list = list(set(ref_report_id_list) - set(last_ref_report_id_list))
        
        logging.info(f"[SERVER][issue_review][{issue}] 共有{len(market_stock_report_meta_list)}篇參考報告，較前次新增{len(added_report_id_list)}篇")
        # 檢查新增的參考報告數量是否足夠
        if len(added_report_id_list) < min_report_num:
            logging.info(f"[SERVER][issue_review][{issue}][新增報告數量小於{min_report_num}篇，本次不進行更新]")
            return
        
        # 生成市場觀點總結（issue_review）
        market_issue_review_text = self._get_issue_review_text_LLM(issue, market_stock_report_meta_list, market_info_meta_list)
        
        # 依照市場觀點總結，列出市場共識與差異點
        market_issue_concensus_and_dissensus_text = _get_issue_concensus_and_dissensus_LLM(issue, market_issue_review_text)
        
        # 若此前已有issue_review，則進行以下，否則略過此部分
        if last_issue_review_meta:
            # 從參考報告中取出新增的報告，不要再次從MongoDB中取出（避免重複查詢 且 先前的pymongo運算子已進行過議題篩選）
            added_report_meta_list = [report_meta for report_meta in market_stock_report_meta_list if report_meta["_id"] in added_report_id_list]
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
            # 4o的token限制為128,000，因此需要將prompt進行截斷
            prompt = truncate_text_to_token_limit(prompt, 125000)
            
            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format="text")
        
        # 將自MongoDB取得的issue資料進行整理，改為適合GPT解析的格式，dict (source: content)
        def _generate_issue_json_string(stock_report_meta_list):
            # 將stock_report_meta_list按照data_timestamp進行排序（可使AI摘要的內容顯示由新到舊）
            stock_report_meta_list = sorted(stock_report_meta_list, key=lambda x: x["data_timestamp"], reverse=True)
            issue_content_dict = defaultdict(list)
            for stock_report_meta in stock_report_meta_list:
                issue_summary_meta_list = stock_report_meta.get("issue_summaries", [])
                # 先前已經使用pymongo的運算子過濾（僅剩下一個issue summary element），不需再次過濾
                issue_summary_meta = issue_summary_meta_list[0]
                issue_content = issue_summary_meta.get("issue_content", '')
                
                # 假设 source_trans_dict 和 keys_to_convert 是全局变量或类属性
                # 待改：應集中管理
                source_trans_dict = {"gs": "Goldman Sachs", 
                                     "jpm": "J.P. Morgan", 
                                     "citi": "Citi", 
                                     "barclays": "Barclays",
                                     "hti": "海通國際",
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
        
        market_info_content, filtered_market_info_content = '', ''
        if market_info_meta_list is not None:
            for info_meta in market_info_meta_list:
                market_info_content += datetime2str(info_meta["data_timestamp"])
                market_info_content += f"內容: {info_meta['content']}"
        
            filtered_market_info_content = _filter_market_info_LLM(market_info_content)
            
        issue_review_text = _create_issue_review_LLM(issue, issue_json_string, filtered_market_info_content)
        return issue_review_text
    
    # 取得搜尋時間範圍內的新聞（依照ticker_list搜尋）
    def get_stock_news_summary(self, ticker_list, start_timestamp, end_timestamp):
        stock_news_summary_meta_list = list(self.MDB_client["preprocessed_content"]["stock_news_summary"].find(
            {"ticker": {"$in": ticker_list},
            "data_timestamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
        ))
        return stock_news_summary_meta_list
    
    def get_stock_transcript_summary(self, ticker_list, start_timestamp, end_timestamp):
        summary = ''
        stock_transcript_meta_list = list(self.MDB_client["preprocessed_content"]["event_document"].find({"tickers": {"$in": ticker_list}, "data_timestamp": {"$gte": start_timestamp, "$lte": end_timestamp}}))
        for item_meta in stock_transcript_meta_list:
            summary += f"Title: {item_meta["title"]}\n\n"
            summary += f"全文摘要： {item_meta["text_summaries"]["full_text_summary"]} \n"
            summary += f"分析師問答： {item_meta["text_summaries"]["analyst_QA_summary"]} \n"
            summary += f"前期對比 {item_meta["text_summaries"].get("prior_period_comparison", '')} \n\n"
            
            summary += "相關議題段落\n\n"
            issue_summary_meta_list = item_meta["issue_summaries"]
            for issue_summary_meta in issue_summary_meta_list:
                # 過濾掉過短的段落
                if len(issue_summary_meta["issue_content"]) <= 10:
                    continue
                summary += f"議題： {issue_summary_meta["issue"]} \n"
                summary += f"相關段落： {issue_summary_meta["issue_content"]} \n\n"
            
        return summary
    
    def save_investment_assumption_review(self, assumption_id, max_days=90, min_days=14, end_timestamp=None):
        def _create_risk_score_LLM(investment_assumption, market_info_content, last_risk_score, last_assumption_review):
            prompt = (
                f"請針對投資假設「{investment_assumption}」，進行風險評分，要求如下：\n"
                f"1. 根據以下的資訊（議題分析 & 新聞 & 財報會議...），計算投資假設的risk_score（範圍：0～100）。\n"
                f"   - 計算標準：80分以上代表假設已接近被推翻；70-79分表示假設存在重大疑慮；60-69分為存在部分風險；59分以下表示假設維持良好。\n"
                f"2. 評分須參考前次風險評分與評分依據，若判斷本次風險高於前次，則risk score須上升，反之下降。\n"
                f"3. 須給出評分依據，寫成一篇評論文章（risk_assessment），其中須包含「評分理由、前次對比」兩大段落，內容須詳細\n"
                f"4. 不要包含開場白與結語，使用markdown語法，須翻譯為繁體中文。 \n\n"
                f"5. 以 JSON 格式回傳，risk_assessment的value須為text，不要內嵌dict\n\n"
                f"  - 格式範例：{{'risk_score': 60, 'risk_assessment': '###評分理由... ###前次對比 ...'}}\n\n"
                
                f"前次風險評分：{last_risk_score}\n"
                f"前次評分依據：{last_assumption_review}\n\n"
                f"市場資訊：「{market_info_content}」\n\n"
            )
            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format="json_object")
        
        def _create_risk_analysis_LLM(investment_assumption, market_info_content):
            prompt = (
                f"根據以下的市場資訊（議題分析 & 新聞 & 財報會議摘要...），針對投資假設「{investment_assumption}」進行分析，要求如下：\n"
                f"1. 撰寫一段詳細的分析文章，其中包含「總結」、「支持假設的資訊」、「反對假設的資訊」、三個段落，內容盡可能詳細，包含具體的數據\n"
                f"2. 分析內容須標明資訊來源與日期，新聞若缺乏來源可保留內容，標注來源為新聞即可\n"
                f"3. 使用markdown語法書寫，翻譯為繁體中文\n"
                f"4. 不要包含開場白與結語 \n\n"
                f"市場資訊：「{market_info_content}」"
            )
            
            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="o1-mini", output_format="text")
        
        # 製作並整理可以參考的市場資訊（待改：code很亂）
        def _organize_market_info_content(investment_assumption, issue_review_meta_list, stock_news_summary_meta_list, stock_transcript_summary):
            market_info_content = ''
            
            market_info_content += "\n 「議題分析」\n"
            for issue_review_meta in issue_review_meta_list:
                market_info_content += f"議題：\n{issue_review_meta['issue']}\n"
                market_info_content += f"議題總結：\n{issue_review_meta['issue_review']}\n\n"
            
            #新聞部分
            raw_news_summary = ''
            for news_meta in stock_news_summary_meta_list:
                raw_news_summary += f"日期：{datetime2str(news_meta['data_timestamp'])}\n"
                raw_news_summary += f"內容：{news_meta['content']}\n\n"
            
            prompt = (
                    f"從以下新聞資訊中篩選出所有與驗證或推翻「投資假設：{investment_assumption}」相關的新聞，保留每則新聞的完整內容，"
                    f"包括盡可能多的細節，不進行任何刪減，並標注新聞日期：\n"
                    f"{raw_news_summary}"
                    )
            
            filtered_stock_news_summary = call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o-mini", output_format="text")
            market_info_content += "\n ### 新聞資訊 ### \n"
            market_info_content += filtered_stock_news_summary
            
            market_info_content += "\n「財報會議摘要」\n"
            market_info_content += stock_transcript_summary
            
            return market_info_content
        
        # 取得投資假設資料
        investment_assumption_meta = self.MDB_client["users"]["investment_assumptions"].find_one({"_id": assumption_id})
        investment_assumption, ticker_list, following_user_id_list = investment_assumption_meta["assumption"], investment_assumption_meta["tickers"], investment_assumption_meta.get("following_users", [])
        
        # 設定搜尋文件的時間範圍（預設為從當前回顧N日）
        if end_timestamp is None:
            end_timestamp = datetime.now()
        
        start_timestamp = end_timestamp - timedelta(days=max_days)
        logging.info(f"[SERVER][assumption_review][{investment_assumption}][{datetime2str(start_timestamp)} ~ {datetime2str(end_timestamp)}]")
        
        # 依照時間間隔，判斷是否需要進行更新，並取得上一次的風險分數與評論，供後續評分參考
        last_assumption_review_meta = self.MDB_client["published_content"]["assumption_review"].find_one({"assumption_id": assumption_id}, sort=[("data_timestamp", -1)])
        if last_assumption_review_meta:
            last_risk_score = last_assumption_review_meta["risk_score"]
            last_assumption_review = last_assumption_review_meta["assumption_review"]
            last_review_timestamp = last_assumption_review_meta["data_timestamp"]
            
            # 若距離前次更新不足min_days日，則不進行更新（須先判斷大小，因有時可能會需要回溯歷史，此類情況不需要判斷間隔）
            if (end_timestamp > last_review_timestamp) and ((end_timestamp - last_review_timestamp).days < min_days):
                logging.info(f"[SERVER][assumption_review][{investment_assumption}] 距離前次更新不足{min_days}日，本次不進行更新")
                return
            
        else:
            last_risk_score = '前次未評分'
            last_assumption_review = '前次未評分'
         
        # 獲取與投資假設聯動的issue（預先設定）
        linked_issues_meta_list = investment_assumption_meta.get("linked_issues", [])
        # 查詢各issue最新的issue_review（優先取added_issue_review，若無則取market_issue_review）
        issue_review_meta_list = []
        for linked_issues_meta in linked_issues_meta_list:
            issue, issue_id = linked_issues_meta["issue"], linked_issues_meta["issue_id"]
            
            issue_review_meta = self.MDB_client["published_content"]["issue_review"].find_one({"issue_id": issue_id, 
                                                                                               "data_timestamp": {"$lte": end_timestamp, "$gte": start_timestamp}}, 
                                                                                              sort=[("data_timestamp", -1)],)
            # 若無issue review，則跳過
            if not issue_review_meta:
                continue
            
            if issue_review_meta.get("added_issue_review", ''):
                issue_review_text = issue_review_meta["added_issue_review"]
            
            elif issue_review_meta.get("market_issue_review", ''):
                issue_review_text = issue_review_meta["market_issue_review"]
            
            else:
                raise ValueError()
            issue_review_meta_list.append({"issue":issue, "issue_review":issue_review_text})
        
        # 取得搜尋時間範圍內的新聞（依照ticker_list搜尋）
        stock_news_summary_meta_list = self.get_stock_news_summary(ticker_list, start_timestamp, end_timestamp)
        
        # 取得搜尋時間範圍內的財報會議逐字稿摘要
        stock_transcript_summary = self.get_stock_transcript_summary(ticker_list, start_timestamp, end_timestamp)
        
        try:
            # 將參考資訊組合排版成市場資訊
            market_info_content = _organize_market_info_content(investment_assumption, issue_review_meta_list, stock_news_summary_meta_list, stock_transcript_summary)
            risk_assessment_json_text = _create_risk_score_LLM(investment_assumption, market_info_content, last_risk_score, last_assumption_review)
            risk_assessment_dict = json.loads(risk_assessment_json_text)
            assert ("risk_score" in risk_assessment_dict) and ("risk_assessment" in risk_assessment_dict)
            assumption_review_text = _create_risk_analysis_LLM(investment_assumption, market_info_content)
            
            risk_score = risk_assessment_dict["risk_score"]
            risk_assessment = risk_assessment_dict["risk_assessment"]
        
        except json.JSONDecodeError:
            logging.error(f"[SERVER][assumption_review][{assumption_id}]: JSON解析失敗")
            return
        
        # 存入MongoDB
        self.MDB_client["published_content"]["assumption_review"].insert_one(
            {
                "assumption_id": assumption_id,
                "investment_assumption": investment_assumption,
                "linked_issues": investment_assumption_meta.get("linked_issues", []), #待改：評估是否需要
                "data_timestamp": datetime(datetime.now().year, datetime.now().month, datetime.now().day),
                "upload_timestamp": datetime.now(timezone.utc),
                "risk_score": risk_score,
                "risk_assessment": risk_assessment,
                "assumption_review": assumption_review_text
            }
        )

        # 標註investment_assumption更新日期
        self.MDB_client["users"]["investment_assumption"].update_one({"_id": assumption_id}, {"$set": {"updated_timestamp": datetime.now(timezone.utc)}})
        
        # 寄送用戶通知
        if following_user_id_list:
            if risk_score >= 80:
                score_intepretation = "假設已接近被推翻"
            elif risk_score >= 70:
                score_intepretation = "假設存在重大疑慮"
            elif risk_score >= 60:
                score_intepretation = "假設存在部分風險"
            else:
                score_intepretation = "假設維持良好"
            
            notification_variables_dict = {
                "investment_assumption": investment_assumption,
                "risk_score": risk_score,
                "score_intepretation": score_intepretation,
                "page_url": f"/main/investment_assumption_review/{assumption_id}",
            }
            
            # 寄送用戶通知
            self.create_notification(user_id_list=following_user_id_list, 
                                            priority=3,
                                            notification_type="update", # "system"、"update"、"todo" 或 "alert"。
                                            notification_sub_type="investment_assumption_review_update",
                                            variables_dict=notification_variables_dict)
        
    def recognize_stock_report_ticker(self):
        def recognize_stock_report_ticker_LLM(text):
            prompt = ("請根據以下報告內容分析，這篇報告與哪些美股上市公司有關，回傳股票代號，要求如下：\n"
                        "1. 股票代號最多不超過3個。\n"
                        "2. 回傳順序依照相關性高低排序。\n"
                        "3. 以json格式回傳，範例如下：。\n"
                        "tickers: ['AAPL', 'GOOGL', 'TSLA']\n\n"
                    )
            
            prompt += f"報告內容: {text}\n\n"
            json_string = call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o-mini", output_format="json_object")
            ticker_list = json.loads(json_string).get("tickers", [])
            return ticker_list
        
        collection = self.MDB_client["raw_content"]["raw_stock_report_non_auto"]
        document_meta_list = list(collection.find({"is_processed": False, "is_deleted": False, "tickers": []}))

        logging.info(f"[SERVER] 共 {len(document_meta_list)} 則報告需要辨識ticker")
        for document_meta in document_meta_list:
            url = document_meta["url"]
            # 取得pdf內容
            report_text = get_pdf_text_from_url(url)
            report_text = clean_gibberish_text(report_text)
            # 透過LLM辨識股票代號
            ticker_list = recognize_stock_report_ticker_LLM(report_text)
            # 處理股票代號（減少歧義，例如BRK.B -> BRK, GOOGL -> GOOG）
            ticker_list = [ticker_disambiguation_dict.get(ticker, ticker) for ticker in ticker_list]
            logging.info(document_meta["title"])
            logging.info(ticker_list)
            # 保存tickers
            collection.update_one({"_id": document_meta["_id"]}, {"$set": {"tickers": ticker_list}})
    
    def save_stock_pick_review(self, end_timestamp=None, days=1):
        def create_stock_pick_review_LLM(report_text):
            prompt = (
                "以下是多篇報告的內容，請根據以下要求彙整為一篇投資建議總結：\n"
                "1. 彙整各篇報告中提及的個股投資建議，去除重複內容，並將相同個股的論述合併。\n"
                "2. 開頭以簡要摘要說明本次包含的推薦個股及其所屬產業，確保涵蓋所有提及的個股，但無需列出非推薦個股。\n"
                "3. 以繁體中文與 Markdown 語法撰寫。\n"
                "4. 總結中不需加入系統性結語。\n"
                "5. 每個個股建議為一個段落，段落結尾簡介該公司的基本資訊（兩句話）。\n"
                "6. 請確保使用以下格式返回，格式範例：\n"
                    "### 總結：\\n- 本次個股推薦包含AAPL, GOOG, SMCI 等AI相關科技類股，以及OXY, ET 等傳統能源類股...\n"
                    "### AAPL：建議賣出，短期缺乏催化劑，存在高估值風險\n"
                    "- 投資建議：（投資建議內容）\n"
                    "- 公司簡介：（公司基本資訊）\n\n"
                
                f"報告內容：\n{report_text}"
            )

            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format="text")

        collection = self.MDB_client['preprocessed_content']['stock_report']
        
        # 若未指定結束時間，則取得最後一次的報告時間，並設定為當天
        if end_timestamp is None:
            last_review_meta = self.MDB_client["published_content"]['stock_pick_review'].find_one(sort=[("data_timestamp", -1)])
            last_data_timestamp = last_review_meta["data_timestamp"]
            current_date_timestamp = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) #不使用.date()，會導致MDB儲存錯誤
            data_timestamp_list = pd.date_range(start=(last_data_timestamp+timedelta(days=1)), end=current_date_timestamp, freq='D').to_list()
        
        
        for end_timestamp in data_timestamp_list:
            start_timestamp = end_timestamp - timedelta(days=1)
            
            # 取得前一天的報告
            query = {"author_level": {"$gte": 3}, "data_timestamp": start_timestamp}
            document_meta_list = list(collection.find(query))
            logging.info(f"[SERVER][Stock Pick Review][{datetime2str(start_timestamp)}-{datetime2str(end_timestamp)}]  共{len(document_meta_list)}篇報告，嘗試進行彙整")
            
            # 若時間區段內報告數量不足，則略過
            if len(document_meta_list) == 0:
                logging.warning(f"[SERVER][Stock Pick Review][{datetime2str(end_timestamp)}] 報告數量不足，僅有{len(document_meta_list)}篇報告")
                continue
            
            report_text = ''
            recommendation_ticker_list = []
            for document_meta in document_meta_list:
                
                full_text_summary = document_meta["text_summaries"]["full_text_summary"]
                ticker_list = document_meta["tickers"]
                recommendation_ticker_list.append(ticker_list[0]) # 僅取第一個ticker（因預設為最相關，若每篇都全部取，會太多）
                tickers_string = ', '.join(ticker_list)
                title = document_meta["title"]
                
                report_text += f"# 報告標題: {title}"
                report_text += f"# 相關個股: {tickers_string}"
                report_text += full_text_summary

            stock_pick_review_text = create_stock_pick_review_LLM(report_text)
            recommendation_ticker_list = list(set(recommendation_ticker_list)) # 去除重複的ticker
            
            review_meta = {
                "data_timestamp": datetime(end_timestamp.year, end_timestamp.month, end_timestamp.day),
                "upload_timestamp": datetime.now(timezone.utc),
                "time_range": {"start": start_timestamp, "end": end_timestamp},
                "stock_pick_review": stock_pick_review_text,
                "tickers": recommendation_ticker_list,
                "ref_report_id": [document_meta["_id"] for document_meta in document_meta_list],
            }

            # 存入資料庫
            result = self.MDB_client["published_content"]['stock_pick_review'].insert_one(review_meta)
            
            # 寄送通知
            following_user_id_list = self.pool_list_db.get_active_user_id_list()
            # following_user_id_list = [ObjectId("66601790f20eb424a340acd3")]
            
            notification_variables_dict = {
                "date": datetime2str(end_timestamp),
                "tickers_str": ', '.join(recommendation_ticker_list),
                "page_url": f"/main/stock_pick_review/{str(result.inserted_id)}",
            }
            
            # 寄送通知
            self.create_notification(
                user_id_list = following_user_id_list,  # 單個用戶列表
                priority = 3,
                notification_type = "update",  # "system"、"update"、"todo" 或 "alert"
                notification_sub_type = "stock_pick_review_update",
                variables_dict = notification_variables_dict
            )

            
    # "system"、"update"、"todo" 或 "alert"。
    def create_notification(self, user_id_list, priority, notification_type, notification_sub_type, variables_dict=None):
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
    
    def create_ticker_event_notification(self, days=7):
        # 計算時間範圍
        start_timestamp = datetime.now(timezone.utc)
        end_timestamp = start_timestamp + timedelta(days=days)
        
        # 查詢活動元數據列表
        event_meta_list = self.pool_list_db.get_ticker_event_meta_list(start_timestamp=start_timestamp, end_timestamp=end_timestamp)
            
        # 提取不重複的標的代碼
        event_ticker_list = {event_meta["ticker"] for event_meta in event_meta_list}  # 使用 set() 提高效率
        event_tickers_str = ', '.join(sorted(event_ticker_list))

        # 測試：只寄送給特定用戶
        # user_id_list = [ObjectId("66601790f20eb424a340acd3")]
        user_id_list = self.pool_list_db.get_active_user_id_list()
    
        # 逐一處理每個用戶
        for user_id in user_id_list:
            following_ticker_list = set(self.pool_list_db.get_following_ticker_list(user_id))
            responsible_ticker_list = set(self.pool_list_db.get_responsible_ticker_list(user_id))

            # 計算交集，找出關注及負責的標的事件
            following_event_ticker_list = sorted(following_ticker_list & event_ticker_list)
            responsible_event_ticker_list = sorted(responsible_ticker_list & event_ticker_list)

            # 字符串處理
            following_event_tickers_str = ', '.join(following_event_ticker_list)
            responsible_event_tickers_str = ', '.join(responsible_event_ticker_list)

            # 準備通知模板的變數
            notification_variables_dict = {
                "start_date_str": datetime2str(start_timestamp),
                "end_date_str": datetime2str(end_timestamp),
                "event_tickers_str": event_tickers_str,
                "following_event_ticker_list": following_event_ticker_list,
                "following_event_tickers_str": following_event_tickers_str,
                "responsible_event_tickers_str": responsible_event_tickers_str,
                "event_meta_list": event_meta_list,
                "page_url": "/main/ticker_event_overview",
            }

            # 寄送通知
            self.create_notification(
                user_id_list=[user_id],  # 單個用戶列表
                priority=2,
                notification_type="alert",  # "system"、"update"、"todo" 或 "alert"
                notification_sub_type="ticker_event_alert",
                variables_dict=notification_variables_dict
            )
    
    def reclassify_industry_report(self):
        """
        重新分類行業報告，將可能為個股報告的報告轉移至raw_stock_report_auto，並進行標題調整
        """
        def _reclassify_industry_report_LLM(title, content_text):
            prompt = ("請分析以下文字，要求如下："
                "1. 判斷這是一篇「個股報告」或「行業報告」，給出其為針對單一個股研究的個股報告的可能性（0～1），越高代表越可能是個股報告。\n"
                "2. 列出與這篇報告最相關的公司的股票代號，台股代號格式使用（2330_TT），美股代號不要添加後綴\n"
                "3. 判斷該股票代號的所在地區（若為台灣為TW，美股為US，歐洲為EU，中國為CN，韓國為KW，日本為JP，若不在以上類別，皆歸類為US）\n"
                "3. 取一個新的標題，概要這篇報告的主題（標題所用的語言與待分析文字的語言一致）。\n"
                "4. 使用Json格式回傳，回傳格式如下：\n"
                    "{'is_stock_report': 0.7, 'relevant_ticker': 'AAPL', 'market': 'US', title': 'Apple AI表現差強人意，iphone升級意願不如預期'}\n"
                f"以下為待分析的文字：\n\n Title:{title} \n Content: {content_text}"
                )
            json_string = call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format="json_object")
            result_dict = json.loads(json_string)
            
            required_keys = ["is_stock_report", "relevant_ticker", "title", "market"]
            for key in required_keys:
                assert key in result_dict, f"API回傳格式錯誤: 缺少 {key}"
            
            return result_dict

        doc_meta_list = list(self.MDB_client["raw_content"]["raw_industry_report"].find({"is_deleted": False, "processed_info.is_confirmed": {"$ne": True}}, 
                                                                                sort=[("data_timestamp", -1)]).limit(1000))

        logging.info(f"【SERVER】共{len(doc_meta_list)}則行業報告待重新分類")
        for doc_meta in doc_meta_list:
            title = doc_meta["title"]
            text = get_pdf_text_from_url(url=doc_meta["url"])
            text = clean_gibberish_text(text)
            text = truncate_text_to_token_limit(text, 500)
            
            result_dict = _reclassify_industry_report_LLM(doc_meta["title"], text)
            
            # 標註為已由LLM確認
            doc_meta["processed_info"]["is_confirmed"] = True
                
            # 若為個股報告的可能性大於0.75，則判斷為個股報告（加入ticker後轉移collection位置）
            if result_dict["is_stock_report"] >= 0.85:
                print(title)
                print(result_dict)
                doc_meta["tickers"].append(result_dict["relevant_ticker"])
                doc_meta["market"] = result_dict["market"]
                self.MDB_client["raw_content"]["raw_stock_report_auto"].insert_one(doc_meta)
                self.MDB_client["raw_content"]["raw_industry_report"].delete_one({"_id": doc_meta["_id"]})
            
            else:
                #print("Industry")
                # 若原標題過於簡短，則使用LLM產生的新標題
                if len(title) <= 20:
                    doc_meta["title"] = result_dict["title"]
                
                self.MDB_client["raw_content"]["raw_industry_report"].update_one({"_id": doc_meta["_id"]}, {"$set": doc_meta})
    
    def summarize_industry_report(self):
        def _create_industry_report_summary_LLM(text):
            prompt = ("請整理以下的行業研究報告內容，格式與要求如下：\n"
                        "1. 依主題分段摘要，每段以一句話作為標題概括內容，例如：「半導體製程：Nanosheet採全包覆式技術突破FinFET瓶頸」。\n"
                        "2. 每段以Markdown格式呈現，重點以條列式描述，避免過多連續文字描述。\n"
                        "3. 每段文字內容需超過 200 字，並確保涵蓋關鍵數字與細節。\n"
                        "4. 報告中不要添加開場白與結語，直接進入主題，且不要包含全文標題。\n"
                        f"報告內容：\n\n {text}"
                    )
            raw_text = call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o-mini", output_format="text")
            # 清除LLM可能包含的markdown標記（會導致顯示錯誤）
            cleaned_markdown_text = re.sub(r"```(?:markdown)?", "", raw_text)
            return cleaned_markdown_text

        src_collection = self.MDB_client["raw_content"]["raw_industry_report"]
        des_collection = self.MDB_client["preprocessed_content"]["industry_report"]

        doc_meta_list = list(src_collection.find({"is_deleted": False,
                                                "processed_info.is_summary_extracted": False,
                                                }, sort=[("data_timestamp", -1)]).limit(800))
        for doc_meta in doc_meta_list:
            title, url = doc_meta["title"], doc_meta["url"]
            logging.info(f"【SERVER】【Industry Report】【Summary】處理行業報告：{title}")
            report_text = get_pdf_text_from_url(url)
            # 限制token數量，避免超過LLM限制導致報錯
            report_text = truncate_text_to_token_limit(report_text, 128000)
            full_text_summary = _create_industry_report_summary_LLM(report_text)
            text_summaries_dict = { 
                "text_summaries": {
                            "full_text_summary": full_text_summary,
                }
            }
            doc_meta.update(text_summaries_dict)
            
            doc_meta["processed_info"]["is_summary_extracted"] = True
            full_text_summary_for_embedding = truncate_text_to_token_limit(full_text_summary, 8000)
            doc_meta["embedding"] = get_embedding(full_text_summary_for_embedding)
            
            des_collection.insert_one(doc_meta)
            src_collection.update_one({"_id": doc_meta["_id"]},  # Filter to locate the document
                                    {"$set": {"processed_info.is_summary_extracted": True}}  # Update operation
                                    )
    
    def delete_duplicated_docs(self, doc_type, gcs_bucket_name="investment_report", days_interval=10, size_threshold=0.01, time_diff_days=3):
        """
        找出指定集合類型中可能重複的文檔。
        
        :param doc_type: str, 指定集合類型 ("industry_report", "stock_report", "stock_memo")
        :param gcs_bucket_name: str, GCS 存儲桶名稱，默認為 "investment_report"
        :param credential_path: str, Google Cloud Storage 認證文件路徑
        :param days_interval: int, 查詢日期範圍（默認10天）
        :param size_threshold: float, Blob 大小差異閾值（默認 1%）
        :param time_diff_days: int, 判定重複的時間差閾值（默認3天）
        :return: list, 包含可能重複文檔的元數據
        """
        
        # 待改：初始化 GCS 客戶端
        credential_path="/Users/yahoo168/Desktop/GOOGLE_APPLICATION_CREDENTIALS.json"
        gcs_client = GoogleCloudStorageTools(credential_file_path=credential_path)
        
        # 根據 doc_type 選擇集合
        collection_map = {
            "industry_report": self.MDB_client["raw_content"]["raw_industry_report"],
            "stock_report": self.MDB_client["raw_content"]["raw_stock_report_auto"],
            "stock_memo": self.MDB_client["raw_content"]["raw_stock_memo"]
        }
        if doc_type not in collection_map:
            raise ValueError("Invalid collection type. Choose from: 'industry_report', 'stock_report', 'stock_memo'.")
        
        collection = collection_map[doc_type]
        # 計算時間範圍
        end_timestamp = datetime.now()
        start_timestamp = end_timestamp - timedelta(days=days_interval)

        # 聚合管道
        pipeline = [
            {
                "$match": {
                    "data_timestamp": {
                        "$gte": start_timestamp,
                    },
                    "is_deleted": {"$ne": True}
                }
            },
            {
                "$group": {
                    "_id": {
                        "tickers": "$tickers",
                        "source": "$source"
                    },
                    "docs": {
                        "$push": {
                            "_id": "$_id",
                            "data_timestamp": "$data_timestamp",
                            "tickers": "$tickers",
                            "source": "$source",
                            "blob_name": "$upload_info.blob_name",
                        }
                    }
                }
            }
        ]
        
        # 執行聚合管道
        grouped_docs = list(collection.aggregate(pipeline))
        
        # 結果文檔集合
        duplicated_doc_meta_list = []

        # 遍歷每個分組
        for group in grouped_docs:
            docs = group["docs"]
            docs.sort(key=lambda x: x["data_timestamp"])  # 根據 data_timestamp 排序
            
            # 遍歷排序後的文檔，找出 data_timestamp 在指定天數內的文檔
            for i in range(len(docs) - 1):
                doc1 = docs[i]
                doc2 = docs[i + 1]
                timestamp1 = doc1["data_timestamp"]
                timestamp2 = doc2["data_timestamp"]

                # 判斷時間差
                if abs((timestamp1 - timestamp2).days) <= time_diff_days:
                    # 使用 GCS 客戶端取得 blob 大小
                    blob_size1 = gcs_client.get_blob(gcs_bucket_name, doc1["blob_name"]).size
                    blob_size2 = gcs_client.get_blob(gcs_bucket_name, doc2["blob_name"]).size

                    # 判斷 blob 大小差異
                    if blob_size1 and blob_size2:
                        size_difference = abs(blob_size1 - blob_size2) / max(blob_size1, blob_size2)
                        if size_difference <= size_threshold:
                            print(doc2["tickers"], doc2["source"])
                            print(doc2["data_timestamp"], doc2["blob_name"])
                            # 只保留 timestamp 較後的文檔
                            duplicated_doc_meta_list.append({
                                "duplicate_doc_id": doc2["_id"],
                                "duplicate_blob_name": doc2["blob_name"]
                            })
        
        duplicate_doc_id_list = [doc["duplicate_doc_id"] for doc in duplicated_doc_meta_list]
        collection.update_many({"_id": {"$in": duplicate_doc_id_list}}, {"$set": {"is_deleted": True}})
        # return duplicated_doc_meta_list
        

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
    
        # 根據與特定個股相關的報告，製作看多/看空的論點摘要，可設定最大報告數量
    # def save_stock_report_review(self, ticker, review_report_nums=10):
    #     def _create_raw_stock_report_review(summary_list, ticker, output_format="json_object"):
    #         prompt = (
    #             f"以下是一些「股票代號為{ticker}的公司」近期的多篇研究報告，"
    #             f"挑選出其中的bullish_outlook以及bearish_outlook進行整理，2個面向各約300字，不包含對目標價的預測。"
    #             f"將回傳值以 JSON 格式提供，其中包含以下2️個key:'bullish_outlook'以及'bearish_outlook'。"
    #             f"除了上述的部分外，不要有任何其他內容\n\n"
    #             f"研究報告內容:\n{'\n'.join(summary_list)}\n"
    #         )            
    #         return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format=output_format)

    #     def _adjust_stock_report_review_format(summary_text, ticker, output_format="text"):
    #         prompt = (f"以下是一些「股票代號為{ticker}的公司」近期的研究結論，"
    #                 f"以條列式的方式，整理其中的論點，每個論點以2~3句話表示內容，論點數量不高於10個。")
    #         prompt += f"回傳格式：每個論點以換行符('\n')分隔，論點前面不需要數字編號"
    #         prompt += f"除了上述的部分外，不要有任何其他內容\n\n"
    #         prompt += f"研究報告內容:\n{'\n'.join(summary_text)}\n"
    #         return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format=output_format)
             
    #     # 將json格式的看多/看空論點解析成list
    #     def _parse_outlook_argument(argument_text):
    #         argument_list = argument_text.split("\n")
    #         argument_list = [argument for argument in argument_list if argument]
    #         return argument_list
        
    #     def _compare_outlook_argument(old_argument_list, new_argument_list):
    #         old_argument, new_argument = '\n'.join(old_argument_list), '\n'.join(new_argument_list)
    #         prompt =  f"以下是投資機構針對{ticker}的新、舊觀點，請針對新增與減少的部分寫成一篇約150字的短文。"
    #         prompt += f"回傳格式：以markdown語法顯示，段落標題為「新增的部分」與「減少的部分」，格式使用### \n"
    #         prompt += f"舊的論點: {old_argument}\n\n"
    #         prompt += f"新的論點: {new_argument}\n\n"
    #         return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format="text")
            
    #     # 取得自上次總結後，新上傳報告的meta data，預設為最新的10篇
    #     # 查找stock_report_review中data_timestamp字段的最大值
    #     last_reviews_doc = self.MDB_client["published_content"]["stock_report_review"].find_one(sort=[("data_timestamp", -1)])

    #     last_processed_timestamp = None
    #     if last_reviews_doc:
    #         last_processed_timestamp = last_reviews_doc["data_timestamp"]
            
    #     stock_report_meta_list = list(self.MDB_client["preprocessed_content"]["stock_report"].find({"ticker": ticker, "processed_timestamp": {"$gt": last_processed_timestamp}})
    #                                 .sort([("processed_timestamp", -1)]).limit(review_report_nums))
        
    #     # 若無新的報告，則不進行總結
    #     if len(stock_report_meta_list) == 0:
    #         return
    #     logging.info(f"[SERVER][Data Process][{ticker}][stock_report_review]")
    #     # 逐篇進行摘要，取得看多/看空的論點（json格式）
    #     summary_list = [stock_report_meta["summary"] for stock_report_meta in stock_report_meta_list]
            
    #     # 調用LLM，取得看多/看空的論點（json格式）
    #     stock_report_review_json_text = _create_raw_stock_report_review(summary_list, ticker, output_format="json_object")
    #     stock_report_review = json.loads(stock_report_review_json_text)
    #     # 取得看多/看空的論點(dict)
    #     bullish_outlook_raw_text, bearish_outlook_raw_text = stock_report_review["bullish_outlook"], stock_report_review["bearish_outlook"]
    #     # 調整看多論點格式，原先為整段文字，改為條列式的論點列表
    #     bullish_argument_text = _adjust_stock_report_review_format(summary_text=bullish_outlook_raw_text, ticker=ticker, output_format="text")
    #     bullish_outlook_argument_list = _parse_outlook_argument(argument_text=bullish_argument_text)
    #     # 調整看空論點格式，原先為整段文字，改為條列式的論點列表
    #     bearish_argument_text = _adjust_stock_report_review_format(summary_text=bearish_outlook_raw_text, ticker=ticker, output_format="text")
    #     bearish_outlook_argument_list = _parse_outlook_argument(argument_text=bearish_argument_text)
    #     # 取得舊的看多/看空論點，進行比較
    #     old_stock_report_review_meta = self.MDB_client["published_content"]["stock_report_review"].find_one({"ticker": ticker}, sort=[("date", -1)])
        
    #     if old_stock_report_review_meta:
    #         old_bullish_argument_list = old_stock_report_review_meta["stock_report_review"]["bullish_outlook"]
    #         old_bearish_argument_list = old_stock_report_review_meta["stock_report_review"]["bearish_outlook"]
            
    #         bullish_outlook_diff = _compare_outlook_argument(old_argument_list=old_bullish_argument_list, new_argument_list=bullish_outlook_argument_list)
    #         bearish_outlook_diff = _compare_outlook_argument(old_argument_list=old_bearish_argument_list, new_argument_list=bearish_outlook_argument_list)
    #     # 若無舊的看多/看空論點，則不進行比較，填入空字串
    #     else:
    #         bullish_outlook_diff, bearish_outlook_diff = "", ""
            
    #     # 存入MongoDB
    #     stock_report_review_meta = {
    #         "ticker": ticker,
    #         "data_timestamp": datetime(datetime.now().year, datetime.now().month, datetime.now().day),
    #         "upload_timestamp": datetime.now(),
    #         "stock_report_review": {
    #             "bullish_outlook": bullish_outlook_argument_list,
    #             "bearish_outlook": bearish_outlook_argument_list,
    #             "bullish_outlook_diff": bullish_outlook_diff,
    #             "bearish_outlook_diff": bearish_outlook_diff,
    #         },
    #     }
    #     self.MDB_client["published_content"]["stock_report_review"].insert_one(stock_report_review_meta)