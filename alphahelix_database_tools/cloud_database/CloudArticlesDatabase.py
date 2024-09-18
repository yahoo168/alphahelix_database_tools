from collections import defaultdict
from datetime import datetime
import json

from pymongo import DESCENDING
from jinja2 import Template #type: ignore

# from .config.notification_template import _all_notification_template_dict
# 避免採用相對導入，在雲端伺服器容易報錯
from alphahelix_database_tools.cloud_database.config.notification_template import _all_notification_template_dict

from alphahelix_database_tools.external_tools.news_tools import *
from alphahelix_database_tools.external_tools.openai_tools import call_OpenAI_API
from alphahelix_database_tools.external_tools.pdf_tools import get_paragraph_list_from_pdf
from alphahelix_database_tools.external_tools.google_tools import *

from alphahelix_database_tools.utils.format_utils import standardize_dict, standardize_key
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

class CloudArticlesDatabase(AbstractCloudDatabase):
    def __init__(self, config_folder_path=None):
        super().__init__(config_folder_path=config_folder_path)  #調用AbstractCloudDatabase 的__init__方法
        self.rapid_API_key = "5eeaf20b6dmsh06b146a0f8df7d6p1fb4c8jsnb19977dfeebf"
        self.news_API_key = "3969806328c5462ebc86dfe94acecd9c"
        self.gmail_API_servie = None
        self.OpenAI_API_key = "sk-svcacct-pVzSoilISNh877cqHQ4QT3BlbkFJf2eM2pjuAnwzAOrNFvCL"
        self.MDB_client = MongoClient(self.cluster_uri_dict["articles"], server_api=ServerApi('1'))
        
    def save_stock_news(self, ticker_list, start_date=None):
        # MongoDB返回的datetime不帶時區資訊，此處進行轉換，以便與新聞utc對接
        if start_date == None:
            start_date = self.get_latest_data_date(item="raw_stock_news")
        if isinstance(start_date, str):
            start_date = str2datetime(start_date)
                
        df_list = list()
        for ticker in ticker_list:
            logging.info(f"[SAVE][stock_news][{ticker}][{datetime2str(start_date)}~{TODAY_DATE_STR}]")
            # 從seekingalpha下載stock news
            logging.info(f"[SAVE][stock_news][{ticker}][source: seekingalpha]")            
            _df = get_stock_news_from_seekingalpha(API_key=self.rapid_API_key, ticker=ticker)
            df_list.append(_df)
            
            # 從News API下載stock news
            logging.info(f"[SAVE][stock_news][{ticker}][source: News API]")
            _df = get_stock_news_from_news_API(API_key=self.news_API_key, ticker=ticker)
            df_list.append(_df)
        
        df = pd.concat(df_list).sort_values(by="date")
        df = self._group_news_df_by_ticker(df)
        df = df[df["date"] >= start_date].reset_index(drop=True)
    
        data_list = df.to_dict("records")
        self.save_data_to_MDB(item="raw_stock_news", data_list=data_list, upsert=True, key="url")

    def save_stock_reports_auto(self, ticker_list, start_date=None):
        if start_date == None:
            # MongoDB返回的datetime不帶時區資訊，此處進行轉換，以便與新聞utc對接
            start_date = self.get_latest_data_date(item="raw_stock_report_auto")
        if isinstance(start_date, str):
            start_date = str2datetime(start_date)

        df_list = list()
        for ticker in ticker_list:
            logging.info(f"[SAVE][stock_report_auto][{ticker}][{datetime2str(start_date)}~{TODAY_DATE_STR}]")
            # 從seekingalpha下載stock report
            logging.info(f"[SAVE][stock_report_auto][{ticker}][source: seekingalpha]")
            # 不該直接傳入start_date，因個別標的最新的報告收錄時間，不適用於其他標的
            _df = get_stock_report_from_seekingalpha(API_key=self.rapid_API_key, ticker=ticker)
            df_list.append(_df)
        
        df = pd.concat(df_list).sort_values(by="date")
        df = self._group_news_df_by_ticker(df)
        df = df[df["date"] >= start_date].reset_index(drop=True)
    
        data_list = df.to_dict("records")
        self.save_data_to_MDB(item="raw_stock_report_auto", data_list=data_list, upsert=True, key="url")

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
    
    def get_raw_stock_news_df(self, ticker, start_date):
        all_news_df = self.get_item_df(item="raw_stock_news", method="by_date", start_date=start_date)
        # 篩選出包含此ticker的news
        news_df = all_news_df[all_news_df["ticker"].apply(lambda x: ticker in x)]
        # 因資料庫中可能存在重複的新聞，故刪除
        news_df = news_df.drop_duplicates("url")
        news_df = news_df.loc[:, ["title", "url", "source"]]
        news_df = news_df.reset_index()
        return news_df

    def save_shorts_summary(self, ticker_list, end_date=datetime.now(), period=1, words_per_shorts=8):
        def _call_shorts_summary_LLM(shorts_content_list, ticker, word_number):
            prompt = (
                      f"以下是一些可能與「股票代號為{ticker}的公司」有關的新聞消息，"
                      f"請挑選其中與公司相關性較高的消息，組合成一篇中文新聞，切勿包含任何不在消息中的內容"
                      f"並在各個段落用標題敘述段落重點，不含標題的總字數約{word_number}字，標題使用markdown語法中的###表示\n"
                    )
            
            prompt += "\n".join(shorts_content_list)
            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format="text")
        
        # 若不預設區間，則固定取最新一日內的raw shorts
        def get_raw_shorts_df(ticker, start_date=None, end_date=datetime.now()):
            if start_date == None:
                start_date = end_date - timedelta(days=1)

            all_shorts_df = self.get_item_df(item="raw_shorts", method="by_date", start_date=start_date, end_date=end_date)
            # 篩選出包含此ticker的shorts
            shorts_df = all_shorts_df[all_shorts_df["ticker"].apply(lambda x: ticker in x)]
            shorts_df = shorts_df.loc[:, ["title", "source"]]
            shorts_df = shorts_df.reset_index()
            return shorts_df

        start_date = end_date - timedelta(days=period)
        logging.info(f"[SHORTS][SUMMARY]{datetime2str(start_date)}-{datetime2str(end_date)}")
        data_list = list()
        for ticker in ticker_list:        
            raw_shorts_df = get_raw_shorts_df(ticker, start_date, end_date)
            num_of_raw_shorts = len(raw_shorts_df)
            
            if num_of_raw_shorts==0:
                logging.info(f"[GPT][shorts_summary][{ticker}][no shorts exist in the selected period]")
                continue
            # 設定字數上限，避免因新聞過多導致摘要過長
            summary_word_num = min(num_of_raw_shorts * words_per_shorts, 800)
            logging.info(f"[GPT][shorts_summary][{ticker}] 共{num_of_raw_shorts}則shorts，預計摘要字數約{summary_word_num}")
            # 待改：原始欄位應該叫content
            shorts_content_list = list(raw_shorts_df["title"])
            # 每則新聞以8個字摘要（因含無關的新聞，實際約為10字）
            shorts_summary = _call_shorts_summary_LLM(shorts_content_list, ticker=ticker, word_number= words_per_shorts*num_of_raw_shorts)
            
            data_list.append(
                {
                 "data_timestamp": datetime(end_date.year, end_date.month, end_date.day),
                 "ticker": ticker,
                 "shorts_summary": shorts_summary
                }
            )
        if len(data_list) > 0:
            self.save_data_to_MDB(item="shorts_summary", data_list=data_list)
        
    # 針對特定issue，找出尚未針對此issue進行摘要的document meta list（以ticker搜索），可限定最長回顧天數與最大報告數量
    def _get_unextracted_documents_of_certain_issue(self, ticker, issue_id, max_days=90, max_doc_num=10):
        # 針對ticker搜索相關的文件（目前僅有stock report）
        document_meta_list = list(self.MDB_client["preprocessed_content"]["stock_report"]
                                    .find({"ticker": ticker, 
                                            "data_timestamp": {"$gte": datetime.now() - timedelta(max_days=max_days)}
                                        },
                                        {"url":1, "issue_summary.issue_id": 1, "data_timestamp": 1})  # projection
                                    .sort("data_timestamp", DESCENDING)
                                    .limit(max_doc_num))

        # for-loop目前已經摘要過的issue，過濾掉已經有此issue的document
        result_document_meta_list = [
            document_meta for document_meta in document_meta_list
            if not any(issue["issue_id"] == issue_id for issue in document_meta.get("issue_summary", []))
        ]

        return result_document_meta_list
    
    # 針對特定issue，在一定的天數內，找出尚未針對此issue進行摘要的document並進行摘要（以ticker搜索），
    def extract_documents_of_certain_issue(self, ticker, issue_id, max_days=90, max_doc_num=30):
        # 依照issue_id，取得issue的meta資料
        issue_meta = self.MDB_client["users"]["following_issues"].find_one({"_id": issue_id})
        # 取得issue名稱（即issue）
        issue = issue_meta["issue"]
        # 找出尚未針對此issue進行摘要的document
        document_meta_list = self._get_unextracted_documents_of_certain_issue(ticker=ticker, issue_id=issue_id, max_days=max_days, max_doc_num=max_doc_num)
        logging.info(f"[SERVER][ISSUE] Found {len(document_meta_list)} unsummarized documents for issue {issue} of {ticker}")
        for index, document_meta in enumerate(document_meta_list):
            logging.info(f"[SERVER][LLM] Summarizing({index}). Title: {document_meta['title']}")
            report_id, pdf_file_url = document_meta["_id"], document_meta["url"]
            
            # 針對單篇document與issue進行摘要，取得issue content（關閉全文摘要，僅針對issue摘要）
            result_data_dict = self._extract_info_from_document(ticker, pdf_file_url, issue_list=[issue], 
                                                                extract_summary=False, extract_issue_summary=True)
            
            # 將issue content存入MongoDB（在原document的issue_summary中添加issue_content）
            issue_meta = {
                "issue_id": issue_id,
                "issue": issue,
                "issue_content": result_data_dict["issue_content_dict"],
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
                    f"依據以下不同的'issue'，各自挑選出相關的段落（盡量完整），整合成短文後翻譯為「繁體中文」，各個段落短文的字數約{word_number}字。")
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

            # 取出該標的的追蹤issue（預設最多10個）
            following_issues_cursor = self.MDB_client["users"]["following_issues"].find({"tickers": ticker}, limit=10)
            stock_following_issue_meta_list = list(following_issues_cursor)

            # 建立issue與issue_id的對應字典
            issue_id_mapping = {item['issue']: item['_id'] for item in stock_following_issue_meta_list}
            stock_following_issue_list = list(issue_id_mapping.keys())

            # 進行文件處理（全文摘要 / 追蹤問題摘要）
            extracted_info_data_dict = self._extract_info_from_document(
                ticker=ticker, 
                pdf_file_url=pdf_file_url, 
                issue_list=stock_following_issue_list, 
                extract_summary=True, 
                extract_issue_summary=True
            )

            # 格式化issue摘要並加入issue_id
            issue_summary_list = [
                {
                    "issue": issue,
                    "issue_id": issue_id_mapping.get(issue, None),  # 通过字典获取issue_id，若不存在则为None
                    "issue_content": issue_content
                }
                for issue, issue_content in extracted_info_data_dict["issue_content_dict"].items()
            ]

            # 標識為已處理
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
            following_user_id_list = self.MDB_client["pool_list"]["ticker_info"].find_one({"ticker": ticker}, {"following_users": 1, "_id": 0})["following_users"]

            # 用於render通知模板的變數字典
            variables_dict = {
                    "ticker": ticker,
                    # _external=True 参数确保生成的是绝对 URL
                    "report_page_url": f"/main/report_summary_page/{report_id}",
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
    
    def _save_issue_review(self, issue_id, max_days=90, max_doc_num=30):
        def _get_issue_concensus_and_dissensus_LLM(issue_review_text, output_format="text"):
            prompt =  f"根據以下的市場看法，列出共識與差異點\n"
            prompt += f"不要使用Markdown語法會出現的符號，純文字即可。不要有其他內容，如系統回覆、問候語等。\n"
            prompt += f"市場看法: {issue_review_text}\n"
            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format=output_format)

        # 調用LLM，將新的問題總結與舊的問題總結進行比較，找出差異
        def _get_issue_review_change_text_LLM(new_issue_review, old_issue_review, output_format="text"):
            prompt =  f"以下是針對相同議題的兩篇論述，請比較「新的論述」的內容，是否存在「舊的論述」沒有提及的部分，將新增的論點整合為短文\n"
            prompt += f"不要使用Markdown語法會出現的符號，純文字即可。不要有其他內容，如系統回覆、問候語等。\n"
            prompt += f"新的論述: {new_issue_review}\n"
            prompt += f"舊的論述: {old_issue_review}\n"
            
            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format=output_format)
                # 針對每個問題，調用LLM，生成問題總結
        
        issue_meta = self.MDB_client["users"]["following_issues"].find_one({"_id": issue_id})
        issue = issue_meta["issue"]
        logging.info(f"[SERVER][issue_review][{issue}]]")
        
        # 找出包含這個issue的stock report
        market_stock_report_meta_list = list(self.MDB_client["preprocessed_content"]["stock_report"].find(
            {"issue_summary.issue_id": issue_id,
            "data_timestamp": {"$gte": datetime.now() - timedelta(days=max_days)}
            },
            projection={"issue_summary": {"$elemMatch": {"issue_id": issue_id}}, "data_timestamp": 1, "source": 1}
        ).limit(max_doc_num))
        
        # 取得參考報告的_id以留存
        ref_report_id_list = [
            report_meta["_id"] for report_meta in market_stock_report_meta_list
        ]
        # 生成問題總結（issue_review），並進行共識與差異點的分析
        market_issue_review_text = self._get_issue_review_text_LLM(issue_meta, market_stock_report_meta_list)
        market_issue_concensus_and_dissensus_text = _get_issue_concensus_and_dissensus_LLM(market_issue_review_text)
        
        # 預先定義變數，避免後續出現未定義的情況
        last_ref_report_id_list = []
        added_report_id_list = []
        added_report_meta_list = []
        added_issue_review_text = ''
        issue_review_change_text = ''
        
        # 取得上一次的問題總結（issue_review），以進行比較  
        last_issue_review_meta = self.MDB_client["published_content"]["issue_review"].find_one({"issue_id": issue_id}, sort=[("upload_timestamp", DESCENDING)])
        if last_issue_review_meta:
            # 取得上一次的參考報告的_id
            last_ref_report_id_list = last_issue_review_meta["ref_report_id"]
            # 找出新增的報告id
            added_report_id_list = list(set(ref_report_id_list) - set(last_ref_report_id_list))
            if added_report_id_list:
                added_report_meta_list = list(self.MDB_client["preprocessed_content"]["stock_report"].find({"_id": {"$in": added_report_id_list}}))
                added_issue_review_text = self._get_issue_review_text_LLM(issue, added_report_meta_list)
                issue_review_change_text = _get_issue_review_change_text_LLM(
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
    
    # 生成問題總結 (following issues review)，並儲存到資料庫（MongoDB）
    def _get_issue_review_text_LLM(self, issue, stock_report_meta_list):
        # 調用LLM，生成問題總結
        def _create_issue_review_LLM(issue, issue_content_json, output_format="text"):
            prompt =  f"以下是一些不同的報告來源，針對'{issue}'議題，近期的研究報告論點，請整合其內容，寫成一篇短文，追蹤市場這個issue的近期看法。\n"
            prompt += f"須註明看法的來源與日期，若沒有相關的論點，可返回''，不用另外搜尋，除了上述的部分外，不要有任何其他內容\n"
            prompt += f"回傳格式：不要使用markdown語法會出現的符號，使用純文字即可"
            prompt += f"研究報告內容:\n {issue_content_json}"
            return call_OpenAI_API(API_key=self.OpenAI_API_key, prompt=prompt, model_version="gpt-4o", output_format=output_format)
        
        # 將自MongoDB取得的issue資料進行整理，改為適合GPT解析的格式，dict (source: content)
        def _generate_issue_json_string(stock_report_meta_list):
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
        issue_review_text = _create_issue_review_LLM(issue, issue_json_string)
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

        template_dict = _all_notification_template_dict.get(notification_type, {}).get(notification_sub_type, '')    
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
                "upload_timestamp": datetime.now(),
                "is_read": False,
                "is_displayed": False,
                "meta_data": meta_data_dict
            }
            notifications.append(notification_meta)

        # Perform a bulk insert for efficiency
        if notifications:
            self.MDB_client["users"]["notifications"].insert_many(notifications)