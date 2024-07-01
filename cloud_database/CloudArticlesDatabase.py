import os, json, requests
from collections import defaultdict
from datetime import datetime

from alphahelix_database_tools.external_tools.news_tools import *
from alphahelix_database_tools.external_tools.openai_tools import call_OpenAI_API
from alphahelix_database_tools.external_tools.pdf_tools import get_cleaned_paragraph_list_from_pdf
from alphahelix_database_tools.external_tools.google_tools import *
from .AbstractCloudDatabase import *

# fitz（PyMuPDF）擷取PDF文字 &圖片

"""
to-do-list:
    news source:
        - reuter
        - industry news
    report:
        - non_auto: verification (today)
        
    processed:
        - build stock news
        - build shorts summary (today)
        - build stock_report

    add-on
        - 紀錄重要新聞的來源佔比
"""

"""
### 都是台幣
- shorts_summary: 0.1/ticker/日
"""

"""
在 MongoDB 中，datetime 对象默认是不带时区信息的（即offset-naive）。
当你将 Python 的 datetime 对象插入到 MongoDB 中时，如果该对象包含时区信息，
MongoDB 会将其转换为 UTC 时间并存储为一个没有时区偏移的时间戳。在查询时，返回的 datetime 对象也不会包含时区信息。
"""

class CloudArticlesDatabase(AbstractCloudDatabase):
    def __init__(self, config_folder_path):
        super().__init__(config_folder_path=config_folder_path)  # 調用父類 MDB_DATABASE 的__init__方法
        #self.polygon_API_key = "vzrcQO0aPAoOmk3s_WEAs4PjBz4VaWLj"
        self.rapid_API_key = "5eeaf20b6dmsh06b146a0f8df7d6p1fb4c8jsnb19977dfeebf"
        self.news_API_key = "3969806328c5462ebc86dfe94acecd9c"
        self.gmail_API_servie = None
        self.OpenAI_API_key = "sk-proj-GzvuIu7QRcMeXMxzpRcJT3BlbkFJXYcMxEWH6aiytV5woJOc"
        self.MDB_client = MongoClient(self.cluster_uri_dict["articles"], server_api=ServerApi('1'))
        
    def save_stock_news(self, ticker_list, start_date=None):
        if start_date == None:
            # MongoDB返回的datetime不帶時區資訊，此處進行轉換，以便與新聞utc對接
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

    # 若不預設區間，則固定取最新一日內的raw shorts
    def get_raw_shorts_df(self, ticker, start_date=None, end_date=datetime.now()):
        if start_date == None:
            start_date = end_date - timedelta(days=1)

        all_shorts_df = self.get_item_df(item="raw_shorts", method="by_date", start_date=start_date, end_date=end_date)
        # 篩選出包含此ticker的shorts
        shorts_df = all_shorts_df[all_shorts_df["ticker"].apply(lambda x: ticker in x)]
        shorts_df = shorts_df.loc[:, ["title", "source"]]
        shorts_df = shorts_df.reset_index()
        return shorts_df

    def get_stock_following_issue_meta_list(self, ticker):
        # 待改：這邊的是client是？
        issue_meta_list = list(self.MDB_client["users"]["following_issues"].find({"tickers": ticker}, sort=[("upload_timestamp", -1)], limit=10))
        # 待改：應該是在用戶輸入時，就修改出一個別名
        for issue_meta in issue_meta_list:
            issue_meta["issue"] = issue_meta["issue"].replace(' ', '_')
        return issue_meta_list

    # 預設是取當下到往前一日作摘要
    # 待確認: SE shorts、雲端主機的now怎麼設定
    # 待改：理論上還要融合長新聞，才會生成markdown
    def generate_shorts_summary(self, ticker_list, end_date=datetime.now(), period=1, words_per_shorts=8):
        def _call_shorts_summary_LLM(shorts_content_list, ticker, word_number):
            prompt = (
                      f"以下是一些可能與「股票代號為{ticker}的公司」有關的新聞消息，"
                      f"請挑選其中與公司相關性較高的消息，組合成一篇中文新聞，切勿包含任何不在消息中的內容"
                      f"並在各個段落用標題敘述段落重點，不含標題的總字數約{word_number}字，標題使用markdown語法中的###表示\n"
                     )
            
            prompt += "\n".join(shorts_content_list)
            return call_OpenAI_API(API_key=self.OpenAI_API_key, promt=prompt, model_version="gpt-4o", output_format="markdown")

        start_date = end_date - timedelta(days=period)
        logging.info(f"[SHORTS][SUMMARY]{datetime2str(start_date)}-{datetime2str(end_date)}")
        data_list = list()
        for ticker in ticker_list:        
            raw_shorts_df = self.get_raw_shorts_df(ticker, start_date, end_date)
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
                {"date": end_date,
                 "ticker": ticker,
                 "shorts_summary": shorts_summary}
            )
        if len(data_list) > 0:
            self.save_data_to_MDB(item="shorts_summary", data_list=data_list)
    
    @str2datetime_input
    def get_raw_stock_report_meta_list(self, ticker, start_date=None, end_date=datetime.now()):
        meta_list = self.MDB_client["raw_content"]["raw_stock_report_non_auto"].find({"ticker": ticker, 
                                                        "date": {"$gte": start_date, "$lt": end_date}})
        return meta_list
    
    @str2datetime_input
    def save_stock_report_summary(self, ticker, start_date=None, end_date=datetime.now()):
        def _creat_stock_report_summary(paragraph_list, ticker, output_format="text"):
            prompt = (f"以下是一篇「股票代號為{ticker}的公司」的研究報告，"
                    f"請以markdown的語法來整理這份報告，包括'全文摘要', '看多論點', '看空論點'三個段落，並翻譯為中文，"
                    f"回傳格式：全文僅包括'全文摘要', '看多論點', '看空論點'三個段落，不要有任何其他內容。"
                    f"markdown的段落標題統一使用'###'表示，段落內文不要有任何符號 \n")
            
            prompt += f"研究報告內容:\n{'n'.join(paragraph_list)}\n"
            return call_OpenAI_API(API_key=self.OpenAI_API_key, promt=prompt, model_version="gpt-4o", output_format=output_format)

        def _creat_issue_following_summary(paragraph_list, ticker, issue_list, word_number, output_format="json_object"):
            prompt = (f"以下是一篇與「股票代號為{ticker}的公司」有關的研究報告，"
                    f"依據以下不同的'issue'，各自挑選出相關的段落，整合成短文後翻譯為中文，各個段落短文的字數約{word_number}字，若無相關的內容，可以返回空字串。")    
            prompt += f"'issue':「{', '.join(issue_list)}」\n"
            prompt += f"將回傳值以 JSON 格式提供，其中包含以下的key: {", ".join(issue_list)}\n\n"
            prompt += f"研究報告內容:\n{'\n'.join(paragraph_list)}\n"
            return call_OpenAI_API(API_key=self.OpenAI_API_key, promt=prompt, model_version="gpt-4o", output_format=output_format)
        
        stock_report_meta_list = list(self.MDB_client["raw_content"]["raw_stock_report_non_auto"].find({"ticker": ticker, 
                                            "upload_timestamp": {"$gte": start_date, "$lte": end_date}}, sort=[("date", -1)]))
        
        # 取出該標的的追蹤問題
        stock_following_issue_meta_list = self.MDB_client["users"]["following_issues"].find({"tickers": ticker}, limit=10)
        stock_following_issue_list = [meta["issue"] for meta in stock_following_issue_meta_list]
        
        # 待改：多線程的情況下，會導致重複覆蓋？
        temp_pdf_folder_path = "/Users/yahoo168/Desktop/temp_pdf"
        make_folder(temp_pdf_folder_path)
        for stock_report_meta in stock_report_meta_list:
            response = requests.get(stock_report_meta["url"])
            temp_pdf_file_path = os.path.join(temp_pdf_folder_path, "temp_pdf_file.pdf")
           
            if response.status_code == 200:
                # 將下載的 PDF 文件保存到本地
                with open(temp_pdf_file_path, 'wb') as file:
                    file.write(response.content)     
            else:
                logging.warn(f"[SERVER][PDF][Error {response.status_code}]")
                continue
            
            paragraph_list = get_cleaned_paragraph_list_from_pdf(temp_pdf_file_path)
            logging.info(f"[LLM][preprocess]{stock_report_meta["title"]}")
            # 全文摘要 - 調用LLM
            stock_report_meta["summary"] = _creat_stock_report_summary(paragraph_list, ticker, output_format="text")
            # 問題追蹤 - 調用LLM
            following_issue_json_text = _creat_issue_following_summary(paragraph_list, ticker, stock_following_issue_list, word_number=500, output_format="json_object")
            stock_report_meta["following_issue"] = json.loads(following_issue_json_text)
            stock_report_meta["processed_timestamp"] = datetime.now()
            # 存入MongoDB
            self.MDB_client["preprocessed_content"]["stock_report"].insert_one(stock_report_meta)
            
    def save_stock_report_review(self, ticker, review_report_nums=10):
        def _create_raw_stock_report_review(summary_list, ticker, output_format="json_object"):
            prompt = (f"以下是一些「股票代號為{ticker}的公司」近期的多篇研究報告，"
                    f"挑選出其中的bullish_outlook以及bearish_outlook進行整理，2個面向各約300字，不包含對目標價的預測。")
            prompt += f"將回傳值以 JSON 格式提供，其中包含以下2️個key:'bullish_outlook'以及'bearish_outlook'。"
            prompt += f"除了上述的部分外，不要有任何其他內容\n\n"
            prompt += f"研究報告內容:\n{'\n'.join(summary_list)}\n"
            
            return call_OpenAI_API(API_key=self.OpenAI_API_key, promt=prompt, model_version="gpt-4o", output_format=output_format)

        def _adjust_stock_report_review_format(summary_text, ticker, output_format="text"):
            prompt = (f"以下是一些「股票代號為{ticker}的公司」近期的研究結論，"
                    f"以條列式的方式，整理其中的論點，每個論點以2~3句話表示內容，論點數量不高於10個。")
            prompt += f"回傳格式：每個論點以換行符('\n')分隔，論點前面不需要數字編號"
            prompt += f"除了上述的部分外，不要有任何其他內容\n\n"
            prompt += f"研究報告內容:\n{'\n'.join(summary_text)}\n"
            return call_OpenAI_API(API_key=self.OpenAI_API_key, promt=prompt, model_version="gpt-4o", output_format=output_format)
             
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
            return call_OpenAI_API(API_key=self.OpenAI_API_key, promt=prompt, model_version="gpt-4o", output_format="text")
            
        # 從mongoDB取得報告原始的meta data
        stock_report_meta_list = list(self.MDB_client["preprocessed_content"]["stock_report"].find({"ticker":ticker}, {"_id":0}, sort=[("processed_timestamp"), -1], limit=review_report_nums))
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
        
        old_bullish_argument_list = old_stock_report_review_meta["stock_report_review"]["bullish_outlook"]
        old_bearish_argument_list = old_stock_report_review_meta["stock_report_review"]["bearish_outlook"]
        
        bullish_outlook_diff = _compare_outlook_argument(old_argument_list=old_bullish_argument_list, new_argument_list=bullish_outlook_argument_list)
        bearish_outlook_diff = _compare_outlook_argument(old_argument_list=old_bearish_argument_list, new_argument_list=bearish_outlook_argument_list)
        
        # 存入MongoDB
        stock_report_review_meta = {
            "date": datetime.now(),
            "ticker": ticker,
            "stock_report_review": {
                "bullish_outlook": bullish_outlook_argument_list,
                "bearish_outlook": bearish_outlook_argument_list,
                "bullish_outlook_diff": bullish_outlook_diff,
                "bearish_outlook_diff": bearish_outlook_diff,
            },
        }
        self.MDB_client["published_content"]["stock_report_review"].insert_one(stock_report_review_meta)
    
    def get_preprocessed_stock_report_meta_list_by_num(self, ticker, num):
        return list(self.MDB_client["preprocessed_content"]["stock_report"].find({"ticker":ticker}, 
                                                                            sort=[("processed_timestamp", -1)], limit=num))

    def get_stock_following_issue_meta_list_by_num(self, ticker, num):
        return list(self.MDB_client["users"]["following_issues"].find({"tickers": ticker}, 
                                                                sort=[("upload_timestamp", -1)], limit=num))
        
    # 生成問題總結 (following issues review)，並儲存到資料庫（MongoDB）
    def save_stock_following_issue_review(self, ticker, review_report_nums=10):
        # 調用LLM，生成問題總結
        def _create_issue_review_LLM(ticker, issue, issue_content_json, output_format="text"):
            prompt =  f"以下是一些不同的報告來源，針對{ticker}的'{issue}'議題，近期的研究報告論點，請整合其內容，寫成一篇短文，追蹤市場這個issue的近期看法。\n"
            prompt += f"須註明看法的來源與日期，若沒有相關的論點，可返回空字串，不用另外搜尋，除了上述的部分外，不要有任何其他內容\n"
            prompt += f"回傳格式：不要使用markdown語法會出現的符號，使用純文字即可"
            prompt += f"研究報告內容:\n {issue_content_json}"
            return call_OpenAI_API(API_key=self.OpenAI_API_key, promt=prompt, model_version="gpt-4o", output_format=output_format)
        
        # 調用LLM，將新的問題總結與舊的問題總結進行比較，找出差異
        def _compare_issue_review_LLM(old_issue_review, new_issue_review, output_format="text"):
            prompt =  f"以下是針對相同問題的兩篇論述，請列出「新的論述」與「舊的論述」相比，有哪些不一樣的論點。\n"
            prompt += f"回傳格式：使用markdown語法，段落標題為「對比總結」以及「新舊觀點對比」，格式使用'###'，段落內文不使用任何符號\n"
            prompt += f"新的論述: {new_issue_review}\n"
            prompt += f"舊的論述: {old_issue_review}\n"
            
            OpenAI_API_key = "sk-proj-GzvuIu7QRcMeXMxzpRcJT3BlbkFJXYcMxEWH6aiytV5woJOc"
            return call_OpenAI_API(API_key=OpenAI_API_key, promt=prompt, model_version="gpt-4o", output_format=output_format)
   
        # 將自MongoDB取得的issue資料進行整理，改為適合GPT讀取的格式，嵌套dict (question: source: content)
        def _generate_issue_content_dict(following_issue_meta_list, stock_report_meta_list):
            all_issue_content_dict = {}
            for following_issue_meta in following_issue_meta_list:
                issue_name = following_issue_meta["issue"]
                issue_content_dict = defaultdict(list)
                
                for stock_report_meta in stock_report_meta_list:
                    issue_content = stock_report_meta["following_issue"].get(issue_name, '')
                    if not issue_content:
                        continue
                    
                    # 假设 source_trans_dict 和 keys_to_convert 是全局变量或类属性
                    source_trans_dict = {"gs": "Goldman Sachs", "jpm": "J.P. Morgan", "citi": "Citi", "barclays": "Barclays"}
                    source = source_trans_dict.get(stock_report_meta["source"], "Unknown Source")
                    issue_content_dict[source].append(
                        {
                            "date": datetime2str(stock_report_meta["date"]),
                            "content": issue_content,
                        }
                    )   
                all_issue_content_dict[issue_name] = dict(issue_content_dict)
            return all_issue_content_dict
        
        stock_report_meta_list = self.get_preprocessed_stock_report_meta_list_by_num(ticker, num=review_report_nums)
        following_issue_meta_list = self.get_stock_following_issue_meta_list_by_num(ticker, num=review_report_nums)
        all_issue_content_dict = _generate_issue_content_dict(following_issue_meta_list, stock_report_meta_list)
        
        # 針對每個問題，調用LLM，生成問題總結
        for issue_meta in following_issue_meta_list:
            issue_name = issue_meta["issue"]
            issue_related_content = all_issue_content_dict[issue_name]
            # 使用 ensure_ascii=False 将 JSON 转换为 Unicode 编码格式
            issue_related_content_json_string = json.dumps(issue_related_content, ensure_ascii=False, indent=4)
            issue_review_text = _create_issue_review_LLM(ticker, issue_name, issue_related_content_json_string)
            # 取出舊的問題總結，進行比較，找出差異
            old_issue_review_meta = self.MDB_client["published_content"]["following_issue_review"].find_one({"ticker": ticker, "issue_id": issue_meta["_id"]}, 
                                                                                                            sort=[("upload_timestamp", -1)])
            # 若舊的問題總結不存在，則不進行比較，將issue_review_diff設定為空字串
            issue_review_diff = ''
            if old_issue_review_meta:
                issue_review_diff = _compare_issue_review_LLM(old_issue_review=old_issue_review_meta["issue_review"], new_issue_review=issue_review_text)
            
            self.MDB_client["published_content"]["following_issue_review"].insert_one(
                    {
                    "upload_timestamp":datetime.now(), 
                    "ticker":[ticker],
                    "issue_review": issue_review_text, 
                    "issue_review_diff": issue_review_diff,
                    "issue_id": issue_meta["_id"], 
                    "issue_name": issue_name
                    }
                )