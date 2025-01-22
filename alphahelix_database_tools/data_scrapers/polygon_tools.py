import requests
import os, logging, time
from datetime import datetime, timedelta, timezone
from threading import Thread
import pandas as pd

from alphahelix_database_tools.utils import datetime2str

# Note：已去除存檔，改為回傳data dict
# 自polygon下載EOD價量相關資料，若無指定起始/結束日期，則自動以上次更新日期的後一日開始抓取資料，更新至今日
def save_stock_OHLCV_from_Polygon(API_key, start_date=None, end_date=None, adjust=False):
    def _save_stock_OHLCV_from_Polygon_singleDate(API_key, date, item_list, data_dict, adjust):
        # 將boolean value串連url string，用於呼叫API
        adjust_flag = "true" if adjust else "false"

        url = (
                f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}"
                f"?adjusted={adjust_flag}&apiKey={API_key}"
            )
        
        data_json = requests.get(url).json()
        
        # status DELAYED代表美股盤後交易時段，約台灣時間中午12點方能取得昨日的正式收盤價（確切時間待確定）
        if (data_json["status"] in ["NOT_AUTHORIZED", "DELAYED"]) or (data_json["queryCount"] == 0):
            logging.warning(f"[Polygon][{date}][OHLCV] 資料狀態異常({data_json["status"]})，請稍後再試")
            return False
        # 將polygon回傳資料轉化為dataframe
        df = pd.DataFrame(data_json["results"])
        # 將polygon原key轉化為資料庫column
        df = df.rename(columns={"T":"ticker", "v":"volume", "vw":"avg_price", "o":"open", "c":"close", "l":"low", "h":"high", "n":"transaction_num", "t":"timestamp"})
        # 將polygon ticker格式轉化為資料庫格式（用"_"標示所有連接符）
        df["ticker"] = df["ticker"].apply(lambda x : x.replace(".", "_"))
        df = df.set_index("ticker")
        
        # 依序將各column切出，轉為csv檔，檔名為該日日期
        for item in item_list:
            data_series = df.loc[:, item]
            data_series = data_series.rename(date)
            # 存檔至以指定資料項目為名的資料夾之下
            if adjust==True:
                item = "adj_"+item
            # {item:{date:{ticker:value}}}
            data_dict[item][date] = data_series.to_dict()

    # 列出起始/結束日，中間的日期，並轉為字串形式
    date_range_list = list(map(lambda x:datetime2str(x), list(pd.date_range(start_date, end_date, freq='d'))))
    threads = list()
    
    # 逐日下載資料，儲存在dict中
    item_list = ["open", "high", "low", "close", "volume", "avg_price", "transaction_num"]
    data_dict = {item: {} for item in item_list}

    for index, date in enumerate(date_range_list, 1):
        t = Thread(target=_save_stock_OHLCV_from_Polygon_singleDate, 
            args=(API_key, date, item_list, data_dict, adjust))
        t.start()  # 開啟線程，在線程之間設定間隔，避免資料源過載或爬蟲阻擋
        time.sleep(0.1)
        threads.append(t)
        percentage = 100*round(index/len(date_range_list), 2)            
        logging.info("[Polygon][{date}][OHLCV] 資料下載中，完成度{percentage}%".format(date=date, percentage=percentage))

    for t in threads:
        t.join()
        
    return data_dict

def save_stock_split_from_Polygon(API_key, start_date=None, end_date=None):
    def _save_stock_split_from_Polygon_singleDate(API_key, date, data_dict):
        url = "https://api.polygon.io/v3/reference/splits?execution_date={date}&apiKey={API_key}".format(date=date, API_key=API_key)
        data_json = requests.get(url).json()
        results_dict = data_json["results"]
        if len(results_dict) > 0:
            df = pd.DataFrame(results_dict)
            df["adjust_factor"] = df["split_to"] / df["split_from"]
            df = df.pivot(index="execution_date", columns="ticker", values="adjust_factor").T
            data_dict[date] = df.sort_index()
    
    date_range_list = list(map(lambda x:datetime2str(x), list(pd.date_range(start_date, end_date, freq='d'))))
    
    data_dict = dict()
    threads = list()  # 儲存線程以待關閉
    try:
        for index, date in enumerate(date_range_list, 1):
            t = Thread(target=_save_stock_split_from_Polygon_singleDate, 
                        args=(API_key, date, data_dict))
            t.start()  # 開啟線程

            #在線程之間設定間隔（0.5秒），避免資料源過載或爬蟲阻擋
            time.sleep(0.1)
            threads.append(t)
            percentage = 100*round(index/len(date_range_list), 2)            
            logging.info("[Polygon][{date}][split] 資料下載中，完成度{percentage}%".format(date=date, percentage=percentage))                
        for t in threads:
            t.join()
    
    except Exception as e:
        logging.warning(e)

    return data_dict

def save_stock_cash_dividend_from_Polygon(API_key, start_date, end_date, div_type):
    """
    div_type: ex_dividend_date / pay_date
    """
    def _save_stock_cash_dividend_from_Polygon_singleDate(API_key, date, div_type, data_dict):
        #只下載現金股利（CD）
        url = "https://api.polygon.io/v3/reference/dividends?{0}={1}&apiKey={2}&dividend_type=CD".format(div_type, date, API_key)
        data_json = requests.get(url).json()
        
        # 確認資料狀態
        if data_json["status"] != "OK":
            logging.warning(f"[Polygon][{date}][dividends][{div_type}] 資料狀態異常({data_json["status"]})，請稍後再試")
            return False
        
        results_dict = data_json["results"]
        if len(results_dict) > 0:
            df = pd.DataFrame(results_dict)
            df = df[df["currency"] == "USD"] # 只取美元股利（外幣股利暫不處理）
            if len(df) > 0:
                d = df.pivot(index="ticker", columns=div_type, values="cash_amount").to_dict()
            else:
                d = {date: {}}
            
            data_dict.update(d)
    
    if div_type not in ["ex_dividend_date", "pay_date"]:
        logging.warning(f"[Polygon][{date}][dividends][{div_type}] 資料類型錯誤，請檢查")
        return False
    
    # 列出起始/結束日，中間的日期，並轉為字串形式
    data_dict = dict()
    date_range_list = list(map(lambda x:datetime2str(x), list(pd.date_range(start_date, end_date, freq='d'))))
    threads = list()  # 儲存線程以待關閉
    try:
        for index, date in enumerate(date_range_list, 1):
            t = Thread(target=_save_stock_cash_dividend_from_Polygon_singleDate, 
                        args=(API_key, date, div_type, data_dict))
            t.start()  # 開啟線程
            time.sleep(0.1) #在線程之間設定間隔（0.1秒），避免資料源過載或爬蟲阻擋
            threads.append(t)
            percentage = 100*round(index/len(date_range_list), 2)         
            logging.info(f"[Polygon][{date}][dividends][{div_type}] 資料下載中，完成度{percentage}%")                
        for t in threads:
            t.join()
    
    except Exception as e:
        logging.warning(e)

    return data_dict

def save_stock_shares_outstanding_from_Polygon(API_key, ticker_list, start_date, end_date):
    # 流通股數係透過polygon中的ticker detail資訊取得，索取方式為給定ticker與date，故包裝為雙重函數
    def _save_stock_shares_outstanding_from_Polygon_singleDate(API_key, ticker_list, date):
        def _save_stock_shares_outstanding_from_Polygon_singleTicker(API_key, ticker, date):
            url = "https://api.polygon.io/v3/reference/tickers/{}?date={}&apiKey={}".format(ticker, date, API_key)
            data_json = requests.get(url).json()
            # Note：不能用weighted shares
            data_dict[ticker] = data_json["results"]["share_class_shares_outstanding"]
        
        data_dict = dict()
        threads = list()
        for index, ticker in enumerate(ticker_list, 1):
            t = Thread(target=_save_stock_shares_outstanding_from_Polygon_singleTicker, 
                args=(API_key, ticker, date))
            t.start()  # 開啟線程，在線程之間設定間隔，避免資料源過載或爬蟲阻擋
            time.sleep(0.1)
            threads.append(t)
            percentage = 100*round(index/len(ticker_list), 2)            
            logging.info("[Polygon][{date}][{ticker}]流通股數下載中，完成度{percentage}%".format(date=date, ticker=ticker, percentage=percentage))

        for t in threads:
            t.join()

        return pd.Series(data_dict)

    date_range_list = list(map(lambda x:datetime2str(x), list(pd.date_range(start_date, end_date,freq='d'))))
    result_dict = dict()
    for date in date_range_list:
        data_series = _save_stock_shares_outstanding_from_Polygon_singleDate(API_key, ticker_list, date)
        result_dict[date] = data_series.to_dict()
    return result_dict

def save_stock_universe_ticker_from_polygon(API_key, universe_name, start_date, end_date):    
    # 轉為polygon的URL辨識碼
    if universe_name=="univ_us_stock":
        ticker_type = "CS"
    elif universe_name=="univ_us_etf":
        ticker_type = "ETF"
    
    data_dict = dict()
    date_range_list = list(map(lambda x:datetime2str(x), list(pd.date_range(start_date, end_date, freq='d'))))
    for date in date_range_list:
        ticker_list = list()
        url = "https://api.polygon.io/v3/reference/tickers?type={ticker_type}&date={date}&market=stocks&active=True&limit=1000&apiKey={API_key}".format(ticker_type=ticker_type, date=date, API_key=API_key)
        data_json = requests.get(url).json()
        # 因polygon標的資料有索引上限1000，故須進行翻頁索引
        part_ticker_list = list(pd.DataFrame(data_json["results"])["ticker"])
        ticker_list.extend(part_ticker_list)
        while True:
            if "next_url" in data_json.keys():
                next_url = data_json["next_url"] + "&apiKey={API_key}&limit=1000".format(API_key=API_key)
                data_json = requests.get(next_url).json()
                part_ticker_list = list(pd.DataFrame(data_json["results"])["ticker"])
                ticker_list.extend(part_ticker_list)
            else:
                break

        data_dict[date] = ticker_list
        logging.info("[Polygon][{universe_name}][{date}]成分股資料抓取完成".format(universe_name=universe_name, date=date))
    
    return data_dict

def save_stock_delisted_info_from_polygon(folder_path, universe_type, API_key):
    df_list = list()
    url = "https://api.polygon.io/v3/reference/tickers?type={universe_type}&market=stocks&active=false&limit=1000&apiKey={API_key}".format(universe_type=universe_type, API_key=API_key)
    data_json = requests.get(url).json()

    # 因polygon標的資料有索引上限1000，故須進行翻頁索引
    df = pd.DataFrame(data_json["results"])
    df_list.append(df)
    while True:
        if "next_url" in data_json.keys():
            next_url = data_json["next_url"] + "&apiKey={API_key}&limit=1000".format(API_key=API_key)
            data_json = requests.get(next_url).json()
            df = pd.DataFrame(data_json["results"])
            df_list.append(df)
        else:
            break
    
    df = pd.concat(df_list)
    # 原日期編碼為utc字串，前10碼為年月日
    df["delisted_date"] = df["delisted_utc"].apply(lambda x:x[:10])
    df = df.loc[:, ["ticker", "name", "delisted_date"]]
    df = df.drop_duplicates(subset=["ticker"])
    df = df.sort_values(by="delisted_date")
    df = df.reset_index(drop=True)
    date = datetime2str(datetime.today())

    if universe_type=="CS":
        file_name = "delisted_stock_info"

    if universe_type=="ETF":
        file_name = "delisted_etf_info"

    filePath = os.path.join(folder_path, file_name+".csv")
    df.to_csv(filePath)

def save_stock_company_info_from_Polygon(API_key, ticker_list):
    def _save_stock_company_info_from_Polygon_singleTicker(API_key, ticker):
        url = "https://api.polygon.io/v3/reference/tickers/{}?&apiKey={}".format(ticker, API_key)
        data_json = requests.get(url).json()
        company_info_dict[ticker] = data_json["results"]
    
    company_info_dict = {ticker:dict() for ticker in ticker_list}
    threads = list()
    for index, ticker in enumerate(ticker_list, 1):
        t = Thread(target=_save_stock_company_info_from_Polygon_singleTicker, 
            args=(API_key, ticker))
        t.start()  # 開啟線程，在線程之間設定間隔，避免資料源過載或爬蟲阻擋
        time.sleep(0.1)
        threads.append(t)
        percentage = 100*round(index/len(ticker_list), 2)            
        logging.info("[Polygon][{ticker}]公司資訊下載中，完成度{percentage}%".format(ticker=ticker, percentage=percentage))

    for t in threads:
        t.join()

    company_info_df = pd.DataFrame(company_info_dict).T
    return company_info_df

# 自polygon獲取市場狀態，1: 交易日, 0:六日休市, -1:非六日休市
def save_stock_market_status_from_Polygon(API_key):
    url = "https://api.polygon.io/v1/marketstatus/upcoming?apiKey={}".format(API_key)
    data_json = requests.get(url).json()
    
    # polygon會直接返回未來一年多的法定假日，故設定範圍為查詢日期之一年內
    start_date = datetime2str(datetime.today())
    end_date = datetime2str(datetime.today()+timedelta(days=365))
    
    # 獲得法定假日之日期（holiday_series）
    df = pd.DataFrame(data_json)
    holiday_series = df[df["status"]=="closed"]["date"].drop_duplicates()
    holiday_series = holiday_series[(holiday_series>=start_date) & (holiday_series<=end_date)]
    
    # 依照起始、結束日，建立空的market_status_df
    date_range_series = pd.Series(pd.date_range(start_date, end_date, freq='d'))
    market_status_series = pd.Series(index=date_range_series)
    
    # 找出查詢區間內的週末（六日）日期
    is_weekend_series = date_range_series.apply(lambda x:x.day_name() in (['Saturday', 'Sunday']))
    weekend_series = date_range_series[is_weekend_series]
    
    # 依照編碼規則填入（1: 交易日, 0: 六日休市, -1: 非六日休市）
    market_status_series[holiday_series] = -1
    market_status_series[weekend_series] = 0
    market_status_series = market_status_series.fillna(1)
    
    return market_status_series

def save_stock_news_from_Polygon(API_key, ticker, start_timestamp=None):
    url = f"https://api.polygon.io/v2/reference/news?ticker={ticker}&limit=1000&apiKey={API_key}"
    if start_timestamp is not None:
        url += f"&published_utc.gt={start_timestamp.isoformat()}"
    
    response = requests.get(url)
    data_dict = response.json()
    raw_news_meta_list = []

    if data_dict.get("status") == "OK":
        raw_news_meta_list += data_dict.get("results", [])
        
        next_url = data_dict.get("next_url", None)
        
        page_count = 1
        while next_url:
            page_count += 1
            response = requests.get(next_url + f"&apiKey={API_key}")
            data_dict = response.json()
            # 若翻頁過程中，status不為OK，則中斷迴圈
            if data_dict.get("status") != "OK":
                logging.error(f"Error: {data_dict.get('status', 'Unknown error')}")
                break
            
            raw_news_meta_list += data_dict.get("results", [])
            next_url = data_dict.get("next_url", None)

    news_meta_list = list()
    for raw_news_meta in raw_news_meta_list:
        
        news_meta = {
                        # 日期格式範例：'2024-05-01T11:25:57Z'，為UTC時間
                        "data_timestamp": datetime.strptime(raw_news_meta["published_utc"], '%Y-%m-%dT%H:%M:%SZ'), 
                        # 紀錄更新時間（UTC時間）
                        "updated_timestamp": datetime.now(timezone.utc),
                        "title": raw_news_meta.get("title", ''), 
                        "content": raw_news_meta.get("description", ''),
                        # 記錄用於搜尋的ticker，以供下次搜尋前判斷前一次搜尋的新聞日期
                        # 因polygon新聞搜尋較廣泛，若先針對A ticker搜尋新聞，tickers會有多個ticker，若再針對B ticker搜尋新聞，查找tickers判斷最新的日期會失準，導致B的新聞很少
                        "search_ticker": [ticker],
                        "tickers": raw_news_meta.get("tickers", []),
                        "url": raw_news_meta.get("article_url", ''),
                        "source": raw_news_meta.get("publisher", {}).get("name", ''),
                        "data_source": "polygon_io",
                    }

        news_meta_list.append(news_meta)
    
    return news_meta_list