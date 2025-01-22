from datetime import datetime, timedelta, timezone

from alphahelix_database_tools.us_stock_database.data_manager import UsStockDataManager
from alphahelix_database_tools.data_scrapers.polygon_tools import *
from alphahelix_database_tools.utils.datetime_utils import TODAY_DATE_STR, str2datetime, datetime2str

from dotenv import load_dotenv #type: ignore

# 載入.env文件（引入本地測試環境變數）
load_dotenv()

class UsStockDataUpdater(UsStockDataManager):
    def __init__(self, username, password):
        super().__init__(username, password)  # 调用父类的初始化方法
        self._load_api_keys()
    
    # 並取用環境變數（用於本地測試)，若部署至雲端伺服器則使用雲端的環境變數
    def _load_api_keys(self):
        self.polygon_API_key = os.getenv('polygon_API_key')
    
    def update_stock_OHLCV_data(self, start_timestamp=None, end_timestamp=None, adjust=False, source="polygon"):
        if start_timestamp == None:
            start_timestamp = self.get_latest_data_date(item="open")
            start_timestamp = start_timestamp + timedelta(days=1)
        
        if end_timestamp == None:
            end_timestamp = TODAY_DATE_STR
        
        # 若資料已更新至最新日期，則不需進行更新
        current_timestamp = datetime.now()
        if current_timestamp - start_timestamp < timedelta(days=1):
            logging.info(f"[SAVE][OHLCV][資料已更新至最新日期({datetime2str(start_timestamp)})，不需進行更新]")
            return
        
        logging.info(f"[SAVE][OHLCV][{datetime2str(start_timestamp)} ~ {datetime2str(end_timestamp)}]")
        
        if source == "polygon":
            data_dict = save_stock_OHLCV_from_Polygon(self.polygon_API_key, start_timestamp, end_timestamp, adjust)
        else:
            raise ValueError(f"[SAVE][OHLCV][ERROR: Unsupport Source]: {source}")
        
        item_list = ["open", "high", "low", "close", "volume"] # "avg_price", "transaction_num"
        for item in item_list:
            item_dao = self._get_dao_instance(item)
            item_data_dict = data_dict.get(item, {})
            
            # Process and transform the data
            item_data_list = [
                {
                    "data_timestamp": str2datetime(date),
                    "created_timestamp": datetime.now(timezone.utc),
                    "values": values,
                }
                for date, values in item_data_dict.items()
            ]
            item_dao.insert_many(item_data_list, unique_key="data_timestamp")
            
    
    def update_stock_market_status(self, source="polygon"):
        """
        Update the stock market status by fetching data from the specified source and saving it to the database.

        Args:
            source (str): The data source to fetch market status from. Defaults to "polygon".

        Raises:
            Exception: If the data source is not supported.
        """
        if source == "polygon":
            market_status_series = save_stock_market_status_from_Polygon(API_key=self.polygon_API_key)
        else:
            raise Exception(f"Unsupported data source: {source}")
            
        # Get the latest data date in the database
        latest_data_date = self.get_latest_data_date(item="market_status")
        
        # Filter the series to only include new data
        market_status_series = market_status_series[market_status_series.index > latest_data_date]
        
        if not market_status_series.empty:
            # Prepare data for insertion
            data_list = [
                {"data_timestamp": date, 
                 "created_timestamp": datetime.now(timezone.utc),
                 "values": int(market_status),
                }
                for date, market_status in market_status_series.to_dict().items()
            ]
            
            # Insert the data into the database
            dao_instance = self._get_dao_instance("market_status")
            dao_instance.insert_many(data_list, unique_key="data_timestamp")
            
            start_timestamp = datetime2str(data_list[0]["data_timestamp"])
            end_timestamp = datetime2str(data_list[-1]["data_timestamp"])
            logging.info(f"[SAVE][market_status][{datetime2str(start_timestamp)} ~ {datetime2str(end_timestamp)}]")
            
        else:
            logging.info("[SAVE][market_status][資料已更新至最新日期，不需進行更新]")
    
    def update_stock_split_data(self, start_timestamp=None, end_timestamp=None, source="polygon"):
        """
        儲存股票分割資料 (stock splits)
        """
        # 設定時間範圍
        if start_timestamp is None:
            # 若未給定起始日，則從最新資料的隔日開始
            latest_date = self.get_latest_data_date(item="stock_split")
            start_timestamp = datetime2str(latest_date + timedelta(days=1))

        if end_timestamp is None:
            # 若未給定結束日，則預設抓取至隔日
            end_timestamp = datetime2str(datetime.today() + timedelta(days=1))

        # 根據資料來源抓取資料
        if source == "polygon":
            data_dict = save_stock_split_from_Polygon(self.polygon_API_key, start_timestamp, end_timestamp)
            
        else:
            logging.error(f"[SAVE][stock_splits][資料來源不支持: {source}]")
            return

        # 統一時間戳
        created_timestamp = datetime.now(timezone.utc)
        
        # 待改：應該改成無資料也插入doc，但values為空list
        # 轉換資料為列表格式
        data_list = [
            {
                "data_timestamp": str2datetime(date),
                "created_timestamp": created_timestamp,
                "values": values
            }
            for date, values in ((date, s.to_dict()[date]) for date, s in data_dict.items())
        ]
        
        logging.info(f"[SAVE][stock_splits][{datetime2str(start_timestamp)} ~ {datetime2str(end_timestamp)}]")
        # 儲存資料至資料庫
        if data_list:
            dao_instance = self._get_dao_instance("stock_split")
            try:
                dao_instance.insert_many(data_list, unique_key="data_timestamp")
                logging.info(f"[SAVE][stock_splits][成功儲存 {len(data_list)} 筆資料]")
            except Exception as e:
                logging.error(f"[SAVE][stock_splits][資料儲存失敗: {e}]")
        else:
            logging.info("[SAVE][stock_splits][資料已更新至最新日期]")

    # 儲存現金股利資料
    def update_stock_cash_dividend(self, start_timestamp=None, end_timestamp=None, item="ex_dividend", source="polygon"):
        if start_timestamp == None:
            start_timestamp = self.get_latest_data_date(item)
            start_timestamp = datetime2str(start_timestamp + timedelta(days=1))

        if end_timestamp == None:
            end_timestamp = datetime2str(datetime.today() + timedelta(days=1))
        
        if source == "polygon":
            trans_dict = {"ex_dividend": "ex_dividend_date", "pay_dividend": "pay_date"} # polygon字段轉換字典
            
            data_dict = save_stock_cash_dividend_from_Polygon(self.polygon_API_key, 
                                                            start_timestamp, 
                                                            end_timestamp, 
                                                            div_type=trans_dict[item])
        else:
            raise ValueError(f"source: {source} is not supported.")
        
        if not data_dict:
            logging.info("[NOTE][update_stock_cash_dividend][此區間不存在資料]")
            return
        
        created_timestamp = datetime.now(timezone.utc)
        # 待改：應該改成無資料也插入doc，但values為空list
        data_list = [
                    {
                        "data_timestamp": str2datetime(date),
                        "created_timestamp": created_timestamp,
                        "values": values
                    }
                    for date, values in data_dict.items()
                ]
        
        logging.info(f"[SAVE][{item}][{datetime2str(start_timestamp)} ~ {datetime2str(end_timestamp)}]")
        
        # 儲存資料至資料庫
        if data_list:
            dao_instance = self._get_dao_instance(item)
            try:
                dao_instance.insert_many(data_list, unique_key="data_timestamp")
                logging.info(f"[SAVE][{item}][成功儲存 {len(data_list)} 筆資料]")
            except Exception as e:
                logging.error(f"[SAVE][{item}][資料儲存失敗: {e}]")
        else:
            logging.info("[SAVE][{item}][資料已更新至最新日期]")
    
    # 計算並儲存open_to_open return以及close_to_close return
    def update_stock_daily_return(self, start_timestamp=None, end_timestamp=None, item="c2c_ret"):
        # 依照return類型選擇股價類型（open or close）
        price_type = "close" if item == "c2c_ret" else "open"
        
        if start_timestamp == None:
            # 若未指定起始/結束日期，則自資料最新儲存資料日開始抓取資料
            # 不遞延一天，係因若日報酬更新至t日，為計算t+1日的日報酬，須取得t日的價格與分割資料
            start_timestamp = self.get_latest_data_date(item=item)

        if end_timestamp == None:
            end_timestamp = str2datetime(TODAY_DATE_STR)
        
        # 若資料已更新至最新日期，則不需進行更新
        current_timestamp = datetime.now()
        if current_timestamp - start_timestamp < timedelta(days=2):
            logging.info(f"[SAVE][{item}][資料已更新至最新日期({datetime2str(start_timestamp)})，不需進行更新]")
            return

        logging.info(f"[SAVE][{item}][{datetime2str(start_timestamp)} ~ {datetime2str(end_timestamp)}]")
        
        price_df = self.get_item_df(item=price_type, method="by_date", start_timestamp=start_timestamp, end_timestamp=end_timestamp)
        
        # 取得股價調整因子，因須計算當日股利報酬，使用forward法可還原當日股價
        adjust_factor_df = self.get_stock_adjust_factor_df(start_timestamp=start_timestamp, end_timestamp=end_timestamp, method="forward")        
        
        # 取得股利資料並過濾非交易日的數據（少部分特殊ticker假日發股利，這種情況當作沒發，否則會導致return出現Nan）
        dividends_df = self.get_item_df(item="ex_dividend", method="by_date", start_timestamp=start_timestamp, end_timestamp=end_timestamp)
        dividends_df = dividends_df.reindex(price_df.index).fillna(0)  # 確保索引對齊
        dividends_ret_df = (dividends_df / price_df).fillna(0)
        
        # 對股價進行調整
        adjust_ticker_list = price_df.columns.intersection(adjust_factor_df.columns)
        price_df[adjust_ticker_list] *= adjust_factor_df[adjust_ticker_list]

        # 計算日報酬
        return_df = price_df.pct_change().add(dividends_ret_df, fill_value=0)

        # 過濾無效數據
        return_df = return_df.mask(price_df.isna())

        # 移除第一行，因為 pct_change 的結果為 NaN
        return_df = return_df.iloc[1:]

        # 初始化 data_list
        data_list = []
        
        # 設置創建時間戳
        created_timestamp = datetime.now(timezone.utc)
        
        # 逐行處理 return_df
        for date in return_df.index:
            # 提取當日的 Series，移除 NaN 值
            return_series = return_df.loc[date].dropna()
            
            # 移除索引為 NaN 的情況（來自資料異常）
            return_series = return_series[return_series.index.notna()]
            
            if not return_series.empty:
                # 添加到 data_list 中
                data_list.append({
                    "data_timestamp": date,  # 假設索引已為 datetime 格式，否則需要轉換
                    "created_timestamp": created_timestamp,
                    "values": return_series.to_dict()  # 將 Series 轉為字典
                })
        
        # 儲存資料至資料庫
        if data_list:
            dao_instance = self._get_dao_instance(item)
            try:
                dao_instance.insert_many(data_list, unique_key="data_timestamp")
                logging.info(f"[SAVE][{item}][成功儲存 {len(data_list)} 筆資料]")
            except Exception as e:
                logging.error(f"[SAVE][{item}][資料儲存失敗: {e}]")
        else:
            logging.info(f"[SAVE][{item}][資料源返回空值，無資料更新，建議檢查資料源是否正常]")
    
    def update_stock_universe_ticker(self, item, start_timestamp=None, end_timestamp=None, source="polygon"):
        if start_timestamp is None:
            start_timestamp = self.get_latest_data_date(item=item)
            start_timestamp = start_timestamp + timedelta(days=1)

        if end_timestamp is None:
            end_timestamp = TODAY_DATE_STR

        # 若資料已更新至最新日期，則不需進行更新
        current_timestamp = datetime.now()
        if current_timestamp - start_timestamp < timedelta(days=1):
            logging.info(f"[SAVE][{item}][資料已更新至最新日期({datetime2str(start_timestamp)})，不需進行更新]")
            return
        
        logging.info(f"[SAVE][{item}][{datetime2str(start_timestamp)} ~ {datetime2str(end_timestamp)}]")    
        
        if source == "polygon":
            data_dict = save_stock_universe_ticker_from_polygon(self.polygon_API_key, item, datetime2str(start_timestamp), datetime2str(end_timestamp))
        else:
            raise ValueError(f"[SAVE][{item}][source not found]")
        
        # 依照起始/結束日期，取得交易日list
        trade_date_list = self.get_trade_date_list(start_timestamp, end_timestamp)
        
        # 僅讀取交易日的universe data，並存入資料庫
        current_timestamp = datetime.now(timezone.utc)
        for data_timestamp in trade_date_list:
            date_str = datetime2str(data_timestamp)
            if date_str not in data_dict:
                logging.info(f"[SAVE][{item}][{date_str}][data not found]")
                continue
            
            ticker_list = data_dict[date_str]
            meta_data = {"data_timestamp": data_timestamp,
                        "created_timestamp": current_timestamp,
                        "values": ticker_list}
            
            dao_instance = self._get_dao_instance(item)
            dao_instance.insert_one(meta_data, unique_key="data_timestamp")
            logging.info(f"[SAVE][{item}][{date_str}][{len(ticker_list)}]")
            
    
    # 儲存流通股數（已與舊資料校驗，函數功能正常，然而polygon shares資料品質較差，經常更動（主要是小型股，spx500偶爾也有），須定期用BBG刷新）
    def update_stock_shares_outstanding(self, start_timestamp=None, end_timestamp=None, source="polygon"):
        if start_timestamp == None:
            latest_data_timestamp = self.get_latest_data_date("shares_outstanding")
            start_timestamp = datetime2str(latest_data_timestamp + timedelta(days=1))

        if end_timestamp == None:
            end_timestamp = TODAY_DATE_STR

        # 若資料已更新至最新日期，則不需進行更新
        current_timestamp = datetime.now()
        if current_timestamp - latest_data_timestamp <= timedelta(days=1):
            logging.info(f"[SAVE][shares_outstanding][資料已更新至最新日期({datetime2str(latest_data_timestamp)})，不需進行更新]")
            return
        
        logging.info("[SAVE][shares_outstanding][{start_timestamp} ~ {end_timestamp}]".format(start_timestamp=start_timestamp, end_timestamp=end_timestamp))
        if source != "polygon":
            raise ValueError("[SAVE][shares_outstanding][不支持的來源: {source}]".format(source=source))
        
        # 因polygon須依據ticker逐一抓取股數，若每日更新較為費時，故僅在每月最後一日重新抓取
        # 其他日則依據前一日股數，參考split調整
        trade_date_list = self.get_trade_date_list(start_timestamp=start_timestamp, end_timestamp=end_timestamp)
        
        logging.info(f"[SAVE][shares_outstanding][{datetime2str(start_timestamp)} ~ {datetime2str(end_timestamp)}]")
        
        # 設置創建時間戳
        current_timestamp = datetime.now(timezone.utc)
        data_list = list()
        for timestamp in trade_date_list:
            # 取得該日的上一交易日與下一交易日
            last_trade_date = self.get_closest_trade_date(timestamp=timestamp, direction="last", cal_self=False)
            next_trade_date = self.get_closest_trade_date(timestamp=timestamp, direction="next", cal_self=False)

            # 判斷是否為該月最後一交易日（用於判斷是否重新下載股數資料，或者僅用stock split推算）
            if_last_trade_date_of_the_moth = False
            if timestamp.month != next_trade_date.month:
                if_last_trade_date_of_the_moth = True
            
            # 取得該日的上一交易日之流通股數資料
            last_date_shares_series = self.get_item_df(item="shares_outstanding", method="by_num", end_timestamp=last_trade_date, num=1).squeeze(axis=0)
            # 取得split資料（須設定axis為0，否則當日僅有一檔標的split時，ticker會消失，降維成純量）
            stock_splits_series = self.get_item_df(item="stock_split", method="by_num", end_timestamp=timestamp, num=1).squeeze(axis=0)
            
            # 若當日非月底最後一交易日，則依據前一日股數，參考split調整
            if if_last_trade_date_of_the_moth == False:
                shares_series = last_date_shares_series.copy(deep=True)
                adjusted_shares_series = (last_date_shares_series * stock_splits_series).dropna()
                shares_series.update(adjusted_shares_series)                        
                logging.info("[SAVE][shares_outstanding][{date}][流通股數計算完成（依據前日基礎計算）]".format(date=datetime2str(timestamp)))
                
            # 月底最後一交易日，重新抓取（減少計算量）
            else:
                logging.info("[NOTE][shares_outstanding][本日是本月最後一交易日，須重新更新股數計算基礎]")
                # 取得最新一筆美股所有上市公司清單
                univ_us_ticker_list = self.get_latest_universe_tickers(universe_item="univ_us_stock")
                
                # 因polygon流通股數資料，通常會延遲1~2日反應split調整，若直接下載當日數據會出錯，故若重新下載日之前2日，該股曾進行split，則該股不重新下載，改回依據前日股數參考split調整
                recent_splits_df = self.get_item_df(item="stock_split", method="by_num", end_timestamp=timestamp, num=2)
                recent_splits_ticker_list = list(recent_splits_df.columns)
                target_ticker_list = sorted(list(set(univ_us_ticker_list) - set(recent_splits_ticker_list)))                
                
                # 下載流通股數資料（polygon）
                data_dict = save_stock_shares_outstanding_from_Polygon(API_key=self.polygon_API_key, ticker_list=target_ticker_list, start_timestamp=timestamp, end_timestamp=timestamp)
                shares_series = pd.Series(data_dict[datetime2str(timestamp)])
                
                # 取得缺漏標的：近日有作split + polygon資料源缺漏
                lost_ticker_set = set(univ_us_ticker_list) - set(list(shares_series.index))
                # 在缺漏標的中，過濾出昨日即有股數資料的標的
                lost_ticker_list = list(lost_ticker_set.intersection(last_date_shares_series.index))

                # 針對缺漏的標的，改為依據前一日股數，參考split調整（即依照一般日算法）
                lost_ticker_shares_series = last_date_shares_series[lost_ticker_list]
                lost_ticker_shares_series.update((lost_ticker_shares_series * stock_splits_series).dropna())

                # 將缺漏標的資料，併入polygon重新下載而得的資料
                shares_series = pd.concat([shares_series, lost_ticker_shares_series]).sort_index()
                logging.info("[SAVE][shares_outstanding][{date}][流通股數計算完成（重新下載更新）]".format(date=datetime2str(timestamp)))
            
            # 檢查變動的股數資料，並打印
            common_ticker_list = list(set(shares_series.index).intersection(last_date_shares_series.index))
            share_change_series = shares_series[common_ticker_list] / last_date_shares_series[common_ticker_list]
            share_change_series = share_change_series[share_change_series != 1]
            
            if not share_change_series.empty:
                logging.info("[NOTE][shares_outstanding][{date}][本日流通股數變化如下：本日股數/前日股數]".format(date=datetime2str(timestamp)))
                logging.info(dict(share_change_series))
            
            # 檢查新增的標的，並打印
            new_ticker_list = list(set(shares_series.index) - set(common_ticker_list))
            if len(new_ticker_list) > 0:
                logging.info("[NOTE][shares_outstanding][{date}][本日新增標的如下]".format(date=datetime2str(timestamp)))
                logging.info(new_ticker_list)
                
            # 將資料轉換為列表格式，並添加至 data_list，準備儲存
            data_list.append(
                {"data_timestamp": timestamp,
                "created_timestamp": current_timestamp,
                "values": shares_series.to_dict()
            })
        
        # 儲存資料至資料庫
        if data_list:
            dao_instance = self._get_dao_instance("shares_outstanding")
            try:
                dao_instance.insert_many(data_list, unique_key="data_timestamp")
                logging.info(f"[SAVE][shares_outstanding][成功儲存 {len(data_list)} 筆資料]")
            except Exception as e:
                logging.error(f"[SAVE][shares_outstanding][資料儲存失敗: {e}]")