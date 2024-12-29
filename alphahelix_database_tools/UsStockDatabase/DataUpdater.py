from datetime import datetime, timedelta, timezone

from alphahelix_database_tools.UsStockDatabase.DataManager import UsStockDataManager
from alphahelix_database_tools.external_tools.polygon_tools import *
from alphahelix_database_tools.utils.datetime_utils import TODAY_DATE_STR, str2datetime, datetime2str

class UsStockUpdater(UsStockDataManager):
    def __init__(self, uri: str):
        super().__init__(uri)  # 调用父类的初始化方法
        self.uri = uri
        self.polygon_API_key = "RYILBtDAe679w4cMQCeTd1Kfl2_s8HBV"
    
    def update_stock_OHLCV_data(self, start_date=None, end_date=None, adjust=False, source="polygon"):
        if start_date == None:
            start_date = self.get_latest_data_date(item="close")
            start_date = start_date + timedelta(days=1)
        
        if end_date == None:
            end_date = TODAY_DATE_STR
        
        if source == "polygon":
            data_dict = save_stock_OHLCV_from_Polygon(self.polygon_API_key, start_date, end_date, adjust)
        else:
            raise ValueError(f"[ERROR][Unsupport Source]: {source}")
        
        item_list = ["open", "high", "low", "close", "volume"]
        # item_list.extend(["avg_price", "transaction_num"])
        
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
            
            item_dao.insert_many(item_data_list)
            
    
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
            
            start_date = datetime2str(data_list[0]["data_timestamp"])
            end_date = datetime2str(data_list[-1]["data_timestamp"])
            
            # Insert the data into the database
            dao_instance = self._get_dao_instance("market_status")
            dao_instance.insert_many(data_list)
            logging.info("[SAVE][market_status][{start_date}~{end_date}]儲存完成".format(start_date=start_date, end_date=end_date))
            
        else:
            logging.info("[SAVE][market_status][資料已更新至最新日期]")
    
    def update_stock_split_data(self, start_date=None, end_date=None, source="polygon"):
        """
        儲存股票分割資料 (stock splits)
        """
        # 設定時間範圍
        if start_date is None:
            # 若未給定起始日，則從最新資料的隔日開始
            latest_date = self.get_latest_data_date(item="stock_split")
            start_date = datetime2str(latest_date + timedelta(days=1))

        if end_date is None:
            # 若未給定結束日，則預設抓取至隔日
            end_date = datetime2str(datetime.today() + timedelta(days=1))

        # 根據資料來源抓取資料
        if source == "polygon":
            data_dict = save_stock_split_from_Polygon(self.polygon_API_key, start_date, end_date)
            
        else:
            logging.error(f"[SAVE][stock_splits][不支持的來源: {source}]")
            return

        # 統一時間戳
        created_timestamp = datetime.now(timezone.utc)

        # 轉換資料為列表格式
        data_list = [
            {
                "data_timestamp": str2datetime(date),
                "created_timestamp": created_timestamp,
                "values": values
            }
            for date, values in ((date, s.to_dict()[date]) for date, s in data_dict.items())
        ]
        
        # 儲存資料至資料庫
        if data_list:
            dao_instance = self._get_dao_instance("stock_split")
            try:
                dao_instance.insert_many(data_list)
                logging.info(f"[SAVE][stock_splits][成功儲存 {len(data_list)} 筆資料]")
            except Exception as e:
                logging.error(f"[SAVE][stock_splits][資料儲存失敗: {e}]")
        else:
            logging.info("[SAVE][stock_splits][資料已更新至最新日期]")

    # 儲存現金股利資料
    def update_stock_cash_dividend(self, start_date=None, end_date=None, item="ex_dividend", source="polygon"):
        if start_date == None:
            start_date = self.get_latest_data_date(item)
            start_date = datetime2str(start_date + timedelta(days=1))

        if end_date == None:
            end_date = datetime2str(datetime.today() + timedelta(days=1))
        
        if source == "polygon":
            trans_dict = {"ex_dividend": "ex_dividend_date", "pay_dividend": "pay_date"} # polygon字段轉換字典
            
            data_dict = save_stock_cash_dividend_from_Polygon(self.polygon_API_key, 
                                                            start_date, 
                                                            end_date, 
                                                            div_type=trans_dict[item])
        else:
            raise ValueError(f"source: {source} is not supported.")
        
        if not data_dict:
            logging.info("[NOTE][update_stock_cash_dividend][此區間不存在資料]")
            return
        
        created_timestamp = datetime.now(timezone.utc)
        data_list = [
                    {
                        "data_timestamp": str2datetime(date),
                        "created_timestamp": created_timestamp,
                        "values": values
                    }
                    for date, values in data_dict.items()
                ]
        
        # 儲存資料至資料庫
        if data_list:
            dao_instance = self._get_dao_instance(item)
            try:
                dao_instance.insert_many(data_list)
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
        
        print(start_timestamp)
        print(end_timestamp)
        if start_timestamp == end_timestamp:
            logging.warning("[{date}]資料已更新至今日，預設無須進行更新，若須強制更新須輸入起始/結束參數".format(date=start_timestamp))
            return

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
            logging.info(f"[SAVE][{item}][{datetime2str(date)}][資料已計算完成]")
        
        # 儲存資料至資料庫
        if data_list:
            dao_instance = self._get_dao_instance(item)
            try:
                dao_instance.insert_many(data_list)
                logging.info(f"[SAVE][{item}][成功儲存 {len(data_list)} 筆資料]")
            except Exception as e:
                logging.error(f"[SAVE][{item}][資料儲存失敗: {e}]")