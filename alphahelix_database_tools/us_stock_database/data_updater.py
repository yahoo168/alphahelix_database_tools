from datetime import datetime, timedelta, timezone

from alphahelix_database_tools.us_stock_database.data_manager import UsStockDataManager
from alphahelix_database_tools.data_scrapers.polygon_tools import *
from alphahelix_database_tools.utils.datetime_utils import TODAY_DATE_STR, str2datetime, datetime2str

class UsStockUpdater(UsStockDataManager):
    def __init__(self, username, password):
        super().__init__(username, password)  # 调用父类的初始化方法
        self.polygon_API_key = "RYILBtDAe679w4cMQCeTd1Kfl2_s8HBV"
    
    def _get_stock_adjust_factor_df(self, start_timestamp=None, end_timestamp=None, method="backward"):
        def _cal_stock_adjust_factor_df(stock_splits_df, date_list, method):
            adjust_ticker_list = stock_splits_df.columns
            adjust_factor_df = pd.DataFrame(index=date_list, columns=adjust_ticker_list)
            adjust_factor_df[adjust_ticker_list] = stock_splits_df[adjust_ticker_list]    
            adjust_factor_df = adjust_factor_df.fillna(1)
            adjust_factor_df[adjust_factor_df==0] = 1
            
            if method == "forward":
                adjust_factor_df = adjust_factor_df.cumprod()
                
            elif method == "backward":
                cumulative_splits = adjust_factor_df.cumprod().iloc[-1,:]
                adjust_factor_df = (1/adjust_factor_df).cumprod() * cumulative_splits
                adjust_factor_df = 1/adjust_factor_df

            else:
                raise Exception("method typo")

            adjust_factor_df.index = pd.to_datetime(adjust_factor_df.index)
            return adjust_factor_df
        
        trade_date_list = self.get_trade_date_list(start_timestamp=start_timestamp, end_timestamp=end_timestamp)
        stock_splits_df = self.get_item_df(item="stock_split", method="by_date", start_timestamp=start_timestamp, end_timestamp=end_timestamp)
        adjust_factor_df = _cal_stock_adjust_factor_df(stock_splits_df, date_list=trade_date_list, method=method)

        return adjust_factor_df
    
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
            
            start_date = datetime2str(data_list[0]["data_timestamp"])
            end_date = datetime2str(data_list[-1]["data_timestamp"])
            
            # Insert the data into the database
            dao_instance = self._get_dao_instance("market_status")
            dao_instance.insert_many(data_list, unique_key="data_timestamp")
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
                dao_instance.insert_many(data_list, unique_key="data_timestamp")
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
        
        if start_timestamp == end_timestamp:
            logging.warning("[{date}]資料已更新至今日，預設無須進行更新，若須強制更新須輸入起始/結束參數".format(date=start_timestamp))
            return

        price_df = self.get_item_df(item=price_type, method="by_date", start_timestamp=start_timestamp, end_timestamp=end_timestamp)
        
        # 取得股價調整因子，因須計算當日股利報酬，使用forward法可還原當日股價
        adjust_factor_df = self._get_stock_adjust_factor_df(start_timestamp=start_timestamp, end_timestamp=end_timestamp, method="forward")        
        
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
                dao_instance.insert_many(data_list, unique_key="data_timestamp")
                logging.info(f"[SAVE][{item}][成功儲存 {len(data_list)} 筆資料]")
            except Exception as e:
                logging.error(f"[SAVE][{item}][資料儲存失敗: {e}]")
    
    def update_stock_universe_ticker(self, item, start_date=None, end_date=None, source="polygon"):
        if start_date is None:
            start_date = self.get_latest_data_date(item=item)
            start_date = datetime2str(start_date + timedelta(days=1))

        if end_date is None:
            end_date = TODAY_DATE_STR

        if source == "polygon":
            data_dict = save_stock_universe_ticker_from_polygon(self.polygon_API_key, item, start_date, end_date)
        else:
            raise ValueError(f"[SAVE][{item}][source not found]")
        
        # 依照起始/結束日期，取得交易日list
        trade_date_list = self.get_trade_date_list(start_date, end_date)
            
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
    def update_stock_shares_outstanding(self, start_date=None, end_date=None, source="polygon"):
        if start_date == None:
            start_date = self.get_latest_data_date("shares_outstanding")
            start_date = datetime2str(start_date + timedelta(days=1))

        if end_date == None:
            end_date = datetime2str(datetime.today())

        logging.info("[SAVE][流通股數][{start_date} ~ {end_date}]".format(start_date=start_date, end_date=end_date))
        if source != "polygon":
            raise ValueError("[SAVE][流通股數][不支持的來源: {source}]".format(source=source))
        
        # 因polygon須依據ticker逐一抓取股數，若每日更新較為費時，故僅在每月最後一日重新抓取
        # 其他日則依據前一日股數，參考split調整
        trade_date_list = self.get_trade_date_list(start_timestamp=start_date, end_timestamp=end_date)
        
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
            # 須設定axis為0，否則當日僅有一檔標的split時，ticker會消失，降維成純量
            stock_splits_series = self.get_item_df(item="stock_split", method="by_num", end_timestamp=timestamp, num=1).squeeze(axis=0)
            
            # 若非月底最後一交易日，則依據前一日股數，參考split調整
            if if_last_trade_date_of_the_moth == False:
                # 若無split則單純複製前一日shares，若有split，以前一日shares * split = 當日shares
                shares_series = last_date_shares_series.copy(deep=True)
                adjusted_shares_series = (last_date_shares_series * stock_splits_series).dropna()
                shares_series.update(adjusted_shares_series)                        
                logging.info("[SAVE][{date}][流通股數計算完成（依據前日基礎計算）]".format(date=datetime2str(timestamp)))
                
                # 月底最後一交易日，重新抓取  
            else:
                logging.info("[NOTE][shares outstanding][本日是本月最後一交易日，須重新更新股數計算基礎]")
                # 取得最新一筆美股所有上市公司清單
                univ_us_ticker_list = self.get_latest_universe_tickers(universe_item="univ_us_stock")
                
                # 因polygon流通股數資料，通常會延遲1~2日反應split調整，若直接下載當日數據會出錯，故若重新下載日之前2日，該股曾進行split，則該股不重新下載，改回依據前日股數參考split調整
                recent_splits_df = self.get_item_df(item="stock_split", method="by_num", end_timestamp=timestamp, num=2)
                recent_splits_ticker_list = list(recent_splits_df.columns)
                target_ticker_list = sorted(list(set(univ_us_ticker_list) - set(recent_splits_ticker_list)))                
                
                # 下載流通股數資料（polygon）
                data_dict = save_stock_shares_outstanding_from_Polygon(API_key=self.polygon_API_key, ticker_list=target_ticker_list, start_date=timestamp, end_date=timestamp)
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
                logging.info("[SAVE][{date}][流通股數計算完成（重新下載更新）]".format(date=datetime2str(timestamp)))

            common_ticker_list = list(set(shares_series.index).intersection(last_date_shares_series.index))
            share_change_series = shares_series[common_ticker_list] / last_date_shares_series[common_ticker_list]
            share_change_series = share_change_series[share_change_series != 1]

            if not share_change_series.empty:
                values = shares_series.to_dict()
                data_list.append(
                    {"data_timestamp": timestamp,
                    "created_timestamp": current_timestamp,
                    "values": values
                })
                
                logging.info("[NOTE][{date}][本日流通股數變化如下：本日股數/前日股數]".format(date=datetime2str(timestamp)))
                logging.info(dict(share_change_series))

            new_ticker_list = list(set(shares_series.index) - set(common_ticker_list))
            if len(new_ticker_list) > 0:
                logging.info("[NOTE][{date}][本日新增標的如下]".format(date=datetime2str(timestamp)))
                logging.info(new_ticker_list)
        
        # 儲存資料至資料庫
        if data_list:
            dao_instance = self._get_dao_instance("shares_outstanding")
            try:
                dao_instance.insert_many(data_list, unique_key="data_timestamp")
                logging.info(f"[SAVE][{"shares_outstanding"}][成功儲存 {len(data_list)} 筆資料]")
            except Exception as e:
                logging.error(f"[SAVE][{"shares_outstanding"}][資料儲存失敗: {e}]")