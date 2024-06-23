from .AbstractCloudDatabase import *

class CloudUsStockDatabase(AbstractCloudDatabase):
    def __init__(self, config_folder_path):
        super().__init__(config_folder_path=config_folder_path)  # 調用父類 MDB_DATABASE 的__init__方法
        self.polygon_API_key = "vzrcQO0aPAoOmk3s_WEAs4PjBz4VaWLj"
        self.rapid_API_key = "5eeaf20b6dmsh06b146a0f8df7d6p1fb4c8jsnb19977dfeebf"
    
    def get_stock_item_df_dict(self, item_list, start_date=None, end_date=TODAY_DATE_STR, 
                          num=None, method="by_date", if_align=False):
        item_df_list = list()
        for item in item_list:
            item_df = self.get_item_df(item=item, method=method, start_date=start_date, end_date=end_date, num=num)
            item_df_list.append(item_df)
            # 待改：新增資料轉化部分（思考：有沒有比較好的流程？）
            
        if if_align == True:
            item_df_list = get_aligned_df_list(item_df_list)

        return dict(zip(item_list, item_df_list))

    # 待改：trade_date底層資料應改為market_status
    # 取得交易日日期序列（datetime列表），若不指定區間則預設為全部取出
    def get_stock_trade_date_list(self, start_date=None, end_date=None):
        return self.get_trade_date_list(asset_type="US_stock", start_date=start_date, end_date=end_date)
    
    def get_stock_closest_trade_date(self, date, direction="last", cal_self=True):
        return self.get_closest_trade_date(asset_type="US_stock", date=date, direction=direction, cal_self=cal_self)
        
    def save_stock_priceVolume_data(self, start_date=None, end_date=None, 
                                    adjust=False, source="polygon"):
        if start_date == None:
            start_date = self.get_latest_data_date(item="close")
            start_date = start_date + timedelta(days=1)
        
        if end_date == None:
            end_date = TODAY_DATE_STR
        
        if source == "polygon":
            data_dict = save_stock_priceVolume_from_Polygon(self.polygon_API_key, start_date, end_date, adjust)
        
        # 接入的資料格式：{date:{"AAPL":100, "OXY": 80}}
        # 轉換後待入庫的資料格式：[{"date": date, "AAPL":100, "OXY":80}]
        item_list = ["open", "high", "low", "close", "volume", "avg_price", "transaction_num"]
        for item in item_list:
            item_data_dict = data_dict[item]
            for date in item_data_dict.keys():
                item_data_dict[date]["date"] = str2datetime(date)
            item_data_list = list(item_data_dict.values())
            
            if len(item_data_list) > 0:
                self.save_data_to_MDB(item=item, data_list=item_data_list)
            else:
                logging.info(f"[NOTE][{item}][{datetime2str(start_date)}-{datetime2str(end_date)}不存在未更新資料]")
                
    def save_stock_market_status(self, source="polygon"):
        if source == "polygon":
            market_status_series = save_stock_market_status_from_Polygon(API_key=self.polygon_API_key)
            
        latest_data_date = self.get_latest_data_date(item="market_status_US_stock")
        # 僅截取資料庫內尚未涵蓋的資料上傳
        market_status_series = market_status_series[market_status_series.index > latest_data_date]
        if len(market_status_series) > 0:
            data_list = [{"date":date, "market_status":market_status} for date, market_status in market_status_series.to_dict().items()]
            start_date = datetime2str(data_list[0]["date"])
            end_date = datetime2str(data_list[-1]["date"])
            logging.info("[SAVE][market_status][{start_date}~{end_date}]儲存完成".format(start_date=start_date, end_date=end_date))
            self.save_data_to_MDB(item="market_status_US_stock", data_list=data_list)
        else:
            logging.info("[SAVE][market_status][資料已更新至最新日期]")
    
    ## 儲存股票分割資料raw_data
    def save_stock_split_data(self, start_date=None, end_date=None, source="polygon"):
        if start_date == None:
            # 若未給定起始日，則自最新一筆資料的隔日開始抓取
            latest_date = self.get_latest_data_date(item="stock_splits")
            start_date = datetime2str(latest_date + timedelta(days=1))

        if end_date == None:
            # 若未給定結束日，則預設會抓取至明日；因下單須預知隔日分割情況以計算下單參考價
            end_date = datetime2str(datetime.today() + timedelta(days=1))

        # 取得交易日序列，並以此索引資料
        trade_date_list = self.get_stock_trade_date_list(start_date=start_date, end_date=end_date)
        trade_date_list = datetime2str_list(trade_date_list)

        if source == "polygon":
            data_dict = save_stock_split_from_Polygon(API_key=self.polygon_API_key, date_list=trade_date_list)
        
        data_list = list()
        for date, s in data_dict.items():
            s_dict = s.to_dict()[date]
            s_dict["date"] = str2datetime(date)
            data_list.append(s_dict)
            
        if len(data_list) > 0:
            self.save_data_to_MDB(item="stock_splits", data_list=data_list)
        else:
            logging.info("[SAVE][stock_splits][資料已更新至最新日期]")
            
    # 計算並儲存open_to_open return以及close_to_close return
    def save_stock_daily_return(self, start_date=None, end_date=None, method="c2c", cal_dividend=True):
        if method == "c2c":
            item_name, price_item = "c2c_ret", "close"

        elif method == "o2o":
            item_name, price_item = "o2o_ret", "open"
            
        if start_date == None:
            # 若未指定起始/結束日期，則自資料最新儲存資料日開始抓取資料
            # 不遞延一天，係因若日報酬更新至t日，為計算t+1日的日報酬，須取得t日的價格與分割資料
            start_date = self.get_latest_data_date(item=item_name)

        if end_date == None:
            end_date = str2datetime(TODAY_DATE_STR)
        
        if start_date == end_date:
            logging.warning("[{date}]資料已更新至今日，預設無須進行更新，若須強制更新須輸入起始/結束參數".format(date=start_date))
            return 0

        price_df = self._get_item_data_df_by_date(item=price_item, start_date=start_date, end_date=end_date)
        adjust_factor_df = self._get_stock_adjust_factor_df(start_date=start_date, end_date=end_date, method="forward")        
        dividends_df = self._get_item_data_df_by_date(item="ex_dividends", start_date=start_date, end_date=end_date)
        dividends_ret_df = (dividends_df / price_df).fillna(0)
        # 只針對有分割資料的個股作股價調整
        adjust_ticker_list = price_df.columns.intersection(adjust_factor_df.columns)        
        #若不給定ticker_list，會導致賦值時，columns沒有對齊
        price_df[adjust_ticker_list] = price_df[adjust_ticker_list] * adjust_factor_df[adjust_ticker_list]
        return_df = price_df.pct_change()
        # 加上當日除息的現金股利
        return_df = return_df + dividends_ret_df
        # 以price_df再過濾一次
        return_df = return_df.mask(price_df.isna())
        # 去除第一row：因取pct後為nan
        return_df = return_df.iloc[1: ]
        
        data_dict = dict()
        for date in return_df.index:
            return_series = return_df.loc[date, :].dropna()
            #去除index為NaN的情況（可能源自price資料異常）
            return_series = return_series[return_series.index.notna()]
            if len(return_series) > 0:
                data_dict[date] = return_series.to_dict()
                logging.info("[SAVE][{daily_return}][{date}][資料已切分完成]".
                         format(daily_return=item_name, date=datetime2str(date)))
   
        data_list = self.trans_data_dict_to_MDB_data_list(data_dict)
        self.save_data_to_MDB(item=item_name, data_list=data_list)
        logging.info("[SAVE][{daily_return}][{start_date}-{end_date}][資料已儲存完成]".
                         format(daily_return=item_name, start_date=datetime2str(return_df.index[0]), end_date=datetime2str(return_df.index[-1])))
    
    
    # 取得調整因子df，預設為backward方法（使當前adjclose等同於close）（股價 * 調整因子 = 調整價）
    def _get_stock_adjust_factor_df(self, start_date=None, end_date=None, method="backward"):
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
        
        trade_date_list = self.get_stock_trade_date_list(start_date=start_date, end_date=end_date)
        stock_splits_df = self.get_item_df(item="stock_splits", method="by_date", start_date=start_date, end_date=end_date)
        adjust_factor_df = _cal_stock_adjust_factor_df(stock_splits_df, date_list=trade_date_list, method=method)

        return adjust_factor_df
    
    # {"2024-01-01":{"A":a, "B":a}} -> [{"date":"2024-01-01", "A":a, "B":a}, ...]
    def trans_data_dict_to_MDB_data_list(self, data_dict):
        data_list = list()
        for date, d in data_dict.items():
            if type(date) is str:
                date = str2datetime(date)
            d["date"] = date
            data_list.append(d)
        return data_list
    
    # 儲存現金股利資料
    def save_stock_cash_dividend(self, ticker_list=None, start_date=None, end_date=None, source="polygon"):
        if start_date == None:
            start_date = self.get_latest_data_date(item="ex_dividends")
            start_date = datetime2str(start_date + timedelta(days=1))

        if end_date == None:
            end_date = datetime2str(datetime.today() + timedelta(days=1))
        
        ## ex_dividends
        if source == "polygon":
            data_dict = save_stock_cash_dividend_from_Polygon(self.polygon_API_key, start_date, end_date, div_type="ex_dividend_date")
            data_list = self.trans_data_dict_to_MDB_data_list(data_dict)
            
        if len(data_list) > 0:
            self.save_data_to_MDB(item="ex_dividends", data_list=data_list)
        else:
            logging.info("[NOTE][ex_dividends][此區間不存在資料]")
            
        ## pay_dividends
        if source == "polygon":
            data_dict = save_stock_cash_dividend_from_Polygon(self.polygon_API_key, start_date, end_date, div_type="pay_date")
            data_list = self.trans_data_dict_to_MDB_data_list(data_dict)
            
        if len(data_list) > 0:
            self.save_data_to_MDB(item="pay_dividends", data_list=data_list)
        else:
            logging.info("[NOTE][pay_dividends][此區間不存在資料]")
        
    def save_stock_universe_ticker(self, universe_name, start_date=None, end_date=None, source="polygon"):
        if start_date == None:
            start_date = self.get_latest_data_date(item=universe_name)
            start_date = datetime2str(start_date + timedelta(days=1))

        if end_date == None:
            end_date = TODAY_DATE_STR
        
        # 因時差問題，若太早更新可能報錯
        if start_date > end_date:
            logging.info[f"[SAVE][{universe_name}][data has been updated]"]
            return
        
        # 依照起始/結束日期，取得交易日list，並轉化為str格式
        date_list = self.get_stock_trade_date_list(start_date=start_date, end_date=end_date)
        date_list = datetime2str_list(date_list)

        if source == "polygon":
            data_dict = save_stock_universe_ticker_from_polygon(API_key=self.polygon_API_key, universe_name=universe_name, date_list=date_list)
            data_list = self.trans_data_dict_to_MDB_data_list(data_dict)

        if len(data_list) > 0:
            self.save_data_to_MDB(item=universe_name, data_list=data_list)
    
    # 儲存流通股數raw_data
    # Note: 已與舊資料校驗，函數功能正常，然而polygon shares資料品質較差，經常更動（主要是小型股，spx500偶爾也有），須定期用BBG刷新
    def save_stock_shares_outstanding(self, ticker_list=None, start_date=None, end_date=None, source="polygon"):
        if start_date == None:
            start_date = self.get_latest_data_date(item="shares_outstanding")
            start_date = datetime2str(start_date + timedelta(days=1))

        if end_date == None:
            end_date = TODAY_DATE_STR
    
        if source == "polygon":
            # 因polygon須依據ticker逐一抓取股數，若每日更新較為費時，故僅在每月最後一日重新抓取
            # 其他日則依據前一日股數，參考split調整
            trade_date_list = self.get_stock_trade_date_list(start_date=start_date, end_date=end_date)
            data_list = list()
            for date in trade_date_list:
                # 取得該日的上一交易日與下一交易日
                last_trade_date = self.get_stock_closest_trade_date(date=date, direction="last", cal_self=False)
                next_trade_date = self.get_stock_closest_trade_date(date=date, direction="next", cal_self=False)

                # 判斷是否為該月最後一交易日
                if_last_trade_date_of_the_moth = False
                if date.month != next_trade_date.month:
                    if_last_trade_date_of_the_moth = True
                
                # 取得該日的上一交易日之流通股數資料
                last_date_shares_series = self.get_item_df(item="shares_outstanding", method="by_num", end_date=last_trade_date, num=1).squeeze(axis=0)
                # 須設定axis為0，否則當日僅有一檔標的split時，ticker會消失，降維成純量
                stock_splits_series = self.get_item_df(item="stock_splits", method="by_num", end_date=date, num=1).squeeze(axis=0)
               
                # 若非月底最後一交易日，則依據前一日股數，參考split調整
                if if_last_trade_date_of_the_moth == False:
                    # 若無split則單純複製前一日shares，若有split，以前一日shares * split = 當日shares
                    shares_series = last_date_shares_series.copy(deep=True)
                    adjusted_shares_series = (last_date_shares_series * stock_splits_series).dropna()
                    shares_series.update(adjusted_shares_series)                        
                    logging.info("[SAVE][{date}][流通股數計算完成（依據前日基礎計算）]".format(date=datetime2str(date)))
                    
                 # 月底最後一交易日，重新抓取  
                else:
                    logging.info("[NOTE][shares outstanding][本日是本月最後一交易日，須重新更新股數計算基礎]")
                    # 取得最新一筆美股所有上市公司清單
                    univ_us_ticker_list = list(self.get_item_df(item="univ_us_stock", method="by_num", end_date=last_trade_date, num=1).squeeze())
                    # 因自polygon下載的股數，通常會延遲1~2日反應分割調整，若直接下載當日數據會出錯，故若重新下載日之前2日，該股曾進行分割，則該股不重新下載
                    recent_splits_df = self.get_item_df(item="stock_splits", method="by_num", end_date=date, num=2)
                    recent_splits_ticker_list = list(recent_splits_df.columns)
                    target_ticker_list = sorted(list(set(univ_us_ticker_list) - set(recent_splits_ticker_list)))                

                    data_dict = save_stock_shares_outstanding_from_Polygon(API_key=self.polygon_API_key, ticker_list=target_ticker_list, date_list=[datetime2str(date)])
                    shares_series = data_dict[datetime2str(date)]
                    # 取得缺漏標的：近日有作分割 + polygon資料源缺漏
                    lost_ticker_set = set(univ_us_ticker_list) - set(list(shares_series.index))
                    # 在缺漏標的中，過濾出昨日即有股數資料的標的
                    lost_ticker_list = list(lost_ticker_set.intersection(last_date_shares_series.index))

                    # 針對缺漏的標的，改為依據前一日股數，參考split調整（即依照一般日算法）
                    lost_ticker_shares_series = last_date_shares_series[lost_ticker_list]
                    lost_ticker_shares_series.update((lost_ticker_shares_series * stock_splits_series).dropna())

                    # 將缺漏標的資料，併入polygon重新下載而得的資料
                    shares_series = pd.concat([shares_series, lost_ticker_shares_series]).sort_index()
                    logging.info("[SAVE][{date}][流通股數計算完成（重新下載更新）]".format(date=datetime2str(date)))

                common_ticker_list = list(set(shares_series.index).intersection(last_date_shares_series.index))
                share_change_series = shares_series[common_ticker_list] / last_date_shares_series[common_ticker_list]
                share_change_series = share_change_series[share_change_series != 1]

                if len(share_change_series) > 0:
                    logging.info("[NOTE][{date}][本日流通股數變化如下：本日股數/前日股數]".format(date=datetime2str(date)))
                    logging.info(dict(share_change_series))

                new_ticker_list = list(set(shares_series.index) - set(common_ticker_list))
                if len(new_ticker_list) > 0:
                    logging.info("[NOTE][{date}][本日新增標的如下]".format(date=datetime2str(date)))
                    logging.info(new_ticker_list)
                
                data_dict = shares_series.to_dict()
                data_dict["date"] = date
                data_list.append(data_dict)
        
        #self.save_data_to_MDB(item="shares_outstanding", data_list=data_list)

    # 取得最新的ticker_list
    def get_latest_univ_ticker_list(self, univ_name):
        ticker_df = self.get_item_df(item=univ_name, method="by_num", num=1)
        return list(ticker_df.iloc[-1])

    # deprecated
    def get_stock_news_dict(self, ticker_list, start_date):
        news_meta_dict = dict()
        all_news_df = self.get_item_df(item="raw_stock_news", method="by_date", start_date=start_date)
        for ticker in ticker_list:
            # 篩選出包含此ticker的news
            news_df = all_news_df[all_news_df["ticker"].apply(lambda x: ticker in x)]
            if len(news_df) == 0:
                continue
            
            # 因資料庫中可能存在重複的新聞，故刪除
            news_df = news_df.drop_duplicates("url")
            news_df = news_df.loc[:, ["title", "url"]]
            news_df = news_df.reset_index()
            news_df["date"] = datetime2str_list(news_df["date"])
            news_meta_list = news_df.to_dict("records")
            news_meta_dict[ticker] = news_meta_list

        return news_meta_dict