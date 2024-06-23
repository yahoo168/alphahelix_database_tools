from .database import *

class US_STOCK_DATABASE(DATABASE):
    """docstring for US_STOCK_DATABASE"""
    def __init__(self, ROOT_PATH):
        # 設定本資料集名稱
        self.DATA_STACK_NAME = "US_STOCK"
        # 上雲待改：API應另行統一管理
        self.polygon_API_key = "vzrcQO0aPAoOmk3s_WEAs4PjBz4VaWLj"
        self.DATA_ROOT_PATH = os.path.join(ROOT_PATH, "data")
        self.CONFIG_ROOT_PATH = os.path.join(ROOT_PATH, "config")
        self.META_DATA_PATH = os.path.join(self.CONFIG_ROOT_PATH, "meta_data.xlsx")
        self.EXTREME_VALUE_LOG_PATH = os.path.join(self.CONFIG_ROOT_PATH, "data_value_check", "extreme_value_log")
        self._load_fixed_meta_data()
        self._parse_and_build_data_path()
        self._load_variable_meta_data()

    # universe ticker處理相關函數（開始）
    ## 儲存當前正在交易的所有ticker，預設資料源：polygon
    def save_stock_universe_list(self, universe_name="univ_us_stock", start_date=None, end_date=None, source="polygon"):
        if start_date == None:
            # 若未指定起始/結束日期，則自資料最新儲存資料日開始抓取資料
            start_date = self.get_single_data_status(item=universe_name)["end_date"]

        if end_date == None:
            end_date = datetime2str(datetime.today())

        trade_date_list = self.get_trade_date_list(start_date=start_date, end_date=end_date)
        if source=="polygon":
            data_dict = save_stock_universe_list_from_polygon(API_key=self.polygon_API_key, universe_name=universe_name, date_list=trade_date_list)
        
        # 上雲待改：儲存檔案
        folder_path = self.get_data_path(item=universe_name)
        for date in data_dict.keys():
            file_path = os.path.join(folder_path, date+".csv")
            data_dict[date].to_csv(file_path)

        logging.info("[SAVE][{universe_name}]成分股資料[{start_date}~{end_date}]儲存完成".format(universe_name=universe_name, start_date=start_date, end_date=end_date))        

    # 取得指定日期下，特定universe成分股list（底層函數為self.get_stock_universe_df）
    # 若不指定日期，預設為最新1筆
    def get_stock_universe_list(self, universe_name, date=None, exclude_delist=False):        
        # 若未給定日期，則預設取出最新一筆資料
        if date == None:
            date = self.get_single_data_status(item=universe_name)["end_date"]
        # 將起始/結束日期設為同一日，以取出單一筆資料
        ticker_series = self.get_stock_universe_df(universe_name=universe_name, start_date=date, end_date=date, 
                                                 exclude_delist=exclude_delist).squeeze()
        return ticker_series.index.to_list()

    ## 取得特定universe的df資料，row:日期，columns:曾經存在此universe的所有ticker，value:True/False
    def get_stock_universe_df(self, universe_name, start_date, end_date, exclude_delist=False):
        # BBG對下市ticker會更改為7位數字+一個字母（D或Q），如2078185D，可以此判別
        # 待改：應透過下市股票對照表處理
        def remove_delist_ticker(ticker_list):
            ticker_list = [ticker for ticker in ticker_list if len(ticker)<8]
            return ticker_list

        raw_ticker_df = self.get_item_data_df_by_date(item=universe_name, 
                                                      start_date=start_date, end_date=end_date)

        # raw_data中每日儲存的為universe中的ticker序列，故須取出時間區段中所有存在過的ticker
        all_universe_ticker_series = pd.Series(raw_ticker_df.values.flatten()).drop_duplicates().dropna()
        date_list = raw_ticker_df.index.to_list()
        # row：時間序列，column：完整的ticker序列，value：True/False
        ticker_df = pd.DataFrame(index=date_list, columns=all_universe_ticker_series)
        
        # 參照raw_data，若ticker在當日被包含於universe中，便填寫True
        for date in date_list:
            ticker_series = raw_ticker_df.loc[date, :].dropna()
            ticker_df.loc[date, ticker_series] = True
        
        # 空值則補Fasle，代表該ticker當日未被包含於universe中
        ticker_df = ticker_df.fillna(False)
        # 可選擇是否去除已下市股票
        if exclude_delist == True:
            ticker_list = list(ticker_df.columns)
            ticker_list = remove_delist_ticker(ticker_list)
            return ticker_df.loc[:, ticker_list]
        
        else:
            return ticker_df

    ## 儲存股票分割資料raw_data
    def save_stock_split_data(self, ticker_list=None, start_date=None, end_date=None, source="polygon"):
        if start_date == None:
            # 若未給定起始日，則自最新一筆資料的隔日開始抓取
            start_date = self.get_single_data_status(item="stock_splits")["end_date"]
            start_date = datetime2str(str2datetime(start_date) + timedelta(days=1))

        if end_date == None:
            # 若未給定結束日，則預設會抓取至明日；因下單須預知隔日分割情況以計算下單參考價
            end_date = datetime2str(datetime.today() + timedelta(days=1))

        # 取得交易日序列，並以此索引資料
        trade_date_list = self.get_trade_date_list(start_date=start_date, end_date=end_date)
        if source == "polygon":
            data_dict = save_stock_split_from_Polygon(API_key=self.polygon_API_key, date_list=trade_date_list)
        
        # 上雲待改：儲存檔案
        folder_path = self.get_data_path(item="stock_splits")
        for date in data_dict.keys():
            file_path = os.path.join(folder_path, date+".csv")
            data_dict[date].to_csv(file_path)
    
    ## 待改：目前僅是複製前一天的資料
    def save_stock_industry_data(self, start_date=None, end_date=None):
        item_list = [
            "gics_sector", "gics_sector_name", "gics_industry_group", "gics_industry_group_name",
            "gics_industry", "gics_industry_name", "gics_sub_industry", "gics_sub_industry_name",
            ]

        for item in item_list:
            if start_date == None:
                # 若未給定起始日，則自最新一筆資料的隔日開始抓取
                last_date = self.get_single_data_status(item=item)["end_date"]
                start_date = datetime2str(str2datetime(last_date) + timedelta(days=1))

            if end_date == None:
                end_date = datetime2str(datetime.today())

            # 取得交易日序列，並以此索引資料
            trade_date_list = self.get_trade_date_list(start_date=start_date, end_date=end_date)
            # 上雲待改：如何複製 & 儲存檔案
            # Note：因last_date沒有即時更新，整個區間的資料皆是複製第N天的資料，而非迭代複製前一天
            folder_path = self.get_data_path(item=item)
            for date in trade_date_list:
                src_file_path = os.path.join(folder_path, last_date+".csv")
                dst_file_path = os.path.join(folder_path, date+".csv")
                shutil.copyfile(src_file_path, dst_file_path)

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
        
        trade_date_list = self.get_trade_date_list(start_date=start_date, end_date=end_date)
        stock_splits_df = self.get_item_data_df_by_date(item="stock_splits", start_date=start_date, end_date=end_date)
        adjust_factor_df = _cal_stock_adjust_factor_df(stock_splits_df, date_list=trade_date_list, method=method)
        return adjust_factor_df

    # 儲存股票價量資料raw_data
    def save_stock_priceVolume_data(self, ticker_list=None, start_date=None, end_date=None, 
                                    item_list=None, adjust=False, source="polygon"):
        # 若未指定起始/結束日期，則自open最新儲存資料日的下一日開始抓取
        if start_date == None:
            start_date = self.get_single_data_status(item="open")["end_date"]
            start_date = datetime2str(str2datetime(start_date) + timedelta(days=1))

        if end_date == None:
            end_date = datetime2str(datetime.today())

        folder_path = self.get_data_path(item="priceVolume")
        
        if source == "polygon":
            save_stock_priceVolume_from_Polygon(folder_path, self.polygon_API_key, start_date, end_date, adjust)
        
        elif source == "yfinance":
            cache_folder_path = os.path.join(self.cache_folder_path, "yfinance_priceVolume")
            #待改：cache folder須統一管理
            make_folder(cache_folder_path)
            save_stock_priceVolume_from_yfinance(folder_path, cache_folder_path, start_date, end_date, ticker_list, adjust)

        elif source == "yahoo_fin":
            pass
    
    # 計算並儲存open_to_open以及close_to_close的raw_data
    def save_stock_daily_return(self, start_date=None, end_date=None, method="c2c", cal_dividend=True):        
        if method == "c2c":
            item_name, price_item = "c2c_ret", "close"

        elif method == "o2o":
            item_name, price_item = "o2o_ret", "open"

        folder_path = self.get_data_path(item=item_name)
        
        if start_date == None:
            # 若未指定起始/結束日期，則自資料最新儲存資料日開始抓取資料
            # 不遞延一天，係因若日報酬更新至t日，為計算t+1日的日報酬，須取得t日的價格與分割資料
            start_date = self.get_single_data_status(item=item_name)["end_date"]

        if end_date == None:
            end_date = datetime2str(datetime.today())

        if start_date == end_date:
            logging.warning("[{date}]資料已更新至今日，預設無須進行更新，若須強制更新須輸入起始/結束參數".format(date=start_date))

            return 0

        price_df = self.get_item_data_df_by_date(item=price_item, start_date=start_date, end_date=end_date)
        adjust_factor_df = self._get_stock_adjust_factor_df(start_date=start_date, end_date=end_date, method="backward")
        
        # 只針對有分割資料的個股作股價調整
        adjust_ticker_list = price_df.columns.intersection(adjust_factor_df.columns)        
        adjusted_item_df = price_df[adjust_ticker_list] * adjust_factor_df
        # 若不給定ticker_list，會導致賦值時，columns沒有對齊
        price_df[adjust_ticker_list] = adjusted_item_df[adjust_ticker_list]
        # 將股價加上當日除息的現金股利
        dividends_df = self.get_item_data_df_by_date(item="ex_dividends", start_date=start_date, end_date=end_date)
        dividends_df = dividends_df.fillna(0)
        dividends_ticker_list = price_df.columns.intersection(dividends_df.columns)
        adjusted_dividends_df = price_df[dividends_ticker_list] + dividends_df[dividends_ticker_list]
        price_df[dividends_ticker_list] = adjusted_dividends_df[dividends_ticker_list]
                
        # 去除第一row：因取pct後為空值
        return_df = price_df.pct_change().iloc[1:,:]

        # 切分return
        for i in range(len(return_df)):
            data_series = return_df.iloc[i,:].dropna()
            #去除index為NaN的情況（可能源自price資料異常）
            data_series = data_series[data_series.index.notna()]
            data_series = data_series.sort_index()
            date = datetime2str(data_series.name)
            file_path = os.path.join(folder_path, date+".csv")
            logging.info("[SAVE][{method} daily return][{date}] 資料儲存完成".format(date=date, method=method))
            data_series.to_csv(file_path)
    
    # 儲存現金股利raw_data
    def save_stock_cash_dividend(self, ticker_list=None, start_date=None, end_date=None, source="polygon"):
        if start_date == None:
            # 待改：一律使用ex_divi好像不太好（？
            start_date = self.get_single_data_status(item="ex_dividends")["end_date"]
            start_date = datetime2str(str2datetime(start_date) + timedelta(days=1))

        if end_date == None:
            end_date = datetime2str(datetime.today()+timedelta(days=1))
        
        # 待改，資料源管理
        folder_path = self.get_data_path(item="ex_dividends")
        save_stock_cash_dividend_from_Polygon(folder_path, self.polygon_API_key, start_date, end_date, div_type="ex_dividend_date")
        
        folder_path = self.get_data_path(item="pay_dividends")
        save_stock_cash_dividend_from_Polygon(folder_path, self.polygon_API_key, start_date, end_date, div_type="pay_date")
        # 因同一天公司可能會宣布N筆股利（不同發放日），較難儲存故先略過
        #folder_path = self.get_data_path(item="declaration_dividends")
        #save_stock_cash_dividend_from_Polygon(folder_path, self.polygon_API_key, start_date, end_date, div_type="declaration_date")
        
    # 儲存流通股數raw_data
    def save_stock_shares_outstanding(self, ticker_list=None, start_date=None, end_date=None, source="polygon"):
        if start_date == None:
            start_date = self.get_single_data_status(item="shares_outstanding")["end_date"]
            start_date = datetime2str(str2datetime(start_date) + timedelta(days=1))

        if end_date == None:
            end_date = datetime2str(datetime.today())
        # 因polygon須依據ticker逐一抓取股數，若每日更新較為費時，故僅在每月最後一日重新抓取
        # 其他日則依據前一日股數，參考split調整
        trade_date_list = self.get_trade_date_list(start_date=start_date, end_date=end_date)
        folder_path = self.get_data_path(item="shares_outstanding")
        
        for date in trade_date_list:
            last_trade_date = self.get_closest_trade_date(date, direction="last", cal_self=False)
            next_trade_date = self.get_closest_trade_date(date, direction="next", cal_self=False)
            last_date_shares_series = self.get_item_data_df_by_date(item="shares_outstanding", start_date=last_trade_date, end_date=last_trade_date).squeeze()
            stock_splits_df = self.get_item_data_df_by_date(item="stock_splits", start_date=date, end_date=date)
                    
            if len(stock_splits_df.columns) == 0:
                if_splits_exist = False
            else:
                if_splits_exist = True
                #須設定axis為0，否則當日僅有一檔標的split時，ticker會消失，降為成純量
                stock_splits_series = stock_splits_df.squeeze(axis=0)
            
            # 月底最後一交易日，重新抓取
            if str2datetime(date).month != str2datetime(next_trade_date).month:
                logging.info("[NOTE][shares outstanding][本日是本月最後一交易日，須重新更新股數計算基礎]")
                # 取得最新一筆美股所有上市公司清單
                last_univ_us_stock_update_date = self.get_start_date_by_num(item="univ_us_stock", end_date=date, num=1)
                univ_us_ticker_list = list(self.get_item_data_df_by_date(item="univ_us_stock", start_date=last_univ_us_stock_update_date, end_date=last_univ_us_stock_update_date).squeeze())
                # 因自polygon下載的股數，通常會延遲1~2日反應分割調整
                # 直接採用當日數據會導致出錯，故若重新下載日前2日曾進行分割，則該股不重新下載
                recent_splits_start_date = self.get_start_date_by_num(item="stock_splits", end_date=date, num=2)
                recent_splits_ticker_list = list(self.get_item_data_df_by_date(item="stock_splits", start_date=recent_splits_start_date, end_date=date).columns)
                ticker_list = sorted(list(set(univ_us_ticker_list) - set(recent_splits_ticker_list)))                
                # ticker_list = ticker_list[:3]
                result_dict = save_stock_shares_outstanding_from_Polygon(self.polygon_API_key, ticker_list=ticker_list, start_date=date, end_date=date)
                shares_series = result_dict[date]
                # 確認缺漏的標的：近日有作分割 + polygon資料源缺漏
                lost_ticker_set = set(univ_us_ticker_list) - set(list(shares_series.index))
                lost_ticker_list = list(lost_ticker_set.intersection(last_date_shares_series.index))
                # 針對缺漏的標的，改為依據前一日股數，參考split調整（即依照一般日算法）
                lost_ticker_last_date_shares_series = last_date_shares_series[lost_ticker_list]
                if if_splits_exist == False:
                    lost_ticker_shares_series = lost_ticker_last_date_shares_series
                else:
                    lost_ticker_shares_series = lost_ticker_last_date_shares_series.mul(stock_splits_series, fill_value=1).dropna()
                    lost_ticker_shares_series = lost_ticker_shares_series[lost_ticker_last_date_shares_series.index]
                    
                # 將缺漏標的資料，併入polygon重新下載而得的資料
                shares_series = pd.concat([shares_series, lost_ticker_shares_series])
                logging.info("[SAVE][{date}][流通股數計算完成（重新下載更新）]".format(date=date))

            # 非月底最後一交易日，依據前一日股數，參考split調整
            else:
                if if_splits_exist == False:
                    shares_series = last_date_shares_series
                else:
                    shares_series = last_date_shares_series.mul(stock_splits_series, fill_value=1).dropna()
                    # 若不重取index，在stock_split存在，但last_date_shares不存在的ticker會導致錯誤
                    # 因為fill value=1，該標的的股數會 = 1 * 分割數，故須剔除
                    shares_series = shares_series[last_date_shares_series.index]
                    
                logging.info("[SAVE][{date}][流通股數計算完成（依據前日基礎計算）]".format(date=date))

            file_name = os.path.join(folder_path, date+".csv")
            
            common_ticker_list = list(set(shares_series.index).intersection(last_date_shares_series.index))
            share_change_series = shares_series[common_ticker_list] / last_date_shares_series[common_ticker_list]
            share_change_series = share_change_series[share_change_series!=1]

            if len(share_change_series) > 0:
                logging.info("[NOTE][{date}][本日流通股數變化如下：本日股數/前日股數]".format(date=date))
                logging.info(dict(share_change_series))
            
            new_ticker_list = list(set(shares_series.index) - set(common_ticker_list))
            if len(new_ticker_list) > 0:
                logging.info("[NOTE][{date}][本日新增標的如下]".format(date=date))
                logging.info(new_ticker_list)
            
            shares_series.name = date
            shares_series.to_csv(file_name)
    
    def get_stock_financialReport_df(self, item, start_date, end_date):
        # 須往前多取資料，以便填補值
        pre_fetch_date = self.get_start_date_by_num(item=item, end_date=start_date, num=90)
        raw_financialReport_df = self.get_item_data_df_by_date(item=item, start_date=pre_fetch_date, end_date=end_date)
        trade_date_list = self.get_trade_date_list(start_date=pre_fetch_date, end_date=end_date)
        financialReport_df = pd.DataFrame(index=trade_date_list, columns=sorted(raw_financialReport_df.columns))
        financialReport_df.update(raw_financialReport_df)
        financialReport_df = financialReport_df.ffill()

        mask = (financialReport_df.index >= start_date) & (financialReport_df.index <= end_date)
        return financialReport_df[mask]

    # 待完成的函數（開始）
    # 資料品質控管（Raw_Table -> Table）
    def _get_filtered_raw_table_item_df(self, item, data_stack):
        raw_table_folder_path = self.get_data_path(item=item, data_level="raw_table")
        raw_table_file_path = os.path.join(raw_table_folder_path, item+".pkl")
        item_df = pd.read_pickle(raw_table_file_path)
        OHLC_list = ["open", "high", "low", "close"]
        # OHLC檢查項目: 中段空值檢查、上下市日期核對、分割資料核對
        if item in OHLC_list:
            adjust_factor_df = self._get_item_table_df(item="stock_splits", data_level="raw_table")
            interval_nan_ratio = cal_interval_nan_value(item_df)
            max_abs_zscore_series = cal_return_max_abs_zscore(item_df.copy(), adjust_factor_df)

            delete_ticker_list_1 = list(interval_nan_ratio[interval_nan_ratio > 0.1].index)
            delete_ticker_list_2 = list(max_abs_zscore_series[max_abs_zscore_series > 5].index)
            delete_ticker_list_all = list(set(delete_ticker_list_1) | set(delete_ticker_list_2))
            item_df = item_df.drop(delete_ticker_list_all, axis=1)

        elif item == "dividends":
            price_df = self._get_item_table_df(item="close", data_level="raw_table")
            dividends_ratio_df = item_df / price_df
            max_dividends_ratio_series = dividends_ratio_df.max()
            max_dividends_ratio_series = max_dividends_ratio_series[max_dividends_ratio_series > 0.8]
            delete_ticker_list_all = list(max_dividends_ratio_series.index)
            item_df = item_df.drop(delete_ticker_list_all, axis=1)

        elif item == "volume":
            pass

        return item_df

    def trans_stock_item_raw_table_to_table(self, item_list):
        for item in item_list:
            item_df = self._get_filtered_raw_table_item_df(item, data_stack=data_stack)
            table_folder_path = self.get_data_path(item=item, data_level="table")
            table_file_path = os.path.join(table_folder_path, item+".pkl")
            item_df.to_pickle(table_file_path)
    
    # 確認標的成分股變化（預設為univ_us_stock）
    # compare_date_num：和幾天前的資料比，預設為1
    def _check_component_change(self, date=datetime2str(datetime.today()), universe_name="univ_us_stock", compare_date_num=1):        
        last_update_date = self.get_single_data_status(item=universe_name)["end_date"]
        if date > last_update_date:
            logging.warning("[CHECK]成分股資料未更新，比對結果可能出錯")
        
        # 取得universe資料中，指定日期（date）的前2筆資料日期
        benchmark_date = self.get_start_date_by_num(item=universe_name, end_date=date, num=compare_date_num+1)
        ticker_df = self.get_stock_universe_df(universe_name=universe_name, start_date=benchmark_date, end_date=date, exclude_delist=False)
        
        #因ticker_df為T/F形式，錯位相減後為0代表情況不變，為1代表新上市（前日為0，此日為1），為-1代表新下市（前日為-1，此日為0）
        change_signal_series = (ticker_df - ticker_df.shift(compare_date_num)).iloc[-1, :]
        new_component_list = list(change_signal_series[change_signal_series==1].index)
        deleted_component_list = list(change_signal_series[change_signal_series==-1].index)
        return new_component_list, deleted_component_list
    
    # 待新增的資料項目：基本面資料、總經資料
    def save_stock_financialReport_data(self, ticker_list=None, start_date=None, end_date=None, 
                                    item_list=None, source="polygon"):
        if start_date == None:
            start_date = str2datetime(self.get_single_data_status(item="filing_date")["end_date"])
            start_date = datetime2str(start_date + timedelta(days=1))

        if end_date == None:
            end_date = datetime2str(datetime.today())

        folder_path_dict = self.data_path_dict
        trade_date_list = self.get_trade_date_list(start_date=start_date, end_date=end_date)
        if source == "polygon":
            data_dict = save_stock_financialReport_data_from_Polygon(API_key=self.polygon_API_key, date_list=trade_date_list)
        
        for date in data_dict.keys():
            data_df = data_dict[date]
            # 儲存發布日資料
            filing_date_folder_path = self.get_data_path(item="filing_date")
            filing_date_file_path = os.path.join(filing_date_folder_path, date+".csv")
            filing_date = pd.Series(data_df.columns).rename("filing_date")
            filing_date.to_csv(filing_date_file_path)
        
            # 儲存財報資料
            for i in range(len(data_df)):
                data_series = data_df.iloc[i, :]
                item_name = data_series.name
                # 部分科目過於冷門，若不在2023/07/10已註冊的70多個科目當中，便略過
                try:
                    folder_path = self.get_data_path(item=item_name)
                    file_path = os.path.join(folder_path, date+".csv")
                    data_series.to_csv(file_path)
                except Exception as e:
                    print(e)
                    continue
    
    def save_stock_company_info(self, ticker_list=None, source="polygon"):
        folder_path = self.get_data_path(item="company_info")
        file_path = os.path.join(folder_path, "company_info.csv")
        # 取得自檔案上次更新以來，新增的ticker列表
        if ticker_list == None:
            old_company_info_df = pd.read_csv(file_path, index_col=0)
            old_ticker_list = list(old_company_info_df["ticker"])
            new_ticker_list = self.get_stock_universe_list("univ_us_stock", date=TODAY_DATE_STR, exclude_delist=False)
            add_ticker_list = sorted(list(set(new_ticker_list) - set(old_ticker_list)))
            ticker_list = add_ticker_list.copy()

        # 取得新增標的的公司資料df
        if source == "polygon":
            new_company_info_df = save_stock_company_info_from_Polygon(API_key=self.polygon_API_key, ticker_list=ticker_list) 
        
        # 與原公司資料df結合，一併儲存
        old_company_info_df = pd.read_csv(file_path, index_col=0)
        
        if len(ticker_list)>0 and len(new_company_info_df)>0:
            company_info_df = pd.concat([old_company_info_df, new_company_info_df], ignore_index=True)
            logging.info("[SAVE] 更新{N}檔標的之公司資訊，標的如下：".format(N=len(ticker_list)))
            logging.info(ticker_list)
            company_info_df.to_csv(file_path)

    # 為表狀資料
    def save_stock_delisted_info(self, source="polygon"):
        if source == "polygon":
            folder_path = self.get_data_path(item="delisted_stock_info")
            save_stock_delisted_info_from_polygon(folder_path=folder_path, API_key=self.polygon_API_key, universe_type="CS")
            
            folder_path = self.get_data_path(item="delisted_etf_info")
            save_stock_delisted_info_from_polygon(folder_path=folder_path, API_key=self.polygon_API_key, universe_type="ETF")

    def get_stock_item_df_dict(self, item_list, start_date=None, end_date=datetime2str(datetime.today()), 
                                    num=None, method="by_date", if_align=False):
        item_df_list = list()
        for item in item_list:
            logging.info("[GET][{item}][正在組合資料][資料區間{start_date}-{end_date}]".format(item=item, start_date=start_date, end_date=end_date))
            # 若取用方式設定為依照最新N筆數量，則計算取用起始日，再調用組合函數
            if method=="by_num":
                start_date = self.get_start_date_by_num(item=item, end_date=end_date, num=num)
            
            # 部分資料組合為塊狀資料後，尚須額外處理，如adjust_factor, universe_ticker...等
            if item in ["univ_ray3000", "univ_ndx100", "univ_dow30", "univ_spx500"]:
                item_df = self.get_stock_universe_df(universe_name=item, start_date=start_date, end_date=end_date, exclude_delist=False)
            
            # raw data中stock_splits為該日有分割才有資料，在轉化為raw table時進行補日期 & 計算調整係數
            elif item == "stock_splits":
                item_df = self._get_stock_adjust_factor_df(start_date=start_date, end_date=end_date, country=country, method="backward")
            
            elif item == "trade_date":
                #將series轉為df
                item_df = self.get_stock_market_status_series(start_date=start_date, end_date=end_date).to_frame()
            
            # 待改：如何對基本面資料列表？
            else:
                item_df = self.get_item_data_df_by_date(item=item, start_date=start_date, end_date=end_date)

            # 部分column（ticker）可能為Nan
            if np.nan in item_df.columns:
                item_df = item_df.drop(np.nan, axis=1)
            
            item_df_list.append(item_df)

        if if_align == True:
            item_df_list = get_aligned_df_list(item_df_list)

        return dict(zip(item_list, item_df_list)) 

        ## 資料錯誤檢測機制：詳見https://www.notion.so/c525507cf1c04b4e8da757c457bd9c2a?pvs=4
    
    # 檢查L1資料：資料未收進，即檔案缺失
    def check_data_loss(self):
        # 取得日頻資料列表
        item_list = self.get_item_list(by_attribute="freq", attribute="day")
        # 逐一檢查資料end_date是否早於今日日期
        for item in item_list:
            end_date = self.meta_data_dict[item]["end_date"]
            if TODAY_DATE_STR > end_date:
                logging.info("[WARN][{item}]最新資料日期為{end_date}，今日資料缺失（待確認）".format(item=item, end_date=end_date))
    
    # 檢查L3資料錯誤：資料存在基本錯誤，如high < low
    def check_data_basic_error(self):
        # Rule_1: high < low
        data_dict = dict()
        for item in ["open", "high", "low", "close"]:
            data_dict[item] = self.get_item_data_df_by_num(item=item, num=1)
        
        error_test_series = (data_dict["high"] < data_dict["low"])
        if error_test_series.any() == True:
            logging.info("[WARN][DATA CHECK]資料存在L3錯誤（基本錯誤）：high < low，錯誤資料ticker如下：")
            logging.info(data_dict["high"][error_test_series==True].index.tolist())
            return False
        
        logging.info("[NOTE][DATA CHECK] 未檢測出L3錯誤(basic error)")
    
    # 檢查L4/L5錯誤：資料存在異常值，若為SP500成分股則優先處理，其餘存為log檔
    def check_data_extreme(self):
        item_list = ["c2c_ret", "o2o_ret"]
        univ_spx500_ticker_list = self.get_stock_universe_list(universe_name="univ_spx500")
        
        extreme_item_data_dict = dict()
        
        for item in item_list:
            start_date = self.get_start_date_by_num(item=item, end_date=TODAY_DATE_STR, num=1)
            item_series = self.get_item_data_df_by_date(item=item, start_date=start_date, end_date=TODAY_DATE_STR).squeeze()
            UP_THRESHOLD = 2
            DOWN_THRESHOLD = -0.5
            # 若是報酬超過門檻，跳出警示（WS結尾的通常不用理會）
            mask = (item_series >= UP_THRESHOLD) | (item_series <= DOWN_THRESHOLD)
            extreme_item_series = item_series[mask].sort_values()
            
            important_extreme_ticker_list = list(extreme_item_series.index.intersection(univ_spx500_ticker_list))
            if len(important_extreme_ticker_list) > 0:
                logging.info("[WARN][{item}]以下資料變動幅度較大且為S&P500成分股，可能存在異常".format(item=item))
                logging.info(important_extreme_ticker_list)
                logging.info(extreme_item_series[important_extreme_ticker_list])
            
            extreme_item_data_dict[item] = dict(extreme_item_series)
        
        # 將異常值紀錄存為log（.json格式），dict(item -> ticker -> value)
        file_path = os.path.join(self.EXTREME_VALUE_LOG_PATH, start_date+".json")
        with open(file_path, "w") as f:
            json.dump(extreme_item_data_dict, f)