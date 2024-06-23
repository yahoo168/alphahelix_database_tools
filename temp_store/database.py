import os, sys, logging, time
import pandas as pd
import numpy as np
import pickle, json, string
import shutil, random
from threading import Thread, Lock
from datetime import datetime, date, timedelta
#from yahoo_fin.stock_info import *
from fredapi import Fred

#import yfinance as yf
from .utils import *
from .polygon_tools import *
from .yfinance_tools import *
from .yahoo_fin_tools import *

"""

資料命名規則：

- 日期採"2024-01-01"格式
- 資料庫中股號間的特殊字符皆以 "_" 作為連接符，如：BRK_B, TW_0050, ...
- 函數前綴"_"表僅供DATABASE class所調用的內部函數

"""

## 設定logging格式（避免使用print）
logging.basicConfig(level=logging.INFO,
    format = '[%(asctime)s %(levelname)-8s] %(message)s',
    datefmt = '%Y%m%d %H:%M:%S',)

class DATABASE(object):
    # 上雲待改：進入cloud後如何讀取？
    # 載入資料庫之靜態元資料（fixed meta data）
    def _load_fixed_meta_data(self):
        item_meta_data_dict = dict()
        meta_data_df = pd.read_excel(self.META_DATA_PATH, index_col=0)
        self.meta_data_df = meta_data_df
        self.meta_data_dict = meta_data_df.to_dict(orient='index')
        
    # 上雲待改：應有統一儲存函數
    def save_item_data(self):
        pass
    
    # 載入資料庫之動態元資料（variable meta data)（目前僅有資料起始/最新更新日期/資料筆數）
    def _load_variable_meta_data(self):
        # 取得序列型(series type)資料項目(item)
        item_list = self.get_item_list_by_attribute(by_attribute="type", attribute="series")
        # 計算各資料起始/結束日期（dict形式）
        data_status_dict = self._get_data_status_dict(item_list=item_list)
        # 結合靜態（fixed）與動態（variable）之meta data後，重新填入meta_data_dict
        self.meta_data_dict = combine_dict(self.meta_data_dict, data_status_dict)
        # 依據meta_data_dict建立dataframe，以便依照attribute篩選
        self.meta_data_df = pd.DataFrame(self.meta_data_dict).T
    
    # 依照各資料集config/meta_data的data_path載入資料路徑，並自動建立對應的資料夾層級結構
    def _parse_and_build_data_path(self):
        data_path_dict = dict()
        # 資料依層級結構可歸類為四級"class", "sub_class_1", "sub_class_2", "sub_class_3"
        # 如：最高價（high）class=priceVolume，sub_class_1=high，詳見meta_data.xlsx
        path_config_df = self.meta_data_df.loc[:, ["class", "sub_class_1", "sub_class_2", "sub_class_3"]]
        # 逐行讀取路徑後，建立實體資料夾層級
        for i in range(len(path_config_df)):
            path_series = path_config_df.iloc[i, :]
            item, path_list = path_series.name, list(path_series.dropna())
            item_folder_path = os.path.join(self.DATA_ROOT_PATH, *path_list)
            data_path_dict[item] = item_folder_path
            make_folder(item_folder_path)
        # 建立資料路徑字典，以便調用資料路徑
        self.DATA_PATH_DICT = data_path_dict

    # 給定資料項目（item），回傳其元資料（meta data）
    def get_meta_data_dict_by_item(self, item):
        return self.meta_data_dict[item]

    # 透過meta data篩選item，如by_attribute="freq", attribute="day"，可列出資料更新頻率為日頻的資料項目
    def get_item_list_by_attribute(self, by_attribute=None, attribute=None):
        meta_data_df = self.meta_data_df.copy()
        if by_attribute == None:
            item_list = list(meta_data_df.index)

        if by_attribute != None:
            meta_data_series = meta_data_df[by_attribute]
            meta_data_series = meta_data_series[meta_data_series==attribute]
            item_list = list(meta_data_series.index)
        return item_list

    # 指定item，返回對應的資料儲存路徑
    # 待改：應新增雲端or local
    def get_data_path(self, item, location="local"):
        return self.DATA_PATH_DICT[item]
    
    # 取得多項資料儲存狀況：起始日期/最新更新日期/資料筆數，回傳Dataframe
    def _get_data_status_dict(self, item_list):
        status_dict = dict()
        for item in item_list:
            sub_status_dict = self.get_single_data_status(item=item)
            status_dict[item] = sub_status_dict
        return status_dict

    # 取得單一資料項目的儲存狀況：起始日期/最新更新日期/資料筆數，回傳dict
    def get_single_data_status(self, item):
        item_folder_path = self.get_data_path(item=item)
        status_dict = dict()
        # 若該項目不存在raw_data，則返回空值（項目資料夾不存在 or 雖存在但其中沒有檔案，皆視為raw_data不存在）
        if not os.path.exists(item_folder_path) or len(os.listdir(item_folder_path))==0:
            status_dict["start_date"], status_dict["end_date"], status_dict["date_num"] = None, None, 0
        
        #待改：部分資料非時序型資料，如trade_date，待處理
        else:
            # 取得指定項目資料夾中的檔案名
            raw_date_list = os.listdir(item_folder_path)
            # 去除後綴檔名（如.csv）
            date_list = [date.split(".")[0] for date in raw_date_list]
            # 去除異常空值（由隱藏檔所導致）
            date_list = [date for date in date_list if len(date)!=0]
            # 建立date series後排序，以取得資料的起始、結束日
            date_series = pd.Series(date_list)
            date_series = date_series.apply(lambda x:str2datetime(x)).sort_values().reset_index(drop=True)
            start_date, end_date = date_series.iloc[0].strftime("%Y-%m-%d"), date_series.iloc[-1].strftime("%Y-%m-%d")
            date_num = len(date_series)
            status_dict["start_date"], status_dict["end_date"], status_dict["date_num"] = start_date, end_date, date_num

        return status_dict

    # 給定特定資料之指定日期(end_date)與所需的資料筆數(num)，回傳對應的起始日（因應部分資料取用需求須給定資料筆數）
    def get_start_date_by_num(self, item, end_date, num):
        item_folder_path = self.get_data_path(item=item)
        raw_date_list = os.listdir(item_folder_path)
        date_list = [date.split(".")[0] for date in raw_date_list]
        # 去除異常空值（由mac隱藏檔所導致）
        date_list = [date for date in date_list if len(date)!=0]
        date_series = pd.Series(date_list)
        date_series = date_series.apply(lambda x:str2datetime(x)).sort_values().reset_index(drop=True)
        date_series = date_series[date_series <= end_date]
        # 依照結束日截取N筆資料，回傳起始日
        start_date = datetime2str(list(date_series.iloc[-num:,])[0])
        return start_date
    
    # 待改：這樣沒有經過資料轉化（如univ）
    # 取出最新N筆（預設為1）資料，N若為1，回傳格式為series，否則回傳格式為df
    def get_item_data_df_by_num(self, item, num=1):
        start_date = self._get_start_date_by_num(item=item, end_date=TODAY_DATE_STR, num=num)
        item_df = self._get_item_data_df_by_date(item=item, start_date=start_date, end_date=TODAY_DATE_STR).squeeze()
        return item_df

    # 將序列型資料（series data）取出後組合為df
    # 可指定起始日（start_date）、結束日（end_date）、往前延伸的資料筆數（pre_fetch_nums）
    # 可指定目標標的列表（target_ticker_list），預設為不指定，即全部取出
    def get_item_data_df_by_date(self, item, target_ticker_list=None, start_date=None, end_date=None, pre_fetch_nums=0):
        item_folder_path = self.get_data_path(item=item)
        if type(start_date) is not str:
            start_date = datetime2str(start_date)
        if type(end_date) is not str:
            end_date = datetime2str(end_date)

        #item_folder_path = os.path.join(folder_path, item)
        # 取得指定資料夾中所有檔案名(以時間戳記命名）
        raw_date_list = os.listdir(item_folder_path)
        # 去除檔案名中的後綴名（.csv)
        date_list = [date.split(".")[0] for date in raw_date_list]
        # 去除異常空值（可能由.DS_store等隱藏檔所導致）
        date_list = [date for date in date_list if len(date) != 0]
        # 將時間戳記組合成date series，以篩選出指定的資料區間（同時進行排序與重設index）
        date_series = pd.Series(date_list).apply(lambda x:str2datetime(x)).sort_values().reset_index(drop=True)
        # 向前額外取N日資料，以使策略起始時便有前期資料可供計算
        start_date = shift_days_by_strDate(start_date, -pre_fetch_nums)
        mask = (date_series >= start_date) & (date_series <= end_date) 
        # 將date series轉為將date list（字串列表），以用於後續讀取資料
        date_list = list(map(lambda x:datetime2str(x), date_series[mask]))
        # 依照date list逐日取出每日資料，並組合為DataFrame
        df_list = list()
        for date in date_list:
            fileName = os.path.join(item_folder_path, date+".csv")
            df = pd.read_csv(fileName, index_col=0)
            df_list.append(df)

        if len(df_list)==0:
            logging.warning("[NOTE][{item}][{start_date}-{end_date}]區間不存在資料".format(item=item,
                            start_date=start_date, end_date=end_date))
            return pd.DataFrame()
        
        df = pd.concat(df_list, axis=1).T
        # 對Index以日期序列賦值並排序
        df.index = pd.to_datetime(date_list)
        df = df.sort_index()

        # 若不指定ticker，則預設為全部取出
        if target_ticker_list == None:
            return df
        # 若指定ticker，比對取出的資料所包含的ticker與目標ticker是否存在差距
        else:
            lost_ticker_list = list(set(target_ticker_list) - set(df.columns))
            # 篩選出目標ticker與資料庫ticker的交集，以避免loc報錯
            re_target_ticker_list = list(set(target_ticker_list) - set(lost_ticker_list))
            if len(lost_ticker_list) > 0:
                logging.warning("資料項目:{}共{}檔標的缺失，缺失標的如下:".format(item, len(lost_ticker_list)))
                logging.warning(lost_ticker_list)

            return df.loc[:, re_target_ticker_list]    

    # 取得市場狀態序列(market_status_series)，index:Date; value:1: 交易日, 0:六日休市, -1:非六日休市（國定假日）
    # Note：須定時補新的market status
    # Note：應區分不同資產類別，如債券or外匯有時會在股票不開市時交易
    def get_market_status_series(self, start_date=None, end_date=None):
        # 讀取資料集中的trade_date.csv檔
        folder_path = self.get_data_path(item="trade_date")
        file_path = os.path.join(folder_path, "trade_date.csv")
        market_status_series = pd.read_csv(file_path, index_col=0).squeeze()
        # 原csv檔案的日期的日期為字串，格式為2000/01/01，須轉換為datetime格式
        market_status_series.index = pd.DatetimeIndex(market_status_series.index)
        # 若有指定區間，則進行區間篩選，否則回傳全部資料
        if start_date != None and end_date != None:
            return market_status_series[start_date:end_date]

        return market_status_series

    # 取得交易日日期序列（字串列表），若不指定區間則預設為全部取出
    def get_trade_date_list(self, start_date=None, end_date=None):
        # 取得market_status_series：{index:Date; value:1: 交易日, 0:六日休市, -1:非六日休市（國定假日）}
        market_status_series = self.get_market_status_series(start_date=start_date, end_date=end_date)
        # 篩選出交易日，即market_status為True(1)的index
        trade_date_series = market_status_series[market_status_series==True]
        trade_date_list = list(map(datetime2str, trade_date_series.index))
        return trade_date_list

    # 取得距離指定日期最近的交易日，計算方式可選往前（last）或往後（next），預設為往前（last）
    # cal_self可選擇給定的日期本身若為交易日是否納入計算，預設為True
    def get_closest_trade_date(self, date, direction="last", cal_self=True):
        date = str2datetime(date)
        market_status_series = self.get_market_status_series()
        
        if cal_self == True:
            market_status = market_status_series[date]
        else:
            market_status = 0
        
        if direction == "last":
            step = -1
        elif direction == "next":
            step = 1
        
        while True:
            if market_status != 1:
                date = date + timedelta(days=step)
                market_status = market_status_series[date]
            else:
                return datetime2str(date)