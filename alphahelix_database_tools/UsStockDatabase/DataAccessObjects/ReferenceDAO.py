from alphahelix_database_tools.UsStockDatabase.DataAccessObjects.BaseDAO import BaseDAO
from typing import List, Union
from datetime import datetime, timedelta
from alphahelix_database_tools.utils.datetime_utils import str2datetime, datetime2str
import logging

class MarketStatusDAO(BaseDAO):
    def __init__(self, uri):
        db_name = "Reference"
        collection_name = "market_status"
        super().__init__(db_name, collection_name, uri)
        
    def get_trade_date_list(self, start_timestamp:datetime, end_timestamp:datetime) -> List[datetime]:
        # 驗證輸入參數類型
        if not isinstance(start_timestamp, datetime) or not isinstance(end_timestamp, datetime):
            raise ValueError("start_timestamp 和 end_timestamp 必須是 datetime 類型")

        market_status_series = self.get_item_df_by_datetime(start_timestamp, end_timestamp).squeeze()
        
        # 篩選出交易日，即market_status為True(1)的index
        if not market_status_series.empty:
            trade_date_series = market_status_series[market_status_series==1].index
            return list(trade_date_series)
        else:
            logging.warning(f"[WARN][trade_date][{datetime2str(start_timestamp)}~{datetime2str(end_timestamp)} does not exist]")
            return list()
    
    # 取得距離指定日期最近的交易日，計算方式可選往前（last）或往後（next），預設為往前（last）
    # cal_self可選擇給定的日期本身若為交易日是否納入計算，預設為True
    def get_closest_trade_date(self, timestamp:Union[datetime, str], direction:str="last", cal_self:bool=True) -> datetime:
        if isinstance(timestamp, str):
            timestamp = str2datetime(timestamp)
        
        if direction not in {"last", "next"}:
            raise ValueError("direction 必須是 'last' 或 'next'")
        
        start_timestamp = timestamp - timedelta(days=10)
        end_timestamp = timestamp + timedelta(days=10)
        
        # 取出指定日期前後10日的資料
        market_status_series = self.get_item_df_by_datetime(start_timestamp, end_timestamp).squeeze()
        
        # 若資料為空，直接返回異常
        if market_status_series.empty:
            raise ValueError(f"在範圍 {start_timestamp} ~ {end_timestamp} 未找到任何交易日")
        
        # 設定初始值和方向
        current_date = timestamp
        step = -1 if direction == "last" else 1

        # 檢查當前日期是否應該納入計算
        if not cal_self or market_status_series.get(current_date, 0) != 1:
            current_date += timedelta(days=step)

        # 循環查找最近的交易日
        while True:
            if current_date not in market_status_series.index:
                raise ValueError(f"無法在 {start_timestamp} ~ {end_timestamp} 找到有效的交易日")

            if market_status_series[current_date] == 1:
                return current_date

            current_date += timedelta(days=step)