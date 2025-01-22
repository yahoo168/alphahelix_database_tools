from .base_data import BaseDAO
import pandas as pd
from datetime import datetime
from typing import List

class UniverseDAO(BaseDAO):
    def __init__(self, collection_name: str, uri: str):
        self.db_name = "Universe"
        super().__init__(self.db_name, collection_name, uri)
    
    def _remove_delist_ticker(self, ticker_list):
        """
        排除已下市的 ticker。
        下市 ticker 格式：7 位數字 + 字母（D 或 Q）。
        """
        return [ticker for ticker in ticker_list if len(ticker) < 8]
        
    # 覆寫BaseDAO的原始資料轉換函數
    def transform_data_df(self, ticker_df, exclude_delist=False):
        if ticker_df.empty:
            raise ValueError("ticker_df 為空，無法轉換")

        # 使用 stack 将 DataFrame 转换为长格式，忽略 NaN
        stacked_df = ticker_df.stack().reset_index()
        stacked_df.columns = ['data_timestamp', 'dummy_column', 'ticker']
        
        # 删除无意义的 dummy_column 和 NaN 的 ticker
        stacked_df = stacked_df.drop(['dummy_column'], axis=1).dropna(subset=['ticker'])
        
        # 使用 crosstab 创建布尔矩阵
        universe_df = pd.crosstab(
            stacked_df['data_timestamp'],
            stacked_df['ticker']
        ).astype(bool)

        # 若需要排除已下市的 ticker
        if exclude_delist:
            valid_tickers = self._remove_delist_ticker(universe_df.columns.tolist())
            universe_df = universe_df[valid_tickers]
        
        return universe_df
    
    def get_universe_tickers(self, start_timestamp: datetime = None, end_timestamp: datetime = None, num: int = None) -> List[str]:
        """
        根據指定的 Universe 項目和範圍，獲取 Universe 中的 tickers。

        Args:
            start_timestamp (datetime, optional): 範圍開始日期。
            end_timestamp (datetime, optional): 範圍結束日期。
            num (int, optional): 對應的資料筆數。

        Returns:
            List[str]: Universe 中的 tickers 列表。
        """
        # 檢查輸入參數
        if not ((start_timestamp and end_timestamp) or num):
            raise ValueError("必須指定 start_timestamp 和 end_timestamp，或指定 num")

        # 組裝查詢條件
        query = {}
        if start_timestamp and end_timestamp:
            query["data_timestamp"] = {"$gte": start_timestamp, "$lte": end_timestamp}
        elif num is not None and end_timestamp is not None:
            query["data_timestamp"] = {"$lte": end_timestamp}
        
        # 執行查詢
        result = self.find(
            query=query, 
            projection={"values": 1, "_id": 0}, 
            sort=[("data_timestamp", -1)], 
            limit=num
        )

        # 提取並展開所有 values，去重並排序
        tickers = set()
        for item in result:
            # 確保 item["values"] 是列表，並展開到集合中
            tickers.update(item.get("values", []))
        
        return sorted(tickers)

class UnivSPX500DAO(UniverseDAO):
    def __init__(self, uri):
        collection_name = "univ_spx500"
        # 直接傳遞固定的 db_name，而非使用 self.db_name
        super().__init__(collection_name, uri)

class UnivDOW30DAO(UniverseDAO):
    def __init__(self, uri):
        collection_name = "univ_dow30"
        super().__init__(collection_name, uri)

class UnivNDX100DAO(UniverseDAO):
    def __init__(self, uri):
        collection_name = "univ_ndx100"
        super().__init__(collection_name, uri)

class UnivRAY3000DAO(UniverseDAO):
    def __init__(self, uri):
        collection_name = "univ_ray3000"
        super().__init__(collection_name, uri)
        
class UnivUsStock(UniverseDAO):
    def __init__(self, uri):
        collection_name = "univ_us_stock"
        super().__init__(collection_name, uri)
        
class UnivUsETF(UniverseDAO):
    def __init__(self, uri):
        collection_name = "univ_us_etf"
        super().__init__(collection_name, uri)