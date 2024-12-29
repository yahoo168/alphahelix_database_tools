from alphahelix_database_tools.UsStockDatabase.DataAccessObjects.BaseDAO import BaseDAO
from alphahelix_database_tools.utils.datetime_utils import str2datetime, datetime2str

from typing import List, Union
from datetime import datetime, timedelta
import pandas as pd
import logging

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
    def _transform_data_df(self, raw_ticker_df, exclude_delist=False):
        """
        將 ticker DataFrame 轉換為指定格式的 DataFrame。

        行：日期。
        列：曾經存在此 universe 的所有 ticker。
        值：True（當日存在該 ticker），False（當日不存在該 ticker）。

        Args:
            raw_ticker_df (pd.DataFrame): 原始 ticker 的 DataFrame，每行是日期，每列是當日的 ticker。
            exclude_delist (bool): 是否排除已下市的 ticker。預設為 False。

        Returns:
            pd.DataFrame: 格式化後的 ticker DataFrame。
        """

        # 確認輸入的 raw_ticker_df 是有效的 DataFrame
        if raw_ticker_df.empty:
            raise ValueError("raw_ticker_df 為空，無法轉換")

        # 提取所有出現過的 ticker，去重並排除空值
        all_universe_tickers = pd.unique(raw_ticker_df.values.flatten())
        all_universe_tickers = [ticker for ticker in all_universe_tickers if pd.notna(ticker)]

        # 建立空的 DataFrame，行：日期，列：所有的 ticker，值預設為 False
        formatted_ticker_df = pd.DataFrame(
            False,
            index=raw_ticker_df.index,
            columns=all_universe_tickers
        )

        # 填充 DataFrame 中的值：若 ticker 存在於當日，設為 True
        for date, tickers in raw_ticker_df.iterrows():
            formatted_ticker_df.loc[date, tickers.dropna()] = True

        # 若需要排除已下市的 ticker
        if exclude_delist:
            valid_tickers = self._remove_delist_ticker(formatted_ticker_df.columns.tolist())
            formatted_ticker_df = formatted_ticker_df[valid_tickers]

        return formatted_ticker_df

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