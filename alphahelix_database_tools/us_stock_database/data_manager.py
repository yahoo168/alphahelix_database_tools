from datetime import datetime
from typing import Union, List, Dict
from functools import lru_cache
import logging

from .data_model import * # Import all DAO classes

class UsStockDataManager:
    def __init__(self, username, password):
        self.uri = f"mongodb+srv://{username}:{password}@usstockdata.x1hayah.mongodb.net/?retryWrites=true&w=majority&appName=UsStockData"

        # 整併 DAO 類別與能力描述
        self.dao_info = {
            # Price & Volume
            "open": {"class": OpenDAO, "is_universe_specifiable": True, "is_tickers_specifiable": True},
            "high": {"class": HighDAO, "is_universe_specifiable": True, "is_tickers_specifiable": True},
            "low": {"class": LowDAO, "is_universe_specifiable": True, "is_tickers_specifiable": True},
            "close": {"class": CloseDAO, "is_universe_specifiable": True, "is_tickers_specifiable": True},
            "volume": {"class": VolumeDAO, "is_universe_specifiable": True, "is_tickers_specifiable": True},
            "c2c_ret": {"class": CloseToCloseReturnDAO, "is_universe_specifiable": True, "is_tickers_specifiable": True},
            "o2o_ret": {"class": OpenToOpenReturnDAO, "is_universe_specifiable": True, "is_tickers_specifiable": True},
            "shares_outstanding": {"class": SharesOutstandingDAO, "is_universe_specifiable": True, "is_tickers_specifiable": True},
            
            # Actions
            "stock_split": {"class": StockSplitDAO, "is_universe_specifiable": True, "is_tickers_specifiable": True},
            "ex_dividend": {"class": ExDividendDAO, "is_universe_specifiable": True, "is_tickers_specifiable": True},
            "pay_dividend": {"class": PayDividendDAO, "is_universe_specifiable": True, "is_tickers_specifiable": True},
            
            # Universe
            "univ_spx500": {"class": UnivSPX500DAO, "is_universe_specifiable": False, "is_tickers_specifiable": False},
            "univ_dow30": {"class": UnivDOW30DAO, "is_universe_specifiable": False, "is_tickers_specifiable": False},
            "univ_ndx100": {"class": UnivNDX100DAO, "is_universe_specifiable": False, "is_tickers_specifiable": False},
            "univ_ray3000": {"class": UnivRAY3000DAO, "is_universe_specifiable": False, "is_tickers_specifiable": False},
            "univ_us_stock": {"class": UnivUsStock, "is_universe_specifiable": False, "is_tickers_specifiable": False},
            "univ_us_etf": {"class": UnivUsETF, "is_universe_specifiable": False, "is_tickers_specifiable": False},
            
            # Reference
            "market_status": {"class": MarketStatusDAO, "is_universe_specifiable": False, "is_tickers_specifiable": False},
            "gics_code": {"class": GicsCodeDAO},
            "gics_mapping": {"class": GicsMappingDAO},
            
        }

    @lru_cache # 緩存 DAO 實例（避免重複初始化）
    def _get_dao_instance(self, item: str):
        """
        延遲初始化 DAO 實例。

        Args:
            item (str): 資料項目名稱。

        Returns:
            BaseDAO: 資料項目對應的 DAO 實例。
        """
        dao_info = self.dao_info.get(item)
        if not dao_info:
            raise ValueError(f"無效的資料項目名稱: {item}")
        return dao_info["class"](self.uri) 
    
    @staticmethod
    def _parse_datetime(timestamp: Union[None, str, datetime]) -> Union[None, datetime]:
        """
        將輸入的時間參數轉換為 datetime。

        Args:
            timestamp (Union[None, str, datetime]): 原始時間參數。

        Returns:
            Union[None, datetime]: 轉換後的 datetime 對象。

        Raises:
            ValueError: 如果無法解析輸入的時間參數。
        """
        if timestamp is None:
            return None
        if isinstance(timestamp, datetime):
            return timestamp
        if isinstance(timestamp, str):
            try:
                return str2datetime(timestamp)
            except Exception as e:
                raise ValueError(f"無法解析時間參數: {timestamp}") from e
        raise ValueError(f"無效的時間參數類型: {type(timestamp)}")

    def _get_universe_tickers(
        self, universe_item: str, start_timestamp: datetime = None, end_timestamp: datetime = None, num: int = None
    ) -> List[str]:
        """
        根據指定的 Universe 項目和範圍，獲取 Universe 中的 tickers。

        Args:
            universe_item (str): Universe 項目名稱。
            start_timestamp (datetime, optional): 範圍開始日期。
            end_timestamp (datetime, optional): 範圍結束日期。
            num (int, optional): 對應的資料筆數。

        Returns:
            List[str]: Universe 中的 tickers 列表。
        """
        universe_dao = self._get_dao_instance(universe_item)

        # 如果指定了時間範圍
        if start_timestamp and end_timestamp:
            universe_df = universe_dao.get_item_df_by_datetime(
                start_timestamp=start_timestamp, end_timestamp=end_timestamp
            )
        # 如果指定了筆數
        elif num:
            universe_df = universe_dao.get_item_df_by_num(num=num)
        else:
            raise ValueError("必須指定 start_timestamp 和 end_timestamp 或 num")

        # 提取 tickers
        return universe_df.columns.to_list()
    
    def get_latest_universe_tickers(self, universe_item: str) -> List[str]:
        """
        獲取指定 Universe 的最新 tickers
        """
        return self._get_universe_tickers(universe_item, num=1)
    
    def get_trade_date_list(self, start_timestamp: Union[datetime, str], end_timestamp: Union[datetime, str], format:str="datetime") -> List[Union[datetime, str]]:
        # 處理時間格式（若為字串則轉換為 datetime）
        start_timestamp = self._parse_datetime(start_timestamp)
        end_timestamp = self._parse_datetime(end_timestamp)
        # 取得 DAO 實例，調用底層方法
        market_status_dao = self._get_dao_instance("market_status")
        trade_date_list = market_status_dao.get_trade_date_list(start_timestamp, end_timestamp)
        
        if format == "datetime":
            return trade_date_list
        elif format == "str":
            return [datetime2str(date) for date in trade_date_list]
    
    def is_trade_date(self, timestamp: Union[datetime, str]) -> bool:
        """
        判斷指定日期是否為交易日
        """
        query_result_list = self.get_trade_date_list(timestamp, timestamp)
        if query_result_list:
            return True
        else:
            return False
    
    def get_closest_trade_date(self, timestamp: Union[datetime, str], direction: str = "last", cal_self: bool = True) -> datetime:
        """
        獲取距離指定日期最近的交易日（往前或往後），並可選擇是否納入指定日期本身。
        """
        market_status_dao = self._get_dao_instance("market_status")
        return market_status_dao.get_closest_trade_date(timestamp, direction, cal_self)
    
    def get_latest_data_date(self, item: str) -> datetime:
        """
        獲取指定資料項目的最新日期
        """
        try:
            dao_instance = self._get_dao_instance(item)  # Type hint for clarity
            # Find the latest document based on "data_timestamp"
            latest_doc = dao_instance.find_one({}, sort=[("data_timestamp", -1)], projection={"data_timestamp": 1})
            
            if not latest_doc:
                logging.warning(f"[GET][{item}][last_date] The data does not exist.")
                return None

            return latest_doc.get("data_timestamp")
        except Exception as e:
            logging.error(f"[GET][{item}][last_date] An error occurred: {e}")
            return None
    
    def get_item_df(
        self,
        item: str,
        method: str,
        start_timestamp: Union[None, str, datetime] = None,
        end_timestamp: Union[str, datetime] = "9999-12-31",
        num: int = None,
        query: dict = None,
        projection: dict = None,
        universe_item: str = None,
        tickers: List[str] = None,
    ) -> pd.DataFrame:
        """
        根據指定的 item 和方法，從相應的 DAO 獲取數據，支持範圍過濾。

        Args:
            item (str): 資料項目名稱。
            method (str): 方法名稱，"by_date" 或 "by_num"。
            start_timestamp (Union[None, str, datetime], optional): 開始日期。
            end_timestamp (Union[str, datetime], optional): 結束日期。預設為 "9999-12-31"。
            num (int, optional): 數據筆數，僅在 method="by_num" 時有效。
            query (dict, optional): 額外的查詢條件。
            projection (dict, optional): 投影條件。
            universe_item (str, optional): 範圍所屬的 Universe 名稱。
            tickers (List[str], optional): 自定義的 tickers 列表。

        Returns:
            pd.DataFrame: 返回對應的 DataFrame。
        """
        # 取得 DAO 實例 與 操作功能範圍
        dao_instance = self._get_dao_instance(item)
        dao_capabilities = self.dao_info[item]

        # 處理時間格式
        start_timestamp = self._parse_datetime(start_timestamp)
        end_timestamp = self._parse_datetime(end_timestamp)

        # 處理不支持的範圍參數
        if universe_item and not dao_capabilities["is_universe_specifiable"]:
            universe_item = None  # 重置為 None
        if tickers and not dao_capabilities["is_tickers_specifiable"]:
            tickers = None  # 重置為 None

        # 獲取範圍（spx500 或特定 tickers）
        if universe_item:
            tickers = self._get_universe_tickers(
                universe_item, start_timestamp=start_timestamp, end_timestamp=end_timestamp, num=num
            )
        if tickers:
            projection = {f"values.{ticker}": 1 for ticker in tickers}

        # 驗證方法
        if method == "by_date":
            item_df = dao_instance.get_item_df_by_datetime(
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                query=query or {},
                projection=projection or {},
            )
        elif method == "by_num":
            if num is None:
                raise ValueError("method='by_num' 時必須提供 num 參數")
            
            item_df = dao_instance.get_item_df_by_num(
                end_timestamp=end_timestamp,
                num=num,
                query=query or {},
                projection=projection or {},
            )
        else:
            raise ValueError("method 必須是 'by_date' 或 'by_num'")

        return item_df.sort_index(axis=0).sort_index(axis=1)
    
    def get_gics_code_by_level(self, level: int) -> dict:
        """
        取得指定 GICS Level的code dict(ticker: code)
        """
        dao_instance = self._get_dao_instance("gics_code")
        return dao_instance.fetch_gics_code_by_level(level)
    
    def get_gics_names_by_level(self, level: int) -> List[str]:
        """
        取得指定 GICS Level的Name List
        """
        dao_instance = self._get_dao_instance("gics_code")
        return dao_instance.fetch_gics_names_by_level(level)
    
    def get_gics_info_by_ticker(self, ticker: str) -> dict:
        """
        取得指定 ticker 的 GICS 資訊(4個level的code與name)
        """
        dao_instance = self._get_dao_instance("gics_code")
        return dao_instance.fetch_gics_info_by_ticker(ticker)
    
    def get_tickers_by_gics_name(self, level: int, name: str) -> list:
        """
        取得指定 GICS Level 的 ticker list
        """
        dao_instance = self._get_dao_instance("gics_code")
        return dao_instance.fetch_tickers_by_gics_name(level, name)
    