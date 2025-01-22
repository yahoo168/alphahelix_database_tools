from typing import List, Union
from datetime import datetime, timedelta
import pandas as pd

from alphahelix_database_tools.utils.datetime_utils import str2datetime, datetime2str
from .base_data import BaseDAO

class GicsMappingDAO(BaseDAO):
    """
    DAO class for interacting with GICS code mapping data.
    """
    def __init__(self, uri: str):
        db_name = "Reference"
        collection_name = "gics_code_mapping"
        super().__init__(db_name, collection_name, uri)
    
    def fetch_name_to_code(self, level):
        return self.find_one({"level": level}).get("values", {})
    
    def fetch_code_to_name(self, level: int) -> dict:
        """
        Retrieves the GICS mapping dictionary for a specific level (code to name).
        """
        name_to_code = self.fetch_name_to_code(level)
        # 反轉字典：name -> code 變為 code -> name
        return {code: name for name, code in name_to_code.items()}
        
class GicsCodeDAO(BaseDAO):
    """
    DAO class for interacting with GICS code data.
    """
    def __init__(self, uri: str):
        super().__init__("Reference", "gics_code", uri)
        self.mapping_dao = GicsMappingDAO(uri)
        self.level_to_field_map = {
            1: "gics_sector",
            2: "gics_industry_group",
            3: "gics_industry",
            4: "gics_sub_industry",
        }
    
    def fetch_gics_names_by_level(self, level):
        return sorted(list(self.mapping_dao.fetch_name_to_code(level).keys()))
    
    def fetch_gics_code_by_level(self, level: int) -> dict:
        """
        Fetches a mapping of tickers to GICS codes for a specified level.
        """
        gics_field = self.level_to_field_map.get(level)
        if not gics_field:
            return {}
        
        query_results = self.find(
            query={},
            projection={f"values.{gics_field}": 1, "ticker": 1, "_id": 0}
        )
        
        # 將結果轉換為字典(ticker: gics_code)
        return {
            doc["ticker"]: doc["values"].get(gics_field, None)
            for doc in query_results
        }

    def fetch_gics_info_by_ticker(self, ticker: str) -> dict:
        """
        Retrieves GICS information for a specific ticker, including 4 GICS levels (code and name).
        """
        result = self.find_one({"ticker": ticker})
        if not result or "values" not in result:
            return {}

        # 提取 4 個 GICS code，並逐一添加對應的name
        gics_info = result["values"]
        
        for level, field_name in self.level_to_field_map.items():
            gics_code = gics_info.get(field_name)
            gics_name = None
            if gics_code:
                # 獲取對應的名稱
                mapping_dict = self.mapping_dao.fetch_code_to_name(level)
                gics_name = mapping_dict.get(gics_code)
            
            # 將name添加到字典中
            str_field_name = field_name + "_name"
            gics_info[str_field_name] = gics_name

        return gics_info

    
    def fetch_tickers_by_gics_name(self, level: int, name: str) -> list:
        """
        Fetches a sorted list of tickers associated with a specific GICS component.
        """
        mapping_dict = self.mapping_dao.fetch_name_to_code(level)
        gics_code = mapping_dict.get(name)
        gics_field = self.level_to_field_map.get(level)

        if not gics_code or not gics_field:
            return []

        ticker_list = self.distinct(
            "ticker", {f"values.{gics_field}": gics_code}
        )
        return sorted(ticker_list)
        
class MarketStatusDAO(BaseDAO):
    def __init__(self, uri):
        db_name = "Reference"
        collection_name = "market_status"
        super().__init__(db_name, collection_name, uri)
        
    def get_trade_date_list(self, start_timestamp: datetime, end_timestamp: datetime) -> List[datetime]:
        """
        獲取指定時間範圍內的交易日列表。

        Args:
            start_timestamp (datetime): 開始日期。
            end_timestamp (datetime): 結束日期。

        Returns:
            List[datetime]: 時間範圍內的交易日列表。
        """
        # 驗證輸入參數類型
        if not isinstance(start_timestamp, datetime) or not isinstance(end_timestamp, datetime):
            raise ValueError("start_timestamp 和 end_timestamp 必須是 datetime 類型")

        # 獲取交易狀態數據
        query_result_list = list(self.find(query={"data_timestamp": {"$gte": start_timestamp, "$lte": end_timestamp}, "values": 1},
                                           sort=[("data_timestamp", 1)]))
        
        return [item["data_timestamp"] for item in query_result_list]
    
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
            
class ErrorReportDAO(BaseDAO):
    def __init__(self, uri):
        db_name = "Reference"
        collection_name = "error_report"
        super().__init__(db_name, collection_name, uri)
    
    def get_latest_error_report(self):
        return self.find_one({}, sort=[("created_timestamp", -1)])
    
    # def get_error_reports(self, start_timestamp, end_timestamp):
    #     query_doc_list = list(self.find(query={"created_timestamp": {"$gte": start_timestamp, "$lte": end_timestamp}}))
    #     result_list = []
    #     for doc in query_doc_list:
    #         analysis_df = self.analyze_report(doc["values"])
    #         result_list.append({
    #             "start_timestamp": doc["start_timestamp"],
    #             "end_timestamp": doc["end_timestamp"],
    #             "created_timestamp": doc["created_timestamp"],
    #             "analysis": analysis_df
    #         })
    #     return result_list
    
    # def analyze_report(self, error_detect_result_list):
    #     # 初始化結果存儲清單
    #     detection_results = []

    #     # 處理錯誤檢測結果
    #     for result_dict in error_detect_result_list:
    #         detector_name = result_dict.get("detector_name", "Unknown Detector")
    #         theresold = result_dict["error_threshold"]
    #         detector_description = result_dict.get("detector_description", "Unknown")
    #         univ_name_list = ["all_data", "univ_spx500", "univ_ray3000"]

    #         for univ_name in univ_name_list:
    #             if univ_name in result_dict.get("error_analysis", {}):
    #                 error_data = result_dict["error_analysis"][univ_name]
    #                 error_count = error_data.get("error_count", 0)
    #                 error_rate = error_data.get("error_rate", 0.0)

    #                 # 判斷error_rate是否高於門檻
    #                 is_above_threshold = error_rate > theresold

    #                 # 添加結果到清單
    #                 detection_results.append({
    #                     "detector_name": detector_name,
    #                     "detector_description": detector_description,
    #                     "univ_name": univ_name,
    #                     "error_count": error_count,
    #                     "error_rate": round(error_rate, 4), # 四捨五入到小數點後四位
    #                     "threshold": theresold,
    #                     "is_above_threshold": is_above_threshold
    #                 })

    #     return detection_results
    #     # return pd.DataFrame(detection_results).sort_values("univ_name", ascending=False)