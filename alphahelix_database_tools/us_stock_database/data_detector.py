import logging
from typing import List, Dict, Any, Union
import pandas as pd
from datetime import datetime, timezone
from .data_manager import UsStockDataManager

class UsStockDataDetector:
    def __init__(self, name: str, required_item_list: List[str]):
        self.name = name
        self.required_item_list = required_item_list
        self.threshold = 0 # Default threshold for error detection
        self.description = "" # Description of the detector
        self.max_error_count = 1000
        

    def detect(self, data_set: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        raise NotImplementedError("Subclasses must implement the detect method.")

    def run(self, data_set: Dict[str, pd.DataFrame]) -> dict:
        if not self._has_required_columns(data_set):
            raise ValueError(f"Data set is missing required columns for {self.name}.")

        try:
            detect_result_df = self.detect(data_set)
            return self.generate_result(detect_result_df)
        
        except Exception as e:
            logging.error(f"Error in detector '{self.name}': {str(e)}")
            raise
    
    def _has_required_columns(self, data_set: Dict[str, pd.DataFrame]) -> bool:
        missing_columns = [col for col in self.required_item_list if col not in data_set]
        if missing_columns:
            logging.error(f"Missing columns for detector '{self.name}': {missing_columns}")
            return False
        return True

    def generate_result(self, detect_result_df: pd.DataFrame) -> dict:
        result = {
                    "error_records": [], 
                    "error_count": 0.0, 
                    "error_rate": 0.0,
                }
        
        if detect_result_df.any().any():
            result["error_records"] = self._locate_errors(detect_result_df)
            result["error_count"] = len(result["error_records"])
            result["error_rate"] = result["error_count"] / detect_result_df.size
            
            if result["error_rate"] > self.threshold:
                logging.error(f"Error rate exceeds threshold for detector '{self.name}'.")
            
            # Limit the number of error records
            if result["error_count"] > self.max_error_count:
                result["error_records"] = result["error_records"][:self.max_error_count] 
            
        else:
            logging.info(f"Check passed for detector '{self.name}'.")
            
        return result

    def _locate_errors(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        error_locations = df.stack().reset_index()
        error_locations.columns = ['data_timestamp', 'ticker', 'error']
        return error_locations[error_locations['error']][['data_timestamp', 'ticker']].to_dict('records')    


class NegValueDetector(UsStockDataDetector):
    def __init__(self):
        super().__init__("NegValueDetector", ["open", "high", "low", "close", "volume"])
        self.description = """Detect if certian item values are negative"""
        self.threshold = 0.0

    def detect(self, data_set: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        return (data_set["c2c_ret"] < -0.9) | (data_set["o2o_ret"] < -0.9)

class HighLowDetector(UsStockDataDetector):
    def __init__(self):
        super().__init__("HighLowDetector", ["high", "low"])
        self.description = """Detects if the high price is lower than the low price."""
        self.threshold = 0.0
    
    def detect(self, data_set: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        return data_set["high"] < data_set["low"]

class ExtremeHighReturnDetector(UsStockDataDetector):
    def __init__(self):
        super().__init__("ExtremeHighReturnDetector", ["c2c_ret", "o2o_ret"])
        self.description = """Detects if the close-to-close return or open-to-open return is greater than 100%."""
        self.threshold = 0.005 # 0.5%

    def detect(self, data_set: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        return (data_set["c2c_ret"] > 1) | (data_set["o2o_ret"] > 1)

class ExtremeLowReturnDetector(UsStockDataDetector):
    def __init__(self):
        super().__init__("ExtremeLowReturnDetector", ["c2c_ret", "o2o_ret"])
        self.description = """Detects if the close-to-close return or open-to-open return is less than -90%."""
        self.threshold = 0.005 # 0.5%

    def detect(self, data_set: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        return (data_set["c2c_ret"] < -0.9) | (data_set["o2o_ret"] < -0.9)


class UsStockDataDetectorManager(UsStockDataManager):
    """
    Data detector manager for US stock data.
    """
    def __init__(self, username: str, password: str, start_timestamp: datetime, end_timestamp: datetime):
        super().__init__(username, password)
        self.detectors: List[UsStockDataDetector] = []
        self.data_set: Dict[str, pd.DataFrame] = {}
        self.universe_tickers: Dict[str, List[str]] = {}
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp

    def register_detector(self, detector: UsStockDataDetector):
        self.detectors.append(detector)

    def prepare_detector_data(self):
        required_items = {item for detector in self.detectors for item in detector.required_item_list}
        logging.info(f"Preparing data for items: {list(required_items)}")

        self.data_set = self.get_item_df_dict(
            item_list=list(required_items),
            method="by_date",
            start_timestamp = self.start_timestamp,
            end_timestamp = self.end_timestamp,
            if_align=True,
        )

        self.universe_tickers = {
            universe: self.get_universe_tickers(universe, self.start_timestamp, self.end_timestamp)
            for universe in ["univ_spx500", "univ_ray3000"]
        }

    def execute_detectors(self) -> List[dict]:
        def _create_execution_log(detector: UsStockDataDetector, execution_log: dict = None, execution_error: str = None):
            return {
                "detector": detector,
                "execution_log": execution_log,
                "execution_error": execution_error,
            }

        execution_logs = []
        for detector in self.detectors:
            try:
                detect_log = detector.run(self.data_set)
                if detect_log.get("error_count", 0) > 0:
                    logging.error(f"Detector '{detector.name}' found {detect_log['error_count']} errors.")
                    logging.error(detect_log.get("error_records", []))
                execution_logs.append(_create_execution_log(detector, execution_log=detect_log))
            except Exception as e:
                logging.error(f"Error executing detector '{detector.name}': {str(e)}")
                execution_logs.append(_create_execution_log(detector, execution_error=str(e)))
        return execution_logs

    def calculate_error_rate(self, error_tickers: List[str], universe_tickers: List[str]) -> float:
        if not universe_tickers:
            return 0.0
        return len(set(error_tickers) & set(universe_tickers)) / len(universe_tickers)

    def generate_detector_report(self, execution_logs: List[dict]) -> List[Dict[str, Any]]:
        detector_reports = []
        for log in execution_logs:
            detector = log["detector"]
            execution_log = log.get("execution_log", {})
            error_records = execution_log.get("error_records", [])
            error_tickers = sorted(list(set(pd.DataFrame(error_records)["ticker"]))) if error_records else []

            error_analysis = {
                universe: {
                    "error_count": len(set(error_tickers) & set(tickers)),
                    "error_rate": self.calculate_error_rate(error_tickers, tickers),
                }
                for universe, tickers in self.universe_tickers.items()
            }

            report_item = {
                "detector_name": detector.name,
                "error_threshold": detector.threshold,
                "detector_description": detector.description,
                "execution_error": log.get("execution_error"),
                "error_records": error_records,
                "error_analysis": error_analysis,
            }

            detector_reports.append(report_item)
        return detector_reports

    def analyze_detector_report(self, detector_reports: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        detection_results = []
        for report in detector_reports:
            for universe, analysis in report.get("error_analysis", {}).items():
                detection_results.append({
                    "detector_name": report["detector_name"],
                    "detector_description": report["detector_description"],
                    "univ_name": universe,
                    "error_count": analysis["error_count"],
                    "error_rate": round(analysis["error_rate"], 4),
                    "threshold": report["error_threshold"],
                    "is_above_threshold": analysis["error_rate"] > report["error_threshold"],
                })
        return detection_results

    def check_data_error(self, detection_results: List[Dict[str, Any]]) -> bool:
        """
        Check if any universe has an error rate above the threshold.
        
        Args:
            detection_results (List[Dict[str, Any]]): List of detection results containing `univ_name` and `is_above_threshold`.

        Returns:
            bool: True if any universe has an error rate above the threshold, False otherwise.
        """
        df = pd.DataFrame(detection_results)
        # Ensure the return value is a Python bool
        # 確認uni_spx500是否有超過門檻值的錯誤率
        return bool(df[df["univ_name"].isin(["univ_spx500"])]["is_above_threshold"].any())

    def check_data_missing(self, item_list) -> bool:
        """
        Check if there is any missing data for the given item list.
        
        Args:
            item_list (List[str]): List of items to check.

        Returns:
            bool: True if there is missing data, False otherwise.
        """
        def _get_missing_data(item_list: List[str], start_timestamp: datetime, end_timestamp: datetime) -> Dict[str, List[datetime]]:
            trade_dates = self.get_trade_date_list(start_timestamp, end_timestamp)
            return {
                item: list(set(trade_dates) - set(self._get_dao_instance(item).distinct(
                    "data_timestamp", {"data_timestamp": {"$in": trade_dates}}
                )))
                for item in item_list
            }
            
        missing_data = _get_missing_data(item_list, self.start_timestamp, self.end_timestamp)
        # Ensure the return value is a Python bool
        has_missing_data = bool(any(missing_data[item] for item in item_list))
        
        if has_missing_data:
            for item, dates in missing_data.items():
                if dates:
                    logging.info(f"Missing data for {item}: {dates}")
        return has_missing_data

    def run(self) -> Dict[str, Any]:
        self.register_detector(HighLowDetector())
        self.register_detector(NegValueDetector())
        self.register_detector(ExtremeHighReturnDetector())
        self.register_detector(ExtremeLowReturnDetector())
        
        self.prepare_detector_data()
        
        execution_logs = self.execute_detectors()
        detection_result = self.generate_detector_report(execution_logs)
        detection_analysis = self.analyze_detector_report(detection_result)
        
        # 檢測資料缺漏
        item_list = ["open", "high", "low", "close", "volume", "c2c_ret", "o2o_ret"]
        is_data_missing = self.check_data_missing(item_list)
        
        # 檢測資料錯誤
        is_data_error = self.check_data_error(detection_analysis)
        
        data_status = {
            "is_data_missing": is_data_missing, 
            "is_data_error": is_data_error,
            "latest_data_date": self.get_latest_data_date_dict(item_list)
        }

        detect_result = {
            "data_timestamp": self.end_timestamp,
            "created_timestamp": datetime.now(timezone.utc),
            "time_range": {
                "start_timestamp": self.start_timestamp,
                "end_timestamp": self.end_timestamp,
            },
            "data_status": data_status,
            "detection_result": detection_result,
            "detection_analysis": detection_analysis,
        }
        
        self._get_dao_instance("error_report").insert_one(detect_result)
