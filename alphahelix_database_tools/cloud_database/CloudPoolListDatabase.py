from .AbstractCloudDatabase import *
from alphahelix_database_tools.external_tools.google_tools import GoogleDriveTools

class CloudPoolListDatabase(AbstractCloudDatabase):
    def __init__(self, config_folder_path=None):
        super().__init__(config_folder_path=config_folder_path)  # 調用父類 MDB_DATABASE 的__init__方法
        self.MDB_client = MongoClient(self.cluster_uri_dict["articles"], server_api=ServerApi('1'))
    
    def set_google_drive_client(self, GOOGLE_APPLICATION_CREDENTIALS):
        self.google_drive_client = GoogleDriveTools(credential_file_path=GOOGLE_APPLICATION_CREDENTIALS)
    
    # 取得tracking_status（series）
    def get_tracking_status_series(self):
        tracking_status_dict = self.MDB_client["pool_list"]["tracking_status"].find_one(sort=[("updated_timestamp", -1)])["tracking_status"]
        tracking_status_series = pd.Series(tracking_status_dict).rename("tracking_status")
        return tracking_status_series

    # 取得holding_status（series）
    def get_holding_status_series(self):
        holding_status_dict = self.MDB_client["pool_list"]["holding_status"].find_one(sort=[("updated_timestamp", -1)])["holding_status"]
        holding_status_series = pd.Series(holding_status_dict).rename("holding_status")
        return holding_status_series

    # 取得ticker_info(DataFrame)
    def get_ticker_info_df(self):
        ticker_info_meta_list = list(self.MDB_client["pool_list"]["ticker_info"].find({}, {"_id": 0, "ticker": 1, "researcher": 1, "data_manager": 1}))
        ticker_info_df = pd.DataFrame(ticker_info_meta_list).set_index("ticker")
        return ticker_info_df

    # 取得research_status(DataFrame)
    def get_research_status_df(self):
        # 取得research_status
        #research_status_meta_list = list(self.MDB_client["pool_list"]["research_status"].find({"ticker": {"$in": list(tracking_status_dict.keys())}}, {"_id": 0}))
        research_status_meta_list = list(self.MDB_client["pool_list"]["research_status"].find({}, {"_id": 0}))
        research_status_df = pd.DataFrame(research_status_meta_list).set_index("ticker")
        return research_status_df

    def get_pool_list_data_df(self):
        tracking_status_series = self.get_tracking_status_series()
        holding_status_series = self.get_holding_status_series()
        ticker_info_df = self.get_ticker_info_df()
        research_status_df = self.get_research_status_df()
        # 合併為pool list資料表
        pool_list_df = pd.concat([tracking_status_series, holding_status_series, ticker_info_df, research_status_df], axis=1)
        # 使用 loc 和列名布尔索引来去除重复列，保留第一次出现的column
        pool_list_df = pool_list_df.loc[:, ~pool_list_df.columns.duplicated()]
        # 依照tracking_status和holding_status排序
        pool_list_df.sort_values(by="tracking_status", ascending=False, inplace=True)
        pool_list_df.sort_values(by="holding_status", ascending=False, inplace=True)
        # LQD、TLT即使有部位，但tracking_status為空，故不顯示
        pool_list_df.dropna(subset=["tracking_status"], inplace=True)
        return pool_list_df
    
    def get_ticker_research_meta_dict(self, ticker: str):
        research_status_df = self.get_research_status_df()
        if ticker in research_status_df.index:
            return dict(research_status_df.loc[ticker, :])
        else:
            return {}
        
    def get_publications_meta_list(self, ticker: str):
        research_meta_dict = self.get_ticker_research_meta_dict(ticker=ticker)
        publications_meta_list = research_meta_dict.get("publications")
        # 若存在資料，則按照時間排序
        if publications_meta_list:
            publications_meta_list.sort(key=lambda x: x["data_timestamp"], reverse=True)
            return publications_meta_list
        else:
            return []
    
    def get_conclustion_meta_list(self, ticker: str):
        research_meta_dict = self.get_ticker_research_meta_dict(ticker=ticker)
        conclusions_meta_list = research_meta_dict.get("conclusions")
        if conclusions_meta_list:
            conclusions_meta_list.sort(key=lambda x: x["data_timestamp"], reverse=True)

            # 取得conclusion file_id（目前只取最新的一筆）
            conclusion_file_id = conclusions_meta_list[0]["file_id"]
            conclustion_df = self.google_drive_client.get_spreadsheet_data(file_id=conclusion_file_id, sheet_name="10_QA")
            conclusions_meta_list[0]["10_QA"] = conclustion_df.to_dict(orient="records")
            return conclusions_meta_list[0]
        else:
            return {}
        