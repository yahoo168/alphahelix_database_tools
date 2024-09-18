from .AbstractCloudDatabase import *
from alphahelix_database_tools.external_tools.google_tools import GoogleDriveTools
from pymongo import UpdateOne
from datetime import datetime, timedelta, timezone
from collections import defaultdict

class CloudPoolListDatabase(AbstractCloudDatabase):
    def __init__(self, config_folder_path=None):
        super().__init__(config_folder_path=config_folder_path)  # 調用父類 MDB_DATABASE 的__init__方法
        self.MDB_client = MongoClient(self.cluster_uri_dict["articles"], server_api=ServerApi('1'))
    
    def set_google_drive_client(self, GOOGLE_APPLICATION_CREDENTIALS):
        self.google_drive_client = GoogleDriveTools(credential_file_path=GOOGLE_APPLICATION_CREDENTIALS)
            
    def get_active_latest_meta(self, meta_list):
        active_meta_list = [meta for meta in meta_list if meta.get("is_acticve")]
        if not active_meta_list:
            return {}
        else:
            latest_active_meta = max(active_meta_list, key=lambda x: x["updated_timestamp"])
            return latest_active_meta
    
    def check_ticker_info_exist(self, ticker):
        ticker_info_meta = self.MDB_client["research_management"]["ticker_info"].find_one({"ticker": ticker})
        if ticker_info_meta:
            return True
        else:
            return False
        
    def get_id_to_username_mapping_dict(self):
        user_info_meta_list = list(self.MDB_client["users"]["user_basic_info"].find())
        mapping_df = pd.DataFrame(user_info_meta_list).loc[:, ["username", "_id"]].set_index("_id")
        id_to_username_mapping_dict = mapping_df["username"].to_dict()  # 只提取 "username" 欄位作為值
        return id_to_username_mapping_dict

    def get_username_to_id_mapping_dict(self):
        user_info_meta_list = list(self.MDB_client["users"]["user_basic_info"].find())
        mapping_df = pd.DataFrame(user_info_meta_list).loc[:, ["username", "_id"]].set_index("username")
        username_to_id_mapping_dict = mapping_df.to_dict()
        return username_to_id_mapping_dict
    
    def update_ticker_info(self, editor_id, ticker, update_data_dict):
        current_timestamp = datetime.now()
        update_operation_list = []
        
        if "investment_ratings" in update_data_dict:
            assert("investment_thesis" in update_data_dict["investment_ratings"])
            assert("profit_rating" in update_data_dict["investment_ratings"])
            assert("risk_rating" in update_data_dict["investment_ratings"])
            update_data_dict["investment_ratings"].update({"updated_timestamp": current_timestamp, "editor_id": ObjectId(editor_id)})
            
            update_operation_list.append(UpdateOne(
                {"ticker": ticker},
                {"$push": {"investment_ratings": update_data_dict["investment_ratings"]}}
            ))
        
        if "researchers" in update_data_dict:
            assert("researcher_id" in update_data_dict["researchers"])
            assert("is_active" in update_data_dict["researchers"])
            update_data_dict["researchers"].update({"updated_timestamp": current_timestamp, "editor_id": ObjectId(editor_id)})
            update_operation_list.append(UpdateOne(
                {"ticker": ticker},
                {"$push": {"researchers": update_data_dict["researchers"]}}
            ))
    
        if "data_managers" in update_data_dict:
            assert("data_manager_id" in update_data_dict["data_managers"])
            assert("is_active" in update_data_dict["data_managers"])
            update_data_dict["data_managers"].update({"updated_timestamp": current_timestamp, "editor_id": ObjectId(editor_id)})
            update_operation_list.append(UpdateOne(
                {"ticker": ticker},
                {"$push": {"data_managers": update_data_dict["data_managers"]}}
            ))
        
        if "holding_status" in update_data_dict:
            assert("securities" in update_data_dict["holding_status"])
            update_data_dict["holding_status"].update({"updated_timestamp": current_timestamp, "editor_id": ObjectId(editor_id)})
            update_operation_list.append(UpdateOne(
                {"ticker": ticker},
                {"$push": {"holding_status": update_data_dict["holding_status"]}}
            ))
        
        if "poolList_status" in update_data_dict:
            assert("in_poolList" in update_data_dict["poolList_status"])
            update_data_dict["poolList_status"].update({"updated_timestamp": current_timestamp, "editor_id": ObjectId(editor_id)})
            update_operation_list.append(UpdateOne(
                {"ticker": ticker},
                {"$push": {"poolList_status": update_data_dict["poolList_status"]}}
            ))
        
        if "tracking_status" in update_data_dict:
            assert("tracking_level" in update_data_dict["tracking_status"])
            update_data_dict["tracking_status"].update({"updated_timestamp": current_timestamp, "editor_id": ObjectId(editor_id)})
            update_operation_list.append(UpdateOne(
                {"ticker": ticker},
                {"$push": {"tracking_status": update_data_dict["tracking_status"]}}
            ))
        
        if update_operation_list:
            self.MDB_client["research_admin"]["ticker_info"].bulk_write(update_operation_list)
            return True
        return False
    
    # 挑出tracking_level >= 2的ticker，用於搜尋新聞
    # 待改：資料庫結構可能調整拆解
    def get_tracking_ticker_list(self, min_tracking_level=2):
        ticker_meta_list = list(self.MDB_client["research_admin"]["ticker_info"].find())
        
        ticker_list = list()
        for ticker_meta in ticker_meta_list:
            tracking_status_meta_list = ticker_meta["tracking_status"]
            if tracking_status_meta_list:
                tracking_status = max(tracking_status_meta_list, key=lambda x: x["updated_timestamp"])
            else:
                tracking_status = {}
            
            tracking_level = tracking_status.get("tracking_level", 0)
            # 若tracking_level高於指定level，則將ticker加入list
            if tracking_level >= min_tracking_level:
                ticker_list.append(ticker_meta["ticker"])
        
        ticker_list.sort()
        return ticker_list
        
    def create_ticker_info(self, creator_id, ticker, investment_thesis, profit_rating, risk_rating, researcher_id):
        current_timestamp = datetime.now()
        
        # 待改：初次導入可能存在「investment_thesis」為None的情況，之後皆需要有投資論點
        if isinstance(investment_thesis, str):
            investment_rating_meta = {"updated_timestamp": current_timestamp, 
                                        "investment_thesis": investment_thesis,
                                        "profit_rating": profit_rating, 
                                        "risk_rating": risk_rating,
                                        "editor_id": ObjectId(creator_id)}
        else:
            investment_rating_meta = {}
        
        ticker_info_meta = {
            "ticker": ticker,
            "creator_id": ObjectId(creator_id),
            "create_timestamp": current_timestamp,
            
            "investment_ratings": [investment_rating_meta] if investment_rating_meta else [],
            
            "researchers": [{
                    "updated_timestamp": current_timestamp,
                    "researcher_id": ObjectId(researcher_id),
                    "editor_id": ObjectId(creator_id),
                    "is_active": True,
                    }],
            
            # 預設為False，須經審核後才能加入pool list
            "poolList_status": [{"in_poolList": False, 
                                 "updated_timestamp": current_timestamp,
                                 "editor_id": ObjectId(creator_id)}],
            
            # 建立時預設為False，僅預留儲存空間，後續編輯個股資料時再填入
            "data_managers": [],
            "holding_status": [],
            "tracking_status": [],
            "following_users": [],
        }

        result = self.MDB_client["research_admin"]["ticker_info"].insert_one(ticker_info_meta)
        # 通过检查 inserted_id 属性来确定操作是否成功
        if result.inserted_id:
            logging.info(f"Document {ticker} inserted successfully")
            return True
        else:
            logging.info("Document insertion failed")
            return False
    
    def get_latest_ticker_info_meta_list(self, ticker_list=None):
            # 若有指定ticker_list，則只取得指定ticker的資料, 否則取得所有ticker的資料
            if ticker_list:
                ticker_info_meta_list = list(self.MDB_client["research_admin"]["ticker_info"].find({"ticker": {"$in": ticker_list}}))
            else:
                ticker_info_meta_list = list(self.MDB_client["research_admin"]["ticker_info"].find())
            
            for ticker_info_meta in ticker_info_meta_list:
                # default=None，避免"holding_status"、tracking_status不存在時報錯
                ticker_info_meta["researchers"] = max(ticker_info_meta.get("researchers", []), key=lambda x: x.get("updated_timestamp", None), default={})
                ticker_info_meta["data_managers"] = max(ticker_info_meta.get("data_managers", []), key=lambda x: x.get("updated_timestamp", None), default={})
                ticker_info_meta["holding_status"] = max(ticker_info_meta.get("holding_status", []), key=lambda x: x.get("updated_timestamp", None), default={})
                ticker_info_meta["tracking_status"] = max(ticker_info_meta.get("tracking_status", []), key=lambda x: x.get("updated_timestamp", None), default={})
                ticker_info_meta["investment_ratings"] = max(ticker_info_meta.get("investment_ratings", []), key=lambda x: x.get("updated_timestamp", 0), default={})
                ticker_info_meta["poolList_status"] = max(ticker_info_meta.get("poolList_status", []), key=lambda x: x.get("updated_timestamp", 0), default={})
            
            return ticker_info_meta_list
    
    def get_internal_stock_report(self, ticker=None):
        if ticker:
            query = {"ticker": ticker}
        else:
            query = {}
            
        return list(self.MDB_client["research_admin"]["internal_investment_report"].find(query))
    
    def get_market_report_upload_record(self, monitor_period_days=30):
        collection = self.MDB_client["raw_content"]["raw_stock_report_non_auto"]
        # 计算时间戳，获取近30天的数据
        start_timestamp = datetime.now(timezone.utc) - timedelta(days=monitor_period_days)
        # 查找近30天上传的报告
        doc_meta_list = list(collection.find({"upload_timestamp": {"$gte": start_timestamp}}))
        # 初始化一个字典，用于存储统计结果
        record_stats_dict = defaultdict(lambda: {'upload_count': 0, 'latest_upload_timestamp': None, 'uploaders': set()})

        # 遍历每个文档
        for doc_meta in doc_meta_list:
            ticker = doc_meta.get('ticker')
            upload_timestamp = doc_meta.get('upload_timestamp')
            uploader_id = doc_meta.get('uploader_id')

            if ticker:
                # 更新上传次数
                record_stats_dict[ticker]['upload_count'] += 1
                # 更新最新上传日期
                if not record_stats_dict[ticker]['latest_upload_timestamp'] or upload_timestamp > record_stats_dict[ticker]['latest_upload_timestamp']:
                    record_stats_dict[ticker]['latest_upload_timestamp'] = upload_timestamp

                # 更新上传者列表
                record_stats_dict[ticker]['uploaders'].add(str(uploader_id))  # 将ObjectId转成字符串保存

        return record_stats_dict