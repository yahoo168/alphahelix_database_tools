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
    
    def get_active_user_id_list(self):
        user_info_meta_list = list(self.MDB_client["users"]["user_basic_info"].find({"is_active": True}, {"_id": 1}))
        user_id_list = [doc["_id"] for doc in user_info_meta_list]
        return user_id_list
    
    def get_user_viewed_reports(self, user_id):
        result = self.MDB_client["preprocessed_content"]["stock_report"].find(
            { 
                "view_by": {
                    "$elemMatch": { "user_id": ObjectId(user_id) }  # 查找 view_by 陣列中的字典，其 user_id 匹配
                }
            },
            projection={"_id": 1, "title": 1})
        return list(result)

    def get_user_meta_by_roles(self, role_list):
        user_meta_list = list(self.MDB_client["users"]["user_basic_info"].find({"is_active": True,
                                                        "roles": {"$in": role_list}}))
        return user_meta_list

    # 強制研究員須在系統追蹤其負責研究的ticker
    def auto_follow_tickers_for_researchers(self):
        specified_role_list = ["investment_manager", "investment_researcher", "investment_intern"]
        user_meta_list = self.get_user_meta_by_roles(role_list=specified_role_list)
        
        all_ticker_list = list()
        
        for user_meta in user_meta_list:
            user_id = user_meta["_id"]
            responsible_ticker_list = self.get_responsible_ticker_list(user_id=user_id)
            if responsible_ticker_list:
                all_ticker_list.extend(responsible_ticker_list)
                for ticker in responsible_ticker_list:
                    logging.info(f"[SERVER][Auto-Follow-Ticker]{user_meta['username']}->{ticker}")
                    self.MDB_client["research_admin"]["ticker_info"].update_one({"ticker": ticker}, {"$addToSet": {"following_users": user_id}}, upsert=False)
        
        # 待改：Sean追蹤所有有人追蹤的ticker
        investment_manager_id = ObjectId("66836e674c7c79d7d6a7aa0e")
        for ticker in all_ticker_list:
            self.MDB_client["research_admin"]["ticker_info"].update_one({"ticker": ticker}, {"$addToSet": {"following_users": investment_manager_id}}, upsert=False)
            logging.info(f"[SERVER][Auto-Follow-Ticker] Sean ->{ticker}")
            
    # 使得ticker的追蹤者與「投資議題」、「投資假設」的追蹤者保持一致
    def align_ticker_following_users(self):
        # 取得所有 tickers
        ticker_list = [doc["ticker"] for doc in self.MDB_client["research_admin"]["ticker_info"].find({}, {"ticker": 1, "_id": 0})]

        # 需要更新的 collections
        collection_list = ["following_issues", "investment_assumptions"]

        # 遍歷所有 tickers
        for ticker in ticker_list:
            # 取得 ticker 對應的 following_users 列表
            following_user_id_list = self.MDB_client["research_admin"]["ticker_info"].find_one(
                {"ticker": ticker}, {"following_users": 1, "_id": 0}
            )["following_users"]

            # 遍歷 collections 並更新對應的 documents
            for collection in collection_list:
                self.MDB_client["users"][collection].update_many(
                    {"tickers": ticker},
                    {"$addToSet": {"following_users": {"$each": following_user_id_list}}} # 使用 $addToSet 避免重複加入
                )
        logging.info("[SERVER][DATA] All tickers following aligned successfully!")
    
    # 找出特定用戶負責的tickers
    def get_responsible_ticker_list(self, user_id):
        ticker_info_meta_list = self.get_latest_ticker_info_meta_list()
        ticker_info_meta_df = pd.DataFrame(ticker_info_meta_list)
        # 使用 apply 方法來篩選符合的行
        responsible_ticker_series = ticker_info_meta_df[ticker_info_meta_df["researchers"].apply(lambda x: x.get("researcher_id") == user_id)]["ticker"]
        # 顯示符合條件的 ticker 列表
        responsible_ticker_list = sorted(responsible_ticker_series.tolist())
        return responsible_ticker_list

    # 找出特定用戶追蹤的tickers
    def get_following_ticker_list(self, user_id):
        # 若user_id為str，則轉換為ObjectId，若為ObjectId則不變（不會報錯）
        user_meta = self.MDB_client["users"]["user_basic_info"].find_one({"_id": ObjectId(user_id)})
        if not user_meta:
            logging.error(f"[SERVER][ERROR] User not found: {user_id}")
            return []
        
        return sorted(user_meta.get("followed_tickers", []))

    def get_ticker_following_user_list(self, ticker):
        following_users_meta_list = list(self.MDB_client["users"]["user_basic_info"].find({"followed_tickers": ticker}, {"_id": 1}))
        following_users_id_list = [item["_id"] for item in following_users_meta_list]
        return following_users_id_list
    
    def get_ticker_event_meta_list(self, ticker_list=None, start_timestamp=None, end_timestamp=None):
        # 建立查詢條件字典，包含 ticker 條件
        query = {}
        if ticker_list:
            query = {"ticker": {"$in": ticker_list}}
        # 如果有時間範圍，則加入時間條件
        if start_timestamp or end_timestamp:
            timestamp_range_dict = {}
            if start_timestamp:
                timestamp_range_dict["$gte"] = start_timestamp
            if end_timestamp:
                timestamp_range_dict["$lte"] = end_timestamp
            query["event_timestamp"] = timestamp_range_dict
        
        # 待改：目前直接排除earnings_release（因與earnings_call重複）
        query["event_type"] = {"$ne": "earnings_release"}
        query["is_deleted"] = False
        
        event_meta_list = list(self.MDB_client['research_admin']['ticker_event'].find(query, sort=[("event_timestamp", 1)]))
        
        for event_meta in event_meta_list:
            event_meta["event_date_str"] = datetime2str(event_meta["event_timestamp"])
            event_meta["event_time_str"] = event_meta["event_timestamp"].strftime('%Y-%m-%d %H:%M')
        
        return event_meta_list
    
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
    
    def get_internal_stock_report_meta_list(self, ticker=None):
        if ticker:
            query = {"tickers": ticker}
        else:
            query = {}
        
        internal_stock_report_meta_list = list(self.MDB_client["research_admin"]["internal_investment_report"].find(query, sort=[("data_timestamp", -1)]))

        id_to_username_mapping_dict = self.get_id_to_username_mapping_dict()
        for item_meta in internal_stock_report_meta_list:
            item_meta["data_date_str"] = datetime2str(item_meta["data_timestamp"])
            item_meta["author"] = id_to_username_mapping_dict[item_meta["upload_info"]["uploader"]].replace("_", " ").title()
            
        return internal_stock_report_meta_list
    
    def get_market_report_upload_record(self, monitor_period_days=30):
        monitor_period_days = 30
        collection = self.MDB_client["raw_content"]["raw_stock_report_non_auto"]

        # 计算时间戳，获取近 monitor_period_days 天的数据
        start_timestamp = datetime.now(timezone.utc) - timedelta(days=monitor_period_days)

        # 使用 MongoDB 的聚合操作来直接获取每个 (ticker, source) 下的最大 upload_timestamp 和 data_timestamp
        pipeline = [
            {"$match": {"upload_info.upload_timestamp": {"$gte": start_timestamp}}},
            {"$group": {
                "_id": {"ticker": {"$arrayElemAt": ["$tickers", 0]}, "source": "$source"},
                "ticker": {"$first": {"$arrayElemAt": ["$tickers", 0]}},
                "source": {"$first": "$source"},
                # 將 upload_timestamp 和 uploader_id 從 upload_info 中提取並按順序存儲在數組中
                "upload_timestamps": {"$push": "$upload_info.upload_timestamp"},
                "data_timestamps": {"$push": "$data_timestamp"},
                "uploaders": {"$push": "$upload_info.uploader"},
                "upload_count": {"$sum": 1}
            }},
            # 使用 $project 進行數組操作，找到最大 upload_timestamp 對應的 uploader
            {"$project": {
                "ticker": 1,
                "source": 1,
                "upload_timestamp": {"$max": "$upload_timestamps"},
                "data_timestamp": {"$max": "$data_timestamps"},
                # 找到最大 upload_timestamp 對應的索引
                "max_timestamp_index": {"$indexOfArray": ["$upload_timestamps", {"$max": "$upload_timestamps"}]},
                # 獲取對應最大 upload_timestamp 的 uploader
                "uploader": {"$arrayElemAt": ["$uploaders", {"$indexOfArray": ["$upload_timestamps", {"$max": "$upload_timestamps"}]}]},
                "upload_count": 1
            }},
            {"$sort": {"upload_timestamp": -1}}
        ]


        # 执行聚合查询
        record_stats_list = list(collection.aggregate(pipeline))
        
        return record_stats_list

