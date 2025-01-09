from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, BulkWriteError

from datetime import datetime
import logging
import pandas as pd
import numpy as np
from typing import List, Tuple, Union

class BaseDAO:
    def __init__(self, db_name, collection_name, uri):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]

    def insert_one(self, document, unique_key=None):
        """
        插入單筆資料，並在用戶指定 unique_key 時自動建立索引。
        - document: 待插入的文檔
        - unique_key: 指定檢查唯一性的 key（可選）
        """
        if unique_key:
            # 為 unique_key 創建降序索引，若已存在則跳過
            self.ensure_index([(unique_key, -1)], unique=True)

        try:
            # 插入文檔
            return self.collection.insert_one(document).inserted_id
        except DuplicateKeyError:
            logging.warning(f"Duplicate value for key '{unique_key}' with value '{document.get(unique_key)}'. Skipping insert.")
            return None

    def ensure_index(self, keys, unique=True):
        """
        確保索引存在，根據名稱檢查是否需要創建索引。
        - keys: 索引鍵值對列表，例如 [("field_name", 1)]
        - unique: 是否唯一索引（默認為 False）
        """
        index_name = "_".join([f"{k}_{v}" for k, v in keys])  # 基於字段生成索引名稱
        index_info = self.collection.index_information()

        if index_name in index_info:
            logging.info(f"Index {index_name} already exists. Skipping creation.")
            return

        # 創建索引
        self.collection.create_index(keys, unique=unique, name=index_name)
        logging.info(f"Index {index_name} created successfully.")

    
    def insert_many(self, documents, unique_key=None):
        """
        插入多筆資料，並在用戶指定 unique_key 時自動建立索引。
        - documents: 待插入的文檔列表
        - unique_key: 指定檢查唯一性的 key（可選）
        """
        if unique_key:
            # 為 unique_key 創建降序索引，若已存在則跳過
            self.ensure_index([(unique_key, -1)], unique=True)

        to_insert = []
        for document in documents:
            if unique_key and unique_key in document:
                to_insert.append(document)

        try:
            # 插入文檔
            if to_insert:
                return self.collection.insert_many(to_insert, ordered=False).inserted_ids
            else:
                logging.warning("No documents were inserted due to duplicate keys.")
                return []
        
        except BulkWriteError as e:
            logging.warning(f"Bulk write error: {e.details}. Skipping duplicates.")
            return []

    def find(self, query, projection=None, sort=None, limit=None):
        """
        查詢資料。
        Args:
            query (dict): 查詢條件。
            projection (dict, optional): 返回字段投影。
            sort (list, optional): 排序條件。
            limit (int, optional): 最大返回數量。

        Returns:
            list: 查詢結果。
        """
        # 構建查詢參數
        query_params = {
            "filter": query,
            "projection": projection,
        }
        if sort:
            query_params["sort"] = sort
        if limit is not None:  # 僅在 limit 不為 None 時傳遞，否則會報錯
            query_params["limit"] = limit

        return list(self.collection.find(**query_params))

    
    def find_one(self, query, projection=None, sort=None):
        """查詢資料"""
        return self.collection.find_one(query, projection, sort=sort)

    def distinct(self, field, query):
        """查詢唯一值"""
        return self.collection.distinct(field, query)
    
    def update_one(self, query, update, upsert=False):
        """更新單筆資料"""
        return self.collection.update_one(query, {"$set": update}, upsert=upsert)

    def delete_one(self, query):
        """刪除單筆資料"""
        return self.collection.delete_one(query)
    
    # 供子類別覆寫，將原始資料轉換為指定格式
    def _transform_data_df(self, df):
        return df
    
    def get_item_df_by_datetime(self, start_timestamp:datetime, end_timestamp:datetime, query:dict={}, projection:dict={}) -> pd.DataFrame:
        """
        Retrieves a DataFrame of items from the database based on the specified datetime range.
        Args:
            start_timestamp (datetime): The start timestamp of the range.
            end_timestamp (datetime): The end timestamp of the range.
            query (dict, optional): Additional query parameters to filter the items. Defaults to {}.
            projection (dict, optional): Additional projection parameters to specify the fields to include in the result. Defaults to {}.
        Returns:
            pd.DataFrame: A DataFrame containing the retrieved items.
        """
        query = query or {}
        projection = projection or {}
        
        # 驗證輸入參數類型
        if not isinstance(start_timestamp, datetime) or not isinstance(end_timestamp, datetime):
            raise ValueError("start_timestamp 和 end_timestamp 必須是 datetime 類型")
        
        default_query = {"data_timestamp": {"$gte": start_timestamp, "$lte": end_timestamp}}
        if query:
            default_query.update(query)
        
        # 若不外部指定欄位篩選（projection），則除_id之外，全部取出
        default_projection = {"_id": 0}
        # 若有外部指定欄位篩選，則更新原設置（此時須特別指定取出date）
        if projection:
            default_projection.update({"data_timestamp": 1})
            default_projection.update(projection)
        
        # 執行查詢並返回結果
        return self._execute_query(default_query, default_projection, sort={"data_timestamp": -1}, limit=None)        
    
    def get_item_df_by_num(self, num:int=1, end_timestamp=None, query:dict=None, projection:dict=None) -> pd.DataFrame:
        query = query or {}
        projection = projection or {}
        
        # 依照指定的item，取得最近N筆資料的開始日期
        if end_timestamp is not None:
            assert(isinstance(end_timestamp, datetime))
            query = {"data_timestamp": {"$lte": end_timestamp}, **query}
        
        # 若不外部指定欄位篩選（projection），則除_id之外，全部取出
        default_projection = {"_id": 0}
        # 若有外部指定欄位篩選，則更新原設置（此時須特別指定取出date）
        if projection:
            default_projection.update({"data_timestamp": 1})
            default_projection.update(projection)
        
        # 執行查詢並返回結果
        return self._execute_query(query, default_projection, sort={"data_timestamp": -1}, limit=num)
    
    def _execute_query(self, query: dict, projection: dict, sort: List[Tuple[str, int]] = None, limit: int = None) -> pd.DataFrame:
        """
        Executes a MongoDB query and transforms the results into a DataFrame.

        Args:
            query (dict): MongoDB query filter.
            projection (dict): MongoDB projection fields.
            sort (list, optional): Sorting order for the query. Defaults to None.
            limit (int, optional): Maximum number of documents to retrieve. Defaults to None.

        Returns:
            pd.DataFrame: Transformed DataFrame.
        """
        # 構建查詢參數，僅在 limit 有效時傳遞
        find_params = {
            "filter": query,
            "projection": projection,
            "batch_size": 1000
        }
        
        if sort:
            find_params["sort"] = sort
        if limit is not None:
            find_params["limit"] = limit

        # 執行查詢
        query_result = self.collection.find(**find_params)
        query_result_list = list(query_result)

        if query_result_list:
            # 將查詢結果轉換為 DataFrame
            raw_df = pd.DataFrame(query_result_list).set_index("data_timestamp")
            item_df = pd.DataFrame(raw_df["values"].tolist(), index=raw_df.index)

            # 轉換原始資料格式（若子類別有覆寫）
            item_df = self._transform_data_df(item_df)

            # 避免資料可能存在空值（可能原始資料存在錯誤）
            if np.nan in item_df.columns:
                item_df = item_df.drop(np.nan, axis=1)

            # 排序並返回
            return item_df.sort_index(axis=0).sort_index(axis=1)

        # 若無資料，返回空 DataFrame
        logging.info(f"[INFO] 無數據匹配查詢條件: {query}")
        return pd.DataFrame()
