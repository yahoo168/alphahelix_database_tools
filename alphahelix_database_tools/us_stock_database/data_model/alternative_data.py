from pymongo import MongoClient, UpdateOne
from datetime import datetime, timezone

class GpuDataModel:
    def __init__(self):
        username = "yahoo168"
        password = "yahoo210"
        db_uri = f"mongodb+srv://{username}:{password}@alphahelixdatabase.nadkzwd.mongodb.net/?retryWrites=true&w=majority&appName=alphahelixDatabase"
        self.client = MongoClient(db_uri)
        self.collection = self.client["alternative"]["cloud_gpu_price"]

    def upsert_gpu_models(self, source, gpu_data):
        """
        插入或更新 GPU 型號的價格數據，基於型號和來源進行 upsert。
        若同一型號在同一 data_timestamp 已存在數據，則不進行更新。

        :param source: 資料來源 (e.g., cudocompute, runpod, datacrunch)
        :param gpu_data: GPU 資料列表
        """
        operations = []
        current_timestamp = datetime.now(timezone.utc)
        data_timestamp = current_timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        skipped_models = []  # 儲存跳過的型號

        for record in gpu_data:
            model = record.get("model")
            cost = record.get("cost")
            unit = record.get("unit", "hr")

            # 僅在首次插入時設置的字段
            set_on_insert = {
                "model": model,
                "source": source,
                "created_timestamp": current_timestamp,
            }

            # 檢查是否已存在相同 data_timestamp 的資料
            existing_entry = self.collection.find_one(
                {"model": model, "pricing_data.data_timestamp": data_timestamp}
            )
            if existing_entry:
                skipped_models.append(model)  # 收集跳過的型號
                continue

            # 更新文檔或插入新文檔
            update_doc = {
                "$setOnInsert": set_on_insert,
                "$push": {
                    "pricing_data": {
                        "data_timestamp": data_timestamp,
                        "upload_timestamp": current_timestamp,
                        "cost": cost,
                        "unit": unit
                    }
                }
            }
            operations.append(UpdateOne({"model": model, "source": source}, update_doc, upsert=True))

        # 批量執行 upsert
        if operations:
            result = self.collection.bulk_write(operations)
            print(f"Matched: {result.matched_count}, Inserted: {result.upserted_count}, Modified: {result.modified_count}")
        else:
            print("No new data to update.")

        # 顯示跳過的型號
        if skipped_models:
            print("Warning: The following models already have data for the same data_timestamp and were skipped:")
            for model in skipped_models:
                print(f"- {model}")

    def find_gpu_by_model(self, model):
        """
        根據型號查詢資料。

        :param model: GPU 型號
        :return: 查詢結果
        """
        return self.collection.find_one({"model": model})

    def find_all_gpus(self):
        """
        查詢所有 GPU 型號資料。

        :return: 所有 GPU 資料
        """
        return list(self.collection.find())

    def find_all_gpu_model_names(self):
        """
        查詢所有 GPU 型號名稱。

        :return: 所有 GPU 型號名稱
        """
        return list(self.collection.distinct("model"))
    
    def delete_gpu_by_model(self, model):
        """
        刪除特定型號的 GPU 資料。

        :param model: GPU 型號
        """
        result = self.collection.delete_one({"model": model})
        return result.deleted_count
