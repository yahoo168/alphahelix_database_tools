from .AbstractCloudDatabase import *
from alphahelix_database_tools.us_stock_database.data_model.alternative_data import GpuDataModel
from alphahelix_database_tools.data_scrapers.gpu_data_scraper import fetch_gpu_pricing

class CloudAlternativeDatabase(AbstractCloudDatabase):
    def __init__(self, config_folder_path=None):
        super().__init__(config_folder_path=config_folder_path)  # 調用父類 MDB_DATABASE 的__init__方法
        self.MDB_client = MongoClient(self.cluster_uri_dict["articles"], server_api=ServerApi('1'))
    
    def save_cloud_gpu_pricing_data(self):
        # 建立 DAO 實例
        gpu_dao = GpuDataModel()
        source_list = ["coreweave", "cudocompute", "runpod", "datacrunch"]
        
        # 從各個 source 獲取數據並寫入資料庫
        for source in source_list:
            try:
                gpu_data = fetch_gpu_pricing(source)
                if gpu_data:
                    gpu_dao.upsert_gpu_models(source, gpu_data)
            except Exception as error:
                print(f"Error fetching GPU pricing data from {source}: {error}")
                continue