import configparser
import numpy as np

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

from alphahelix_database_tools.utils.datetime_utils import *
from alphahelix_database_tools.external_tools.polygon_tools import *

# 日期格式原則：對外函數的input為str(YYYY-MM-DD)，output為datetime，內部函數的input/output皆為datetime

"""
To-do list:
    Infra：
    - get_item_df (by = not date)
    資料對接：
    - company_info / delisted_info
    - industry data (直接在local端複製？)    
    - transform special data for reading
    - shares報錯 (月底更新時)
    - 修改時間（用decorator）
    - 修改顯示訊息（不要在polygon）？
    
    local端：
    - local端函數修改（針對polygon tool改版）
    - local端下載資料
    - local取用函數修改（針對item df調用一致）
    - 補註釋
"""

## 設定logging格式（避免使用print）
logging.basicConfig(level=logging.INFO,
    format = '[%(asctime)s %(levelname)-8s] %(message)s',
    datefmt = '%Y%m%d %H:%M:%S',)

class AbstractCloudDatabase():
    def __init__(self, config_folder_path: str):
        self._parse_config(config_folder_path)
        self.cur_cluster_name = None
        #self._connect_to_cluster(self.cur_cluster_name)
    
    def _parse_config(self, config_folder_path: str):
        data_route_file_path = os.path.join(config_folder_path, "data_route.xlsx")
        ini_file_path = os.path.join(config_folder_path, "access.ini")
        
        self._parse_data_route(data_route_file_path)
        ini_config = configparser.ConfigParser()
        ini_config.read(ini_file_path)
        username, password = ini_config["MDB"]["username"], ini_config["MDB"]["password"]

        self.cluster_uri_dict = {
            # cluster: alphahelixdatabase
            "quant": f"mongodb+srv://{username}:{password}@alphahelixdatabase.nadkzwd.mongodb.net/?retryWrites=true&w=majority&appName=alphahelixDatabase",
            # cluster: articles
            "articles": f"mongodb+srv://{username}:{password}@articles.zlnaiap.mongodb.net/?retryWrites=true&w=majority&appName=articles"
        }

    def _parse_data_route(self, file_path: str) -> None:
        data_route_df = pd.read_excel(file_path, index_col=0)
        data_route_df = data_route_df.loc[:, ["cluster", "database", "collection"]]

        item_path_dict = dict()
        for i in range(len(data_route_df)):
            path_series = data_route_df.iloc[i, :]
            item = path_series.name
            item_path_dict[item] = path_series.to_dict()
            
        self.DATA_PATH_DICT = item_path_dict

    # 調整當前指向的cluster
    def _connect_to_cluster(self, cluster_name:str) -> None:
        uri = self.cluster_uri_dict[cluster_name]
        # 指定 API 版本，确保客户端代码在未来的 MongoDB 服务器版本中保持一致
        self.cur_cluster = MongoClient(uri, server_api=ServerApi('1'))
        try:
            # Send a ping to confirm a successful connection
            self.cur_cluster.admin.command("ping")
            logging.info(f"[MDB][server][check] connected to the '{cluster_name}' cluster")

        except Exception as e:
            print(e)
    
    # 切換當前指向的cluster與collection
    def _locate_collection(self, item:str) -> None:
        data_path_dict = self._get_data_path_dict(item=item)
        cluster_name = data_path_dict["cluster"]
        # 確認當前指向的cluster，若與資料所在的cluster不同，則切換
        if cluster_name != self.cur_cluster_name:
            self.cur_cluster_name = cluster_name
            self._connect_to_cluster(cluster_name=cluster_name)
        
        database_name = data_path_dict["database"]
        collection_name = data_path_dict["collection"]

        # 確認db與collection皆實際存在於該cluster中
        try:
            assert(database_name in self.cur_cluster.list_database_names())
            assert(collection_name in self.cur_cluster[database_name].list_collection_names())  
        
        except Exception as e:
            logging.warning(f"[MDB][server][error] '{database_name}/{collection_name}' collection does not exist")
        
        # 轉換當前指向的collection
        self.cur_col = self.cur_cluster[database_name][collection_name]

    # 返回特定data所屬的cluster, database, collection
    def _get_data_path_dict(self, item: str) -> dict:
        return self.DATA_PATH_DICT[item]
    
    # 儲存資料至MDB，可設定upsert（若存在即update，若不存在即insert）
    def save_data_to_MDB(self, item:str, data_list:List, upsert:bool=False, key:str="date") -> None:
        self._locate_collection(item)
        # 因timeseries collection無法使用pymongo原生的upsert，故若須執行upsert，須先刪除重疊doc後，再作insert
        if upsert == True:
            # 提取data_list中的日期
            key_list = [data[key] for data in data_list]
            # 查找集合中與 date_list 中日期重叠的doc，僅返回_id字段
            overlapped_documents_list = list(self.cur_col.find(filter={key: {"$in": key_list}}, projection={"_id":1}))
            # 如果存在重叠的doc
            if overlapped_documents_list:
                logging.warning(f"[SAVE][{item}]共{len(overlapped_documents_list)}筆document重疊，將刪除舊版")
                # 提取重叠的doc的_id列表
                _id_list =  [doc["_id"] for doc in overlapped_documents_list]
                # 删除所有重叠的doc
                self.cur_col.delete_many({"_id": {"$in": _id_list}})
        
        # 插入新的doc
        self.cur_col.insert_many(data_list)

    # ## decorator：設定star_date與end_date
    # def set_default_dates(start_shift:int=0, end_shift:int=0):
    #     @wraps(func)
    #     def _shift_default_dates(func):
    #         @wraps(func)
    #         def wrapper(self, *args, **kwargs):
    #             print(kwargs)
    #             if ("start_date" not in kwargs) or (kwargs['start_date'] is None):
    #                 kwargs["start_date"] = datetime2str(self.get_latest_data_date(item=kwargs["item"]) 
    #                                                     + timedelta(days=start_shift))
    #             if ("end_date" not in kwargs) or (kwargs['end_date'] is None):
    #                 kwargs['end_date'] = shift_days_by_strDate(TODAY_DATE_STR, end_shift)
                
    #             return func(self, *args, **kwargs)
            
    #         return wrapper
    #     return _shift_default_dates
            
    def get_latest_data_date(self, item:str, date_format:str="datetime") -> Union[datetime, str]: 
        self._locate_collection(item)
        date_list = list(self.cur_col.find({}).sort([("date", -1)]).limit(1))
        if len(date_list) == 0:
            logging.warning("[GET][{item}][last_date] the data does not exist")
            return None
        
        date = date_list[0]["date"]
        if date_format == "str":
            return datetime2str(date)
        
        return date
    
    # 取得交易日日期序列（datetime列表），若不指定區間則預設為全部取出
    # asset_type目前包含：US_Stock, ...
    # 日期格式可為str / datetime
    def get_trade_date_list(self, asset_type:str, start_date:Union[str, datetime]="1900-01-01", 
                            end_date:Union[str, datetime]="9999-12-31") -> List[datetime]:
        if isinstance(start_date, str):
            start_date = str2datetime(start_date)
        if isinstance(end_date, str):
            end_date = str2datetime(end_date)

        item = "market_status_" + asset_type
        market_status_df = self.get_item_df(item=item, method="by_date", start_date=start_date, end_date=end_date)
        if len(market_status_df) > 0:
            # 篩選出交易日，即market_status為True(1)的index
            trade_date_series = market_status_df[market_status_df["market_status"]==1].index
            return list(trade_date_series)
        else:
            logging.warning(f"[WARN][trade_date][{asset_type}][{start_date}~{end_date} doest not exist]")
            return list()
    
    # 取得距離指定日期最近的交易日，計算方式可選往前（last）或往後（next），預設為往前（last）
    # cal_self可選擇給定的日期本身若為交易日是否納入計算，預設為True
    def get_closest_trade_date(self, asset_type:str, date:Union[datetime, str], 
                                    direction:str="last", cal_self:bool=True) -> datetime:
        if type(date) is str:
            date = str2datetime(date)
            
        item = "market_status_" + asset_type
        #取出所有trade_date資料
        market_status_series = self.get_item_df(item=item, method="by_date", start_date="1900-01-01", end_date="2999-12-31").squeeze()
        
        if cal_self == True:
            market_status = market_status_series[date]
        else:
            market_status = 0
        
        if direction == "last":
            step = -1
        elif direction == "next":
            step = 1
        
        while True:
            if market_status != 1:
                date = date + timedelta(days=step)
                market_status = market_status_series[date]
            else:
                return date
    
    # 依照指定的item，取得最近N筆資料的開始日期
    def _get_start_date_by_num(self, item:str, end_date:datetime, num:int) -> datetime:
        assert(num > 0)
        self._locate_collection(item)
        # 針對特定資料項目，僅取出其所有資料索引（date），不取出資料（可節省流量）
        date_list = list(self.cur_col.find({}, {'_id': 0, "date":1}).sort([("date", -1)]))
        # 若僅有一筆資料則直接回傳（squeeze會出錯）
        if len(date_list) == 1:
            return date_list[0]["date"]
        
        # 因df可直接透過dict建立，故先轉為df再透過squeeze轉回series
        date_series = pd.DataFrame(date_list).squeeze()        
        date_series = date_series.sort_values().reset_index(drop=True)
        # 依照結束日與N，向前截取N筆資料，回傳起始日
        date_series = date_series[date_series <= end_date]
        start_date = list(date_series.iloc[-num:,])[0]
        return start_date
    
    def _get_item_data_df_by_num(self, item:str, end_date:datetime, num:int=1, query:dict={}, projection:dict={}) -> pd.DataFrame:
        assert(isinstance(end_date, datetime))
        # 依照指定的item，取得最近N筆資料的開始日期
        start_date = self._get_start_date_by_num(item=item, end_date=end_date, num=num)
        item_df = self._get_item_data_df_by_date(item=item, start_date=start_date, end_date=end_date, query=query, projection=projection)
        return item_df
    
    def _get_item_data_df_by_date(self, item:str, start_date:datetime, end_date:datetime, query:dict={}, projection:dict={}):
        # 確認日期格式為datetime物件
        assert(isinstance(start_date, datetime))
        assert(isinstance(end_date, datetime))
        self._locate_collection(item)
        
        default_query = {"date": {"$gte": start_date, "$lte": end_date}}
        if query:
            default_query.update(query)
            
        # 若不外部指定欄位篩選（projection），則除_id之外，全部取出
        default_projection = {"_id": 0}
        # 若有外部指定欄位篩選，則更新原設置（此時須特別指定取出date）
        if projection:
            default_projection.update({"date": 1})
            default_projection.update(projection)
        
        # batch_size可控制MDB在每次网络请求中向客户端返回的数量，可提升查詢效率
        data_list = list(self.cur_col.find(default_query, default_projection, batch_size=1000))
        if len(data_list) > 0:
            item_df = pd.DataFrame(data_list).set_index("date")
            return item_df
        else:
            logging.info(f"[WARN][{item}][{datetime2str(start_date)}-{datetime2str(end_date)}不存在資料]")
            return pd.DataFrame()

    # 取出item df，index為日期（datetime）
    # 日期格式可為str/datetime，輸入後會自動轉換
    def get_item_df(self, item:str, method:str, start_date:Union[None, str, datetime]=None, 
                    end_date:Union[str, datetime]="9999-12-31", num:int=None, query:dict={}, projection:dict={}):
        
        if isinstance(start_date, str):
            start_date = str2datetime(start_date)
        if isinstance(end_date, str):
            end_date = str2datetime(end_date)
                
        if method == "by_date":
            item_df = self._get_item_data_df_by_date(item=item, start_date=start_date, end_date=end_date, query=query, projection=projection)
                
        elif method == "by_num":
            item_df = self._get_item_data_df_by_num(item=item, end_date=end_date, num=num, query=query, projection=projection)

        # 避免資料可能存在空值（可能源自一次性灌入資料導致）
        if np.nan in item_df.columns:
            item_df = item_df.drop(np.nan, axis=1)

        return item_df.sort_index(axis=0).sort_index(axis=1) 