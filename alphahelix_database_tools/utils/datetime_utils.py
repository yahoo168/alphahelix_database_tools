from typing import *
import os
from datetime import datetime, timedelta, timezone
import pandas as pd
import time
from functools import wraps
from bson import ObjectId

# 可轉換為帶時區的UTC格式（Note：帶時區與不帶時區的datetime彼此無法比較大小，故無法混用）
def str2datetime(strdate, _timezone=False):
    datetime_obj = datetime.strptime(strdate, "%Y-%m-%d")
    if _timezone:
        datetime_obj = datetime_obj.replace(tzinfo=timezone.utc)
    return datetime_obj

def datetime2str(date):
    return date.strftime("%Y-%m-%d")

# 將本日的日期以字串形式表達，方便調用
TODAY_DATE_STR = datetime2str(datetime.today())

def str2datetime_list(strdate_list):
    return list(map(lambda x:str2datetime(x), strdate_list))

def datetime2str_list(date_list):
    return list(map(lambda x:datetime2str(x), date_list))

# 將函數的輸入值中的str日期轉換為datetime
def str2datetime_input(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if 'start_date' in kwargs and isinstance(kwargs['start_date'], str):
            kwargs['start_date'] = str2datetime(kwargs['start_date'])
        if 'end_date' in kwargs and isinstance(kwargs['end_date'], str):
            kwargs['end_date'] = str2datetime(kwargs['end_date'])
        return func(*args, **kwargs)
    return wrapper

def shift_days_by_strDate(strdate, days):
    shifted_date = datetime.strptime(strdate, "%Y-%m-%d") + timedelta(days)
    shifted_date = shifted_date.strftime("%Y-%m-%d")
    return shifted_date

#將str日期轉換為unix格式，Ex: 1636693199
def str2unix_timestamp(strDate):
    dt = datetime.strptime(strDate, "%Y-%m-%d")
    dt = str2datetime(strDate)
    # Convert the datetime object to a Unix timestamp
    unix_timestamp = int(time.mktime(dt.timetuple()))
    return unix_timestamp

# 將Unix时间戳转换为datetime
def unix_timestamp2datetime(unix_timestamp):
    if isinstance(unix_timestamp, str):
        unix_timestamp = int(unix_timestamp)
    return datetime.fromtimestamp(unix_timestamp)

# 透過递归函数来遍历容器物件，將其中所有的datetime轉為MDB接受的格式（帶有UTC時區資訊的字串）
def convert_datetimes_to_bson(data):
    if isinstance(data, dict):
        return {key: convert_datetimes_to_bson(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_datetimes_to_bson(element) for element in data]
    elif isinstance(data, datetime):
        #MongoDB 的 REST API 对日期时间字符串的格式有严格的要求，需要符合特定的时区信息格式。
        # $date 是一种特殊的 BSON 扩展，用于表示日期时间对象。使用 MongoDB 的 REST API时，需要将 Python 的 datetime 对象转换成这种格式
        return {"$date": data.replace(tzinfo=timezone.utc).isoformat()}
    else:
        return data

def convert_objectid_to_str(data):
    if isinstance(data, dict):
        return {key: convert_objectid_to_str(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_objectid_to_str(element) for element in data]
    elif isinstance(data, ObjectId):
        return str(data)
    else:
        return data

# 將給定的數個df，對齊coloumn與index，空值補Nan
def get_aligned_df_list(df_list):
    index_list, columns_list = list(), list()
    
    for item_df in df_list:
        index_list.append(set(item_df.index))
        columns_list.append(set(item_df.columns))

    index_series = pd.Series(list(index_list[0].union(*index_list))).dropna()
    columns_series = pd.Series(list(columns_list[0].union(*columns_list))).dropna()
    
    aligned_item_df_list = list()
    for item_df in df_list:
        aligned_item_df = item_df.reindex(index=index_series, columns=columns_series)
        # 因reindex後會出現亂序，須重新排序（row / col皆是）
        aligned_item_df = aligned_item_df.sort_index(axis=0).sort_index(axis=1)
        aligned_item_df_list.append(aligned_item_df)

    return aligned_item_df_list

# 合併兩個key有部分重疊，然而value也是dict（即巢狀dict）的非重疊的dict，將重疊的key的value合併，並保留不重疊部分的key
def combine_dict(dict1, dict2):
    merged_dict = {}

    # 合併第一個字典到結果字典
    for key, value in dict1.items():
        merged_dict[key] = value

    # 合併第二個字典到結果字典
    for key, value in dict2.items():
        if key in merged_dict:
            merged_dict[key].update(value)
        else:
            merged_dict[key] = value
    return merged_dict

def make_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)

def token_trans(name, source, folder_path):
    trans_table_file_path = os.path.join(folder_path, source+".txt")
    
    item_name_list = []
    key_list = []
    with open(trans_table_file_path, 'r') as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip('\n')
            item_name, key = line.split(":")
            item_name_list.append(item_name)
            key_list.append(key)
    
    trans_dict = dict(zip(item_name_list, key_list))
    return trans_dict[name]