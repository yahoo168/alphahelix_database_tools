import re
import pandas as pd

# 将dict标准化key（避免LLM随即性导致key不一致）
def standardize_key(key):
    # 将 key 转换为小写
    key = key.lower()
    # 去除前后空格
    key = key.strip()
    # 去除标点符号
    key = re.sub(r'[^\w\s]', '', key)
    # 移除多余的空格
    key = re.sub(r'\s+', ' ', key)
    return key

# 创建一个新的字典，用于存储key标准化后的dict
def standardize_dict(data_dict):
    standardized_dict = {}
    reverse_key_mapping = {}
    for key, value in data_dict.items():
        standardized_key = standardize_key(key)
        standardized_dict[standardized_key] = value
        reverse_key_mapping[standardized_key] = key
    return standardized_dict, reverse_key_mapping

# 根据标准化后的字典和映射关系还原原始字典
def reverse_standardized_dict(standardized_dict, reverse_key_mapping):
    original_dict = {}
    for standardized_key, value in standardized_dict.items():
        original_key = reverse_key_mapping.get(standardized_key, standardized_key)
        original_dict[original_key] = value
    return original_dict

def remove_duplicates_by_key(dict_list, key):
    seen_values = set()
    unique_dict_list = []
    
    for d in dict_list:
        value = d.get(key)
        if value not in seen_values:
            unique_dict_list.append(d)
            seen_values.add(value)
    
    return unique_dict_list

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
        aligned_item_df = aligned_item_df.sort_index()
        aligned_item_df_list.append(aligned_item_df)

    return aligned_item_df_list

# 合併兩個key有部分重疊，然而value（也是dict）不重疊的dict，將重疊的key的value合併，並保留不重疊部分的key
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