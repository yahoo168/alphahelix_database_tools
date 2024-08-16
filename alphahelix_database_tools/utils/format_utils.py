import re

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