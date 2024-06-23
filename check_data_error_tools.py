# 若每日變動超過門檻（預設為100%），列為可能出錯清單
def check_potential_error_by_change(price_df, threshold=1):
    change_df = price_df.pct_change()
    max_change = abs(change_df).max()
    huge_change_ticker_index = max_change[max_change > threshold].index
    return huge_change_ticker_index

# 若數據顯示從未進行股票分割，列為可能出錯清單
def check_potential_error_by_split(split_df):
    total_split = split_df.sum()
    never_split_ticker_index = total_split[total_split==0].index
    return never_split_ticker_index

# 比對兩序列的交集/差集，並列出序列1、2獨有的資料
def compare_component(s1, s2):
    common_part_list = sorted(list(set(s1) & set(s2)))
    only_1_part_list = sorted(list(set(s1) - set(s2)))
    only_2_part_list = sorted(list(set(s2) - set(s1)))
    return common_part_list, only_1_part_list, only_2_part_list