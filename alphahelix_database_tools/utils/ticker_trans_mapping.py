def trans_BBG_event_type(event_type):
    trans_dict = {
        "EC": "earnings_call",
        "ER": "earnings_release",
        "CP": "conference_call",
        "ID": "investor_day",
    }
    return trans_dict.get(event_type, "other")

# BBG大部分資料頁面採用公司初次上市代號，因此需要轉換成美股代號（如TSM的BBG代號為2330 TT），或者GOOG/GOOGL
def trans_BBG_main_ticker(BBG_ticker):
    ticker_dict = {
        "2330 TT": "TSM",
        "IFX GR": "IFNNY",
        "GOOGL US": "GOOG",
    }
    
    # Return the mapped ticker or use the first part of the BBG ticker
    return ticker_dict.get(BBG_ticker, BBG_ticker.split(' ')[0])