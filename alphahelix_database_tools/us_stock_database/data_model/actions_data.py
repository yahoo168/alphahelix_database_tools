from .base_data import BaseDAO
import pandas as pd

class ActionsBaseDAO(BaseDAO):
    def __init__(self, collection_name: str, uri: str):
        db_name = "Actions"
        super().__init__(db_name, collection_name, uri)

class ExDividendDAO(ActionsBaseDAO):
    def __init__(self, uri):
        super().__init__("ex_dividend", uri)

class PayDividendDAO(ActionsBaseDAO):
    def __init__(self, uri):
        super().__init__("pay_dividend", uri)
    
class StockSplitDAO(ActionsBaseDAO):
    def __init__(self, uri):
        super().__init__("stock_split", uri)