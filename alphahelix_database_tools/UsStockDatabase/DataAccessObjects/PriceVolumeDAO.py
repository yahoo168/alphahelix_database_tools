from alphahelix_database_tools.UsStockDatabase.DataAccessObjects.BaseDAO import BaseDAO

class PriceVolumeBaseDAO(BaseDAO):
    def __init__(self, collection_name: str, uri: str):
        db_name = "PriceVolume"
        super().__init__(db_name, collection_name, uri)
    
    # 添加專屬 PriceVolume 的共用邏輯

class OpenDAO(PriceVolumeBaseDAO):
    def __init__(self, uri):
        super().__init__("open", uri)
        
class HighDAO(PriceVolumeBaseDAO):
    def __init__(self, uri):
        super().__init__("high", uri)

class LowDAO(PriceVolumeBaseDAO):
    def __init__(self, uri):
        super().__init__("low", uri)

class CloseDAO(PriceVolumeBaseDAO):
    def __init__(self, uri):
        super().__init__("close", uri)

class VolumeDAO(PriceVolumeBaseDAO):
    def __init__(self, uri):
        super().__init__("volume", uri)

class CloseToCloseReturnDAO(PriceVolumeBaseDAO):
    def __init__(self, uri):
        super().__init__("c2c_ret", uri)

class OpenToOpenReturnDAO(PriceVolumeBaseDAO):
    def __init__(self, uri):
        super().__init__("o2o_ret", uri)
        
