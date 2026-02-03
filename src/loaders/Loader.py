from typing import overload, Union
from pandas import DataFrame

class Loader:

    def __init__(self):
        pass

    @overload
    def load(self, data: list[dict]) -> None: ...
    
    @overload
    def load(self, data: DataFrame) -> None: ...
    
    def load(self, data: Union[list[dict], DataFrame]) -> None:
        pass