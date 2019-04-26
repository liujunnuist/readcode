class DataFrameException(Exception):
    """
    Custom exception types
    """
    def __init__(self, func_name, data, msg='this is a DataFrame error', error_code=1000):
        if func_name:
            self.func_name = func_name
        if data.empty:
            self.data = data
        if msg:
            self.msg = msg

        if error_code:
            self.error_code = error_code

        super(DataFrameException, self).__init__()


class EmptyDataFrameException(DataFrameException):
    """
    if DataFrame is empty,then raise this Exception
    """

    def __init__(self, func_name, data, msg="this DataFrame is empty", error_code=1001):
        if func_name:
            self.func_name = func_name
        if data.empty:
            self.data = data
        if msg:
            self.msg = msg

        if error_code:
            self.error_code = error_code

        super(DataFrameException, self).__init__()


class ExistNullDataFrameException(DataFrameException):
    """
    if null value exists in DataFrame, then raise this Exception
    """

    def __init__(self, func_name, data, msg="this Dataframe exist null value", error_code=1002):
        if func_name:
            self.func_name = func_name
        if data.empty:
            self.data = data
        if msg:
            self.msg = msg

        if error_code:
            self.error_code = error_code
        super(DataFrameException, self).__init__()


class NotImplementException(Exception):
    def __init__(self, msg="This function has not been implemented yet.", error_code=1003):
        if msg:
            self.msg = msg
        if error_code:
            self.error_code = error_code
        super(NotImplementException, self).__init__()
