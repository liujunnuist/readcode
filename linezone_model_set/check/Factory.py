# coding:utf-8

import datetime as dt

import numpy as np
import pandas as pd

from check.validator import DatetimeTypeCheck, DateTypeCheck, DataFrameTypeCheck, DigitalTypeCheck, \
    DictTypeCheck, ListTypeCheck


class ValidatorFactory:
    def __init__(self, data):
        self.data = data

    @property
    def validator(self):
        if isinstance(self.data, pd.DataFrame):
            validator = DataFrameTypeCheck(self.data)
            return validator

        elif isinstance(self.data, dict):
            validator = DictTypeCheck(self.data)
            return validator

        elif isinstance(self.data, dt.date):
            validator = DateTypeCheck(self.data)
            return validator

        elif isinstance(self.data, dt.datetime):
            validator = DatetimeTypeCheck(self.data)
            return validator

        elif isinstance(self.data, (int, float, np.int64)):
            validator = DigitalTypeCheck(self.data)
            return validator
        elif isinstance(self.data, list):
            validator = ListTypeCheck(self.data)
            return validator

        else:
            print(type(self.data))
            print('not implement')


if __name__ == '__main__':
    validator = ValidatorFactory({'A': 1}).validator
    print(validator.processed_data)
