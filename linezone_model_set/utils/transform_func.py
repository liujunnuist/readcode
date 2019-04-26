# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import datetime

class TypeTransform(object):

    @staticmethod
    def to_float(df, feat):
        df[feat] = df[feat].astype(float)
        return df

    @staticmethod
    def to_date(df, feat):
        df[feat] = pd.to_datetime(df[feat])
        return df

    @staticmethod
    def to_int(df, feat):
        df[feat] = df[feat].astype(np.int8)
        return df

    @staticmethod
    def to_string(df, feat):
        df[feat] = df[feat].astype('str')
        return df


def matching_file(df=None, start_date=None):
    """
    matching the turnover of sale_feat
    :param df:
    :param start_date:
    :return:
    """
    start_date = pd.to_datetime(start_date.date())
    date = df["sale_date"].tolist()[0]
    # start_date = datetime.datetime.strptime(str(start_date), "%Y-%m-%d")
    # date = datetime.datetime.strptime(str(date+datetime.timedelta(days=1)), "%Y-%m-%d")
    date = date + datetime.timedelta(days=1)
    day = int((date - start_date).days)
    df["sale_amt"] = df["d{}".format(day)]
    return df[["store_sk", "product_sk", "sale_date", "weekday_class", "temperature", "sale_amt", "recent_3", "recent_7"]]