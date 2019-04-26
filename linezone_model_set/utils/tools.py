# -*- coding: utf-8 -*-
# @Time         : 2018/12/28
# @Author       : Allenware
# @File         : tools.py
# @Description: : shared function tools

import pandas as pd
from config.settings import config
import os


class TaskExecutor(object):
    """
        use concurrent.futures to execute task in parallel
    """

    def __init__(self, task_list, handler, parallel=None):
        from concurrent import futures
        from multiprocessing import cpu_count

        self._task_list = task_list
        self._handler = handler

        if len(task_list) > cpu_count():
            task_num = cpu_count()
        else:
            task_num = len(task_list)

        if parallel:
            task_num = parallel

        self._executor = futures.ProcessPoolExecutor(task_num)

        self._process()

    def _process(self):

        self._result = self._executor.map(self._handler, self._task_list)

    @property
    def result(self):
        self._executor.shutdown()
        return [elem for elem in self._result]


def persist_table(table_name, dataset, date_string, date):
    """
        persist table to hive

        if exists: insert overwrite
        else: insert append
    :param table_name:
    :param dataset:
    :param date_string:
    :param date:
    :return:
    """

    from model_set import spark

    if isinstance(dataset, pd.DataFrame):
        dataset = spark.createDataFrame(dataset)
    if not isinstance(date, str):
        date = date.strftime("%Y-%m-%d")

    # judge the way of write table
    exist = False
    try:
        read_sql = " select * from {} limit 1 ".format(table_name)
        spark.sql(read_sql)
    except Exception:
        exist = False

    if exist:

        exist_df = spark.sql("select * from {table} where {date_string} <> '{date}'".format(table=table_name, date_string=date_string, date=date))
        new_df = exist_df.unionAll(dataset)

        new_df = spark.createDataFrame(new_df.toPandas())

        new_df.write.saveAsTable(table_name, mode="overwrite")

    else:
        dataset.write.saveAsTable(table_name, mode="append")

    return True


def save_data_to_pickle(data, pickle_name):
    if isinstance(data, pd.DataFrame):
        if not data.empty:
            data.to_pickle(os.path.join(config.system.PROJECT_DIR, 'data', pickle_name + '.pickle'))
            return True
        else:

            return False
    else:

        return False


def read_data_from_pickle(pickle_name):
    return pd.read_pickle(os.path.join(config.system.PROJECT_DIR, 'data', pickle_name + '.pickle'))
