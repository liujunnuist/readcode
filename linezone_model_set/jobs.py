#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Project : linezone_opff_model_1
# @Time    : 2019/1/21 2:29 PM
# @Author  : redpoppet
# @Site    : localhost
# @File    : jobs.py.py
# @Software: PyCharm


# !/usr/bin/env python
# encoding: utf-8

import argparse
import datetime
import getopt
import sys

from config.settings import config
from model_set.rebalance.settings import model_param
from utils.logger import lz_logger


def usage():
    help_str = """
        1:如果想运行预测模型,请在服务器上运行 python job.py -r p
        2:如果想运行补调模型,请在服务器上运行 python job.py -r rt
        3:如果想查看帮助文档,请输入python job.py -h
    """
    lz_logger.info(help_str)


def execute(run_style=None):
    # TODO 请修改对应的CID
    if run_style == 'p':
        from model_set.predict.pred_model import run
        lz_logger.info('predict_run')
        run()
        lz_logger.info('predict_run over')
    elif run_style == 'rt':
        from model_set.rebalance.main import run
        lz_logger.info('rebalance_run')
        run()
        lz_logger.info('rebalance_run over')
    elif run_style == 'yh':
        from model_set.fresh_product_model.demand_suggest import run
        lz_logger.info('yaohuo_run')
        run(datetime.datetime.now())
        lz_logger.info('yaohuo_run over')
    elif run_style == 'a':
        from model_set.product_analysis_model.similar_store_model import ProAnalysis
        import pandas as pd
        lz_logger.info('product_analysis_run')
        start_date = pd.to_datetime('2019-03-13')
        pro_analysis = ProAnalysis(start_date=start_date)
        pro_analysis.main()
        lz_logger.info('product_analysis_run over')
    else:
        lz_logger.info('no implement')


def get_args():
    parser = argparse.ArgumentParser(description='This is a script to start model tasks')
    parser.add_argument('-r', '--run', help="run model")
    parser.add_argument('-d', '--date', help="execute date")
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    try:

        args = get_args()

        if args.date:
            execute_date = args.date
            config.DECISION_DATE = datetime.datetime.strptime(execute_date, "%Y-%m-%d").date()
            config.DATE_RUN = datetime.datetime.strptime(execute_date, "%Y-%m-%d").date()
            config.DATE_RUN_STR = execute_date
            config.DECISION_DATE = datetime.datetime.strptime(execute_date, "%Y-%m-%d").date()

            model_param.DECISION_DATE = datetime.datetime.strptime(execute_date, "%Y-%m-%d").date() -datetime.timedelta(days=1)

        if args.run:
            run_style = args.run
            execute(run_style)

    except getopt.GetoptError:
        lz_logger.info("输入参数错误")
        usage()
        sys.exit(1)
