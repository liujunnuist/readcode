#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Project : linezone_opff_model_1
# @Time    : 2019-03-08 11:42
# @Author  : redpoppet
# @Site    : localhost
# @File    : settings.py.py
# @Software: PyCharm
import datetime as dt


class ModelParam(object):
    def __init__(self):
        self.executor = 'scip'
        # Moving-in protection period (days)
        self.prt_per_in = 7
        # Period of sales data (weeks)
        self.per_sales = 4
        # Week sales weights
        self.w = {'w_1': 0.7, 'w_2': 0.3}
        # Basic demand ratio
        self.dr_base = 1.0
        # Basic safety stock
        self.qss_base = 1.0
        # The maximal length of the main size group
        self.len_main_sizes = 4
        # Basic continue-size length in a fullsize group
        self.qsf_base = 3
        # Weight of sales being prior
        self.spr_we = 0.8
        # Mode of calculating target inventory ('continued' or 'renewed')
        self.cal_ti_mode = 'continued'
        # The maximal moving-out package number of each skc/org
        self.qmp_max = 3
        # Basic unit cost of moving packages from warehouse to store
        self.cmp_base_ws = 0.1
        # Basic unit cost of moving packages between stores
        self.cmp_base_ss = 2.0
        # Basic unit cost of moving packages from store to warehouse
        self.cmp_base_sw = 2.0
        # Basic unit cost of moving quantity from warehouse to store
        self.cmq_base_ws = 0.1
        # Basic unit cost of moving quantity between stores
        self.cmq_base_ss = 1.0
        # Basic unit cost of moving quantity from store to warehouse
        self.cmq_base_sw = 6.0
        # Basic unit cost of moving time between stores
        self.cmt_base_ss = 1.0
        # Basic unit cost of inventory difference
        self.cid_base = 2.0
        # Basic unit cost of the maximal inventory difference
        self.cidx_base = 1.0
        # Basic unit cost of demand lossing
        self.cdl_base = 1.5
        # Basic unit cost of shortsize
        self.css_base = [11.5, 11.5]

        # Whether or not executing calculating target inventory
        self.exe_cal_ti = True
        # Whether or not executing replenishment optimization
        self.exe_rep = True
        # Whether or not executing transferring optimization
        self.exe_trans = True
        # Whether or not executing return optimization
        self.exe_ret = False

        self.DEBUG = True
        # Whether or not running program online
        self.ONLINE = True
        # Whether or not loading data from database
        self.LOAD_DATA = True
        # Whether or not running in test mode
        self.TEST = True
        # Whether or not save data to database online
        self.SAVE_DATA = True

        self.DECISION_DATE = dt.date(2019, 3, 12)

        self.SCENE_DICT = {1: "促销活动", 2: "扩展门店", 3: "日常补调", 4: "生命周期下沉", 5: "货品整合"}


model_param = ModelParam()
