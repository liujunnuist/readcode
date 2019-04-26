# -*- coding: utf-8 -*-
# @Time    : 2018/12/27 10:34
# @Author  : Allenware
# @File    : model_config.py
# @Description: model_set running parameters

from config.settings import config

from pyspark import SparkConf
from pyspark.sql import SparkSession

# create spark session instance
spark_conf = SparkConf()
spark_conf.setMaster(config.RUN_MODE)
spark_conf.setAppName(config.APP_NAME)
spark = SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()
