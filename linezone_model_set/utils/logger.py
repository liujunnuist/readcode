# -*- coding: utf-8 -*-
# @Time    : 2018/12/27 16:33
# @Author  : Allenware
# @File    : logger.py
# @Description: initialize log module


import logging.handlers
import datetime
from config.settings import config
import sys

lz_logger = logging.getLogger('root')
lz_logger.setLevel(logging.DEBUG)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(level=logging.DEBUG)
stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

rf_handler = logging.handlers.TimedRotatingFileHandler(config.STDERR_FILE)
rf_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
rf_handler.setLevel(logging.ERROR)

f_handler = logging.FileHandler(config.STDOUT_FILE)
f_handler.setLevel(logging.DEBUG)
f_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s"))

lz_logger.addHandler(stream_handler)
lz_logger.addHandler(rf_handler)
lz_logger.addHandler(f_handler)

