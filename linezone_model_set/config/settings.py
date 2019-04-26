# -*- coding: utf-8 -*-

import os


class MyBaseConfig(object):
    def __init__(self):
        self.APP_NAME = "apparel"
        self.RUN_MODE = "local"
        self.DEBUG = True
        self.PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.STDOUT_FILE = os.path.join(self.PROJECT_DIR, 'logs/stdout.log')
        self.STDERR_FILE = os.path.join(self.PROJECT_DIR, 'logs/stderr.log')
        self.DB_HOST = '192.168.200.201'
        self.DB_USER = 'postgres'
        self.DB_PASSWORD = 'lzsj1701'
        self.DB_NAME = 'linezone_retail_wh'
        self.DB_PORT = '5432'


class DevConfig(MyBaseConfig):
    def __init__(self):
        super(DevConfig, self).__init__()


class TestConfig(MyBaseConfig):
    def __init__(self):
        super(TestConfig, self).__init__()


class ProductConfig(MyBaseConfig):
    def __init__(self):
        super(ProductConfig, self).__init__()


config_factory = {
    "development": DevConfig(),
    "test": TestConfig(),
    "product": ProductConfig()

}
config = config_factory['development']


