#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Project : DH
# @Time    : 2017/12/19 上午11:22
# @Author  : redpoppet
# @Site    : localhost
# @File    : postgresql_connection.py
# @Software: PyCharm

import psycopg2
from config.settings import config
from sqlalchemy import create_engine


class PostgresqlConnection(object):
    def __init__(self, host=None, port=None, username=None, password=None, db_name=None):
        self.host = host if host else config.system.DB_HOST
        self.port = port if port else config.system.DB_PORT
        self.username = username if username else config.system.DB_USER
        self.password = password if password else config.system.DB_PASSWORD
        self.dbs = 'lz' + db_name if db_name else config.system.DB_NAME

        self.conn = self.connect_2_db()
        self.cursor = self.conn.cursor()

    def connect_2_db(self):
        import os
        os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
        conn = psycopg2.connect(database=self.dbs, user=self.username, password=self.password, host=self.host, port=self.port)
        return conn

    def insert_sql(self, sql=None):
        if not sql:
            return None
        try:
            self.cursor.execute(sql)
            self.commit()
            self.disconnect_2_db()
            return True
        except Exception as e:
            print(e.message)
            return False

    def update_sql(self, sql=None, params=None):
        if not sql:
            return None
        try:
            self.cursor.execute(sql, params)
            self.commit()
            self.disconnect_2_db()
            return True
        except Exception as e:
            print(e.message)
            return False

    def disconnect_2_db(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def get_connection(self):
        return self.conn

    def get_cursor(self):
        return self.cursor

    def commit(self):
        return self.conn.commit()

    def get_engine(self):
        return create_engine('postgresql://{username}:{password}@{host}:{port}/{dbs}'.format(
            username=self.username, password=self.password, host=self.host, port=self.port, dbs=self.dbs))


if __name__ == "__main__":
    # cursor = OracleConnection().cursor
    # cursor.execute('select MAX("时间") from "1_期末库存表"')
    # result = cursor.fetchall()
    # for item in result:
    #     print(item)

    # cursor = PostgresqlConnection().get_cursor()
    # s = cursor.execute('select * from rst.rst_daily_inventory limit 2 ')
    # result = cursor.fetchall()
    # for item in result:
    #     print(item)

    import pandas as pd

    # conn = PostgresqlConnection(db_name='Dst').get_connection()
    # data = pd.read_sql(sql="select * from base.base_company", con=conn)
    conn = PostgresqlConnection(db_name='common').get_engine()
    data = pd.read_sql(sql="select * from base.base_company", con=conn)
    # data.to_sql(name='data1', schema='rs', con=conn, if_exists='append', index=False)
    print(data)
