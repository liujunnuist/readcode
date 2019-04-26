import psycopg2
from config.settings import config
from sqlalchemy import create_engine


class PostgresqlConnection(object):
    def __init__(self, host=None, port=None, username=None, password=None, db_name=None):
        self.host = host if host else config.DB_HOST
        self.port = port if port else config.DB_PORT
        self.username = username if username else config.DB_USER
        self.password = password if password else config.DB_PASSWORD
        self.dbs = db_name if db_name else config.DB_NAME

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
        print(self.dbs)
        return create_engine('postgresql://{username}:{password}@{host}:{port}/{dbs}'.format(
            username=self.username, password=self.password, host=self.host, port=self.port, dbs=self.dbs))