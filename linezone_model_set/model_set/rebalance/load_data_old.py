# -*- coding: utf-8 -*-

import pandas as pd
from dateutil.relativedelta import relativedelta

from check.TypeAssert import typeassert
from model_set import spark
from utils.logger import lz_logger
from utils.wraps import singleton
import datetime as dt


@singleton
class DataLayer(object):
    def __init__(self):
        pass

    @typeassert(df=pd.DataFrame, data_type=dict)
    def trans_data(self, df, data_type):
        """
        Transforming data type

        Parameters
        ----------
        df : pd.DataFrame
             Original data

        data_type : dict
                    Data type of selected columns
                    key : dt, int, float, str
                    value : column name

        Returns
        -------
        df_t : pd.DataFrame
               Data after transformed
        """

        df_t = df.copy()

        for col in data_type['dt']:
            if col in df_t.columns:
                df_t[col] = pd.to_datetime(df_t[col])

        for col in data_type['int']:
            if col in df_t.columns:
                df_t[col] = df_t[col].astype('float').astype('int')

        for col in data_type['float']:
            if col in df_t.columns:
                df_t[col] = df_t[col].astype('float')

        for col in data_type['str']:
            if col in df_t.columns:
                df_t[col] = df_t[col].astype('str')

        return df_t

    @typeassert(date_dec=dt.date)
    def load_prod_basic_info(self, date_dec):
        """
        Loading product basic information

        Parameters
        ----------
        date_dec : date
                   Decision making date

        Returns
        -------
        pi : pd.DataFrame
             Product information
             index : prod_id, color_id
             columns : size, size_order, year, season_id, class_0
        """

        sql = '''
                           SELECT t1.product_code AS prod_id,
                                t1.color_code as color_id,
                                t1.size_code AS size,
                                t1.size_order,
                                t1.sku_year AS year,
                                t1.sku_quarter AS season_id,
                                t1.big_class AS class_0
                           FROM abu_opff_edw_ai_dev.dim_sku AS t1
                           JOIN (select product_year, product_quarter 
                                 from abu_opff_edw_ai_dev.config_target_product 
                                 where day_date = '{}') AS t2
                               ON t1.sku_year = t2.product_year and t1.sku_quarter = t2.product_quarter
                         '''.format((date_dec - relativedelta(days=1)).strftime('%Y-%m-%d'))

        data_type = {'dt': [],
                     'int': ['year', 'size_order'],
                     'float': [],
                     'str': ['prod_id', 'color_id', 'size', 'season_id',
                             'class_0']}
        pi = spark.sql(sql).toPandas()
        pi.drop_duplicates(subset=['prod_id', 'color_id', 'size'], inplace=True)
        pi.dropna(inplace=True)
        pi = self.trans_data(pi, data_type)
        pi.sort_values(['prod_id', 'color_id', 'size_order'], inplace=True)
        pi.set_index(['prod_id', 'color_id'], inplace=True)

        return pi

    def load_org_basic_info(self):
        """
        Loading organization basic information

        Parameters
        ----------

        Returns
        -------
        oi : pd.DataFrame
             Organization basic information
             index : org_id
             columns : prov_id, city_id, dist_id, mng_reg_id,
                       is_store(default 1,
                                1 for store,
                                0 for replenishment warehouse,
                                -1 for return warehouse)
        """

        sql = '''
                           SELECT stockorg_code AS org_id, 
                           province AS prov_id, 
                           city AS city_id,
                                  reserved1 as dist_id,
                                  org_code as mng_reg_id,
                                  (case org_flag when 1 then 1 else 0 end) as is_store
                           FROM abu_opff_edw_ai_dev.dim_storeorg
                           WHERE (stockorg_code = 'code0812') OR (status = '正常' AND org_flag = 1)
                           
                         '''

        data_type = {'dt': [],
                     'int': ['is_store'],
                     'float': [],
                     'str': ['org_id', 'prov_id', 'city_id', 'dist_id',
                             'mng_reg_id']}

        oi = spark.sql(sql).toPandas()
        oi.drop_duplicates(subset=['org_id'], inplace=True)
        oi.dropna(subset=['org_id'], inplace=True)
        oi = self.trans_data(oi, data_type)
        oi.set_index('org_id', inplace=True)
        oi.sort_index(inplace=True)

        return oi

    def load_disp_info(self):
        """
        Loading store display information

        Parameters
        ----------

        Returns
        -------
        di : pd.DataFrame
             Display information
             index : org_id, season_id
             columns : qs_min(0), qd_min(0)
        """

        sql = '''
              '''

        data_type = {'dt': [],
                     'int': ['qs_min', 'qd_min'],
                     'float': [],
                     'str': ['org_id', 'season_id']}

        di = spark.sql(sql).toPandas()
        di.drop_duplicates(subset=['org_id', 'season_id'], inplace=True)
        di = self.trans_data(di, data_type)
        di.set_index(['org_id', 'season_id'], inplace=True)
        di.sort_index(inplace=True)

        return di

    @typeassert(date_dec=dt.date)
    def load_init_inv_data(self, date_dec):
        """
        Loading initial inventory data

        Parameters
        ----------
        date_dec : date
                   Decision making date

        Returns
        -------
        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r
        """

        sql = '''
                            with
                                -- 日末库存 
                                init_inv_data AS
                                 (SELECT product_code AS prod_id,
                                         color_code as color_id,
                                         size_code AS size,
                                         org_code as org_id,
                                         stock_qty AS i0
                                  FROM abu_opff_edw_ai_dev.mid_day_end_stock
                                  WHERE stock_date = '{0}'),
    
                                -- 在途库存
                               init_inv_arr_data AS
                                 (SELECT product_code AS prod_id,
                                         color_code as color_id,
                                         size_code AS size,
                                         org_code as org_id,
                                         road_stock_qty AS r
                                  FROM abu_opff_edw_ai_dev.mid_day_end_available_stock
                                  WHERE stock_date = '{0}'
                                  ),
    
                                prod_info_targ AS
                                (SELECT t1.product_code AS prod_id,
                                     t1.color_code as color_id,
                                     t1.size_code AS size
                                 FROM abu_opff_edw_ai_dev.dim_sku AS t1
                                 JOIN (select product_year, product_quarter 
                                       from abu_opff_edw_ai_dev.config_target_product 
                                       where day_date = '{0}') AS t2
                                       ON t1.sku_year = cast(t2.product_year as int) and t1.sku_quarter = t2.product_quarter
                                ),
    
                                 org_info_targ AS
                                 (SELECT distinct stockorg_code AS org_id
                                  FROM abu_opff_edw_ai_dev.dim_storeorg
                                  WHERE (stockorg_code = 'code0812') OR (status = '正常' AND org_flag = '1'))
    
                            SELECT t3.prod_id, t3.color_id, t3.size, t3.org_id, t3.i0, t3.r
                            FROM (SELECT COALESCE(t1.prod_id, t2.prod_id) AS prod_id,
                                       COALESCE(t1.color_id, t2.color_id) AS color_id,
                                       COALESCE(t1.size, t2.size) AS size,
                                       COALESCE(t1.org_id, t2.org_id) AS org_id,
                                       COALESCE(t1.i0, 0) AS i0,
                                       COALESCE(t2.r, 0) AS r
                                FROM init_inv_data AS t1
                                LEFT JOIN init_inv_arr_data AS t2
                                ON t1.prod_id = t2.prod_id AND
                                   t1.color_id = t2.color_id AND
                                   t1.size = t2.size AND
                                   t1.org_id = t2.org_id) AS t3
                            JOIN prod_info_targ AS t4
                            ON t3.prod_id = t4.prod_id AND t3.color_id = t4.color_id AND t3.size = t4.size
                            JOIN org_info_targ AS t5
                            ON t3.org_id = t5.org_id
                          '''.format((date_dec - relativedelta(days=1)).strftime('%Y-%m-%d'))

        data_type = {'dt': [],
                     'int': ['i0', 'r'],
                     'float': [],
                     'str': ['prod_id', 'color_id', 'size', 'org_id']}

        i0 = spark.sql(sql).toPandas()

        i0.drop_duplicates(subset=['prod_id', 'color_id', 'size', 'org_id'],
                           inplace=True)
        i0.dropna(subset=['prod_id', 'color_id', 'size', 'org_id'], inplace=True)
        i0 = self.trans_data(i0, data_type)
        i0.set_index(['prod_id', 'color_id', 'size', 'org_id'], inplace=True)
        i0.sort_index(inplace=True)

        return i0

    @typeassert(date_dec=dt.date)
    def load_sales_data(self, date_dec):
        """
        Loading sales data

        Parameters
        ----------
        date_dec : date
                   Decision making date

        Returns
        -------
        s : pd.DataFrame
            Sales data
            index : prod_id, color_id, size, org_id
            columns : date_sell, s
        """

        sql = '''
                            WITH 
                                prod_info_targ AS
                                (SELECT t1.product_code AS prod_id,
                                     t1.color_code as color_id,
                                     t1.size_code AS size
                                 FROM abu_opff_edw_ai_dev.dim_sku AS t1
                                 JOIN (select product_year, product_quarter 
                                       from abu_opff_edw_ai_dev.config_target_product 
                                       where day_date = '{0}') AS t2
                                       ON t1.sku_year = cast(t2.product_year as int) and t1.sku_quarter = t2.product_quarter
                                ),
    
                               org_info_targ AS
                                 (SELECT distinct stockorg_code AS org_id
                                  FROM abu_opff_edw_ai_dev.dim_storeorg
                                  WHERE status = '正常' AND org_flag = '1')
    
                              SELECT t1.product_code AS prod_id,
                                     t1.color_code AS color_id,
                                     t1.size_code AS size,
                                     t1.org_code AS org_id,
                                     t1.sale_date AS date_sell,
                                     SUM(COALESCE(t1.qty, 0)) AS s
                              FROM abu_opff_edw_ai_dev.fct_sales AS t1
                              JOIN prod_info_targ AS t2
                              ON t1.product_code = t2.prod_id AND
                                 t1.color_code = t2.color_id AND
                                 t1.size_code = t2.size
                              JOIN org_info_targ AS t3
                              ON t1.org_code = t3.org_id
                              WHERE t1.sale_date BETWEEN '{0}' AND '{1}'
                              GROUP BY t1.product_code, t1.color_code, t1.size_code, t1.org_code, t1.sale_date
                          '''.format((date_dec - relativedelta(weeks=4)).strftime('%Y-%m-%d'),
                                     (date_dec - relativedelta(days=1)).strftime('%Y-%m-%d'))

        data_type = {'dt': ['date_sell'],
                     'int': ['s'],
                     'float': [],
                     'str': ['prod_id', 'color_id', 'size', 'org_id']}

        s = spark.sql(sql).toPandas()
        s.dropna(subset=['prod_id', 'color_id', 'size', 'org_id'], inplace=True)
        s = self.trans_data(s, data_type)
        s.set_index(['prod_id', 'color_id', 'size', 'org_id'], inplace=True)
        s.sort_index(inplace=True)

        return s

    @typeassert(date_dec=dt.date)
    def load_mov_data(self, date_dec):
        """
        Loading moving data

        Parameters
        ----------
        date_dec : date
                   Decision making date


        Returns
        -------
        mv : pd.DataFrame
             Moving data
             index : prod_id, color_id, size
             columns : org_send_id, org_rec_id, date_send, date_rec, qty_send,
                       qty_rec
        """

        sql = '''
                          SELECT product_code AS prod_id,
                                 color_code as color_id,
                                 size_code AS size,
                                 send_org_code AS org_send_id,
                                 receive_org_code AS org_rec_id,
                                 send_date AS date_send,
                                 receive_date AS date_rec,
                                 COALESCE(send_qty, 0) AS qty_send,
                                 COALESCE(receive_qty, 0) AS qty_rec
                          FROM abu_opff_edw_ai_dev.fct_stock_move
                          WHERE send_date BETWEEN '{0}' and '{1}'
                          '''.format((date_dec - relativedelta(months=3)).strftime('%Y-%m-%d'),
                                     (date_dec - relativedelta(days=1)).strftime('%Y-%m-%d'))

        data_type = {'dt': ['date_send', 'date_rec'],
                     'int': ['qty_send', 'qty_rec'],
                     'float': [],
                     'str': ['prod_id', 'color_id', 'size',
                             'org_send_id', 'org_rec_id']}

        mv = spark.sql(sql).toPandas()
        mv.drop(mv[mv['qty_send'] < 0].index, inplace=True)
        mv.dropna(
            subset=['prod_id', 'color_id', 'size', 'org_send_id', 'org_rec_id'],
            inplace=True)
        mv = self.trans_data(mv, data_type)
        mv.sort_values(
            ['prod_id', 'color_id', 'size', 'org_send_id', 'org_rec_id'],
            inplace=True)
        mv.set_index(['prod_id', 'color_id', 'size'], inplace=True)

        return mv



    def load_it_pre(self, date_dec):
        """
        Loading target inventory of the previous day

        Parameters
        ----------
        date_dec : date
                   Decision making date


        Returns
        -------
        itp : pd.DataFrame
              Target inventory of the previous day
              index : prod_id, color_id, size, org_id
              columns : itp
        """

        sql = '''
                          WITH 
                                prod_info_targ AS
                                 (SELECT t1.product_id AS prod_id,
                                         t2.color_id,
                                         t2.size_id AS size
                                  FROM (select product_id from edw.dim_target_product where day_date = '{0}' group by product_id) AS t1
                                  JOIN edw.dim_product_sku AS t2
                                      ON t1.product_id = t2.product_id
                                  WHERE t2.status = 'Y'),
    
                               org_info_targ AS
                                 (SELECT store_id AS org_id
                                  FROM edw.dim_store
                                  WHERE is_store = 'Y' AND
                                        is_toc = 'Y')
    
                          SELECT t1.product_id AS prod_id,
                                 t1.color_id,
                                 t1.size_id AS size,
                                 t1.org_id,
                                 t1.stock_qty AS itp
                          FROM edw.mid_day_target_stock_peacebird AS t1
                          JOIN prod_info_targ AS t2
                          ON t1.product_id = t2.prod_id AND
                             t1.color_id = t2.color_id AND
                             t1.size_id = t2.size
                          JOIN org_info_targ AS t3
                          ON t1.org_id = t3.org_id
                          WHERE -- t1.active = 'Y' AND
                                t1.stock_date = '{0}'
                          '''.format((date_dec - relativedelta(days=1)).strftime('%Y-%m-%d'))

        data_type = {'dt': [],
                     'int': ['itp'],
                     'float': [],
                     'str': ['prod_id', 'color_id', 'size', 'org_id']}

        itp = spark.sql(sql).toPandas()
        itp.dropna(inplace=True)
        itp.drop_duplicates(
            subset=['prod_id', 'color_id', 'size', 'org_id'], inplace=True)
        itp = self.trans_data(itp, data_type)
        itp.set_index(['prod_id', 'color_id', 'size', 'org_id'], inplace=True)
        itp.sort_index(inplace=True)

        return itp

    @typeassert(date_dec=dt.date)
    def load_perm_mov_act(self, date_dec):
        """
        Loading permitted moving actions of stores

        Parameters
        ----------
        date_dec : date
                   Decision making date

        is_online : bool
                    Whether or not running program online

        Returns
        -------
        mpa : pd.DataFrame
              Permitted store moving actions
              index : org_id
              columns : is_rep(0), is_trans(0), is_ret(0)
        """

        sql = '''
                         SELECT DISTINCT t1.store_id AS org_id,
                                         (CASE is_rep WHEN 'Y' THEN 1 ELSE 0 END) AS is_rep,
                                         (CASE is_trans WHEN 'Y' THEN 1 ELSE 0 END) AS is_trans,
                                         0 as is_ret
                         FROM edw.mid_store_move_period AS t1
                         JOIN edw.dim_store AS t2
                         ON t1.store_id = t2.store_id
                         WHERE t1.day_date = '{0}' AND
                               t2.is_store = 'Y' AND
                               t2.is_toc = 'Y'
                         '''.format(date_dec.strftime('%Y-%m-%d'))

        data_type = {'dt': [],
                     'int': ['is_rep', 'is_trans', 'is_ret'],
                     'float': [],
                     'str': ['org_id']}

        mpa = spark.sql(sql).toPandas()
        mpa.dropna(inplace=True)
        mpa.drop_duplicates(inplace=True)
        mpa = self.trans_data(mpa, data_type)
        mpa.set_index('org_id', inplace=True)
        mpa.sort_index(inplace=True)

        return mpa

    @typeassert(date_dec=dt.date)
    def load_spc_mov_state(self, date_dec):
        """
        Loading special moving states of product-organization couples

        Parameters
        ----------
        date_dec : date
                   Decision making date
        Returns
        -------
        mss : pd.DataFrame
              Special moving states
              index : prod_id, color_id, org_id
              columns : not_in(0), not_out(0), to_emp(0)
        """

        sql = '''
                          SELECT DISTINCT t1.product_id as prod_id,
                                          t1.color_id,
                                          t1.store_id AS org_id,
                                          t1.not_in AS not_in,
                                          t1.not_out AS not_out,
                                          0 as to_emp
                          FROM edw.dim_store_skc_trans_status AS t1
                          JOIN edw.dim_store AS t2
                          ON t1.store_id = t2.store_id
                          WHERE t1.date_dec = '2019-01-10' AND
                                t2.is_store = 'Y' AND 
                                t2.is_toc = 'Y'
                          '''.format(date_dec.strftime('%Y-%m-%d'))

        data_type = {'dt': [],
                     'int': ['not_in', 'not_out', 'to_emp'],
                     'float': [],
                     'str': ['prod_id', 'color_id', 'org_id']}

        mss = spark.sql(sql).toPandas()
        mss.dropna(inplace=True)
        mss.drop_duplicates(subset=['prod_id', 'color_id', 'org_id'], inplace=True)
        mss = self.trans_data(mss, data_type)
        mss.set_index(['prod_id', 'color_id', 'org_id'], inplace=True)
        mss.sort_index(inplace=True)

        return mss

    @typeassert(date_dec=dt.date)
    def load_data_from_db(self, date_dec):
        """
        Loading data from database

        Parameters
        ----------
        date_dec : date
                   Decision making date

        Returns
        -------
        pi : pd.DataFrame
             Product information
             index : prod_id, color_id
             columns : size, size_order, year, season_id, class_0

        oi : pd.DataFrame
             Organization basic information
             index : org_id
             columns : prov_id, city_id, dist_id, mng_reg_id, is_store(1)

        di : pd.DataFrame
             Display information
             index : org_id, season_id
             columns : qs_min(0), qd_min(0)

        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r

        s : pd.DataFrame
            Sales data
            index : prod_id, color_id, size, org_id
            columns : date_sell, s

        mv : pd.DataFrame
             Moving data
             index : prod_id, color_id, size
             columns : org_send_id, org_rec_id, date_send, date_rec, qty_send,
                       qty_rec

        itp : pd.DataFrame
              Target inventory of the previous day
              index : prod_id, color_id, size, org_id
              columns : itp

        mpa : pd.DataFrame
              Store permitted moving actions
              index : org_id
              columns : is_rep(0), is_trans(0), is_ret(0)

        mss : pd.DataFrame
              Special moving states
              index : prod_id, color_id, org_id
              columns : not_in(0), not_out(0), to_emp(0)
        """

        # Loading product information
        pi = self.load_prod_basic_info(date_dec)

        # Loading organization information
        oi = self.load_org_basic_info()

        # Loading initial inventory data
        i0 = self.load_init_inv_data(date_dec)

        # Loading sales data
        s = self.load_sales_data(date_dec)

        # Loading moving data
        mv = self.load_mov_data(date_dec)

        # Loading target inventory of the previous day
        # itp = load_it_pre(date_dec, is_online)
        itp = pd.DataFrame()
        # Loading permitted moving actions of stores
        # mpa = load_perm_mov_act(date_dec, is_online)
        mpa = pd.DataFrame()

        # Loading special moving states of product-organization couples
        # mss = load_spc_mov_state(date_dec, is_online)

        mss = pd.DataFrame()
        # Loading store display information
        # di = load_disp_info(date_dec, is_online)
        di = pd.DataFrame()
        return pi, oi, di, i0, s, mv, itp, mpa, mss

    @typeassert(date_dec=dt.date)
    def load_data(self, date_dec):
        """
        Loading data

        Parameters
        ----------

        date_dec : date
                   Decision making date

        Returns
        -------
        pi : pd.DataFrame
             Product information
             index : prod_id, color_id
             columns : size, size_order, year, season_id, class_0

        oi : pd.DataFrame
             Organization basic information
             index : org_id
             columns : prov_id, city_id, dist_id, mng_reg_id, is_store(1)

        di : pd.DataFrame
             Display information
             index : org_id, season_id
             columns : qs_min(0), qd_min(0)

        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r

        s : pd.DataFrame
            Sales data
            index : prod_id, color_id, size, org_id
            columns : date_sell, s

        mv : pd.DataFrame
             Moving data
             index : prod_id, color_id, size
             columns : org_send_id, org_rec_id, date_send, date_rec, qty_send,
                       qty_rec

        itp : pd.DataFrame
              Target inventory of the previous day
              index : prod_id, color_id, size, org_id
              columns : itp

        mpa : pd.DataFrame
              Store permitted moving actions
              index : org_id
              columns : is_rep(0), is_trans(0), is_ret(0)

        mss : pd.DataFrame
              Special moving states
              index : prod_id, color_id, org_id
              columns : not_in(0), not_out(0), to_emp(0)
        """

        lz_logger.info('Reading data from database.')
        pi, oi, di, i0, s, mv, itp, mpa, mss = \
            self.load_data_from_db(date_dec)
        return pi, oi, di, i0, s, mv, itp, mpa, mss
