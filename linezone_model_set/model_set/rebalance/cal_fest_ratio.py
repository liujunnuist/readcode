# -*- coding: utf-8 -*-

import datetime as dt

import numpy as np
import pandas as pd

from model_set import spark


fest_lst = ('元旦', '春节', '清明节', '劳动节', '端午节', '中秋节', '国庆节',
            '双十一', '双十二', '圣诞节')


def trans_data(df, data_type):
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


def load_fut_fest_info(fest_lst, date_dec):
    """
    Loading future festival information

    Parameters
    ----------
    fest_lst : tuple
               List of festival names to be considered

    date_dec : date
               Decision making date

    Returns
    -------
    fi : pd.DataFrame
         Festival information
         index : fest_name
         columns : date_fest_start, date_fest_end
    """

    sql = """
          SELECT fest_name,
                 date_fest_start,
                 date_fest_end
          FROM erke_shidian_edw_ai_dev.dim_festival_info
          WHERE fest_name in {0} AND
                ((date_fest_start BETWEEN '{1}' AND '{2}') OR
                 (date_fest_end BETWEEN '{1}' AND '{2}'))
          """.format(fest_lst,
                     date_dec.strftime('%Y-%m-%d'),
                     (date_dec + dt.timedelta(days=7)).strftime('%Y-%m-%d'))

    data_type = {'dt': ['date_fest_start', 'date_fest_end'],
                 'int': [],
                 'float': [],
                 'str': ['fest_name']}

    fi = spark.sql(sql).toPandas()

    fi.dropna(inplace=True)
    fi = trans_data(fi, data_type)
    fi.set_index('fest_name', inplace=True)
    fi.sort_index(inplace=True)

    return fi


def load_past_fest_info(fest_lst, date_dec):
    """
    Loading the past festival information in the previous 2 weeks

    Parameters
    ----------
    fest_lst : tuple
               List of festival names to be considered

    date_dec : date
               Decision making date

    Returns
    -------
    rfi : pd.DataFrame
          Recent festival information
          index : fest_name
          columns : date_fest_start, date_fest_end
    """

    sql = """
          SELECT fest_name,
                 date_fest_start,
                 date_fest_end
          FROM erke_shidian_edw_ai_dev.dim_festival_info
          WHERE fest_name in {0} AND
                (date_fest_end BETWEEN '{1}' AND '{2}')
          """.format(fest_lst,
                     (date_dec - dt.timedelta(days=14)).strftime('%Y-%m-%d'),
                     (date_dec - dt.timedelta(days=1)).strftime('%Y-%m-%d'))

    data_type = {'dt': ['date_fest_start', 'date_fest_end'],
                 'int': [],
                 'float': [],
                 'str': ['fest_name']}

    fi = spark.sql(sql).toPandas()

    fi.dropna(inplace=True)
    fi = trans_data(fi, data_type)
    fi.set_index('fest_name', inplace=True)
    fi.sort_index(inplace=True)

    return fi


def load_pre_fest_info(fest_name, date_dec):
    """
    Loading the previous selected festival information

    Parameters
    ----------
    fest_name : tuple
                Selected festival name

    date_dec : date
               Decision making date

    Returns
    -------
    pfi : pd.DataFrame
          Previous festival information
          index : fest_name
          columns : date_fest_start, date_fest_end
    """

    sql = """
          WITH sel_fest_info AS
               (SELECT MAX(date_fest_start) AS date_past_fest_start
                FROM erke_shidian_edw_ai_dev.dim_festival_info
                WHERE fest_name = '{0}' AND
                      date_fest_end < '{1}')
          SELECT t1.fest_name,
                 t1.date_fest_start,
                 t1.date_fest_end
          FROM erke_shidian_edw_ai_dev.dim_festival_info AS t1
          JOIN sel_fest_info AS t2
          ON t1.date_fest_start = t2.date_past_fest_start
          WHERE fest_name = '{0}'
          """.format(fest_name,
                     date_dec.strftime('%Y-%m-%d'))

    data_type = {'dt': ['date_fest_start', 'date_fest_end'],
                 'int': [],
                 'float': [],
                 'str': ['fest_name']}

    pfi = spark.sql(sql).toPandas()

    pfi.dropna(inplace=True)
    pfi = trans_data(pfi, data_type)
    pfi.set_index('fest_name', inplace=True)
    pfi.sort_index(inplace=True)

    return pfi


def load_sales_data(date_start, date_end):
    """
    Loading sales data

    Parameters
    ----------
    date_start : date
                 Festival starting date

    date_end : date
               Festival ending date

    Returns
    -------
    s : pd.DataFrame
        Sales data
        index : class_2, mng_reg_id
        columns : date_sell, s
    """

    # sql = """
    #       WITH class_info AS
    #            (SELECT product_id AS prod_id,
    #                    tiny_class AS class_2
    #             FROM erke_shidian_edw_ai_dev.dim_sku),
    #            reg_info AS
    #            (SELECT store_id AS org_id,
    #                    CONCAT(COALESCE(dq_id, ' '), '_',
    #                                    COALESCE(zone_id,
    #                                             COALESCE(province, ' ')))
    #                    AS mng_reg_id
    #             FROM erke_shidian_edw_ai_dev.dim_stockorg
    #             WHERE is_store = 'Y')
    #       SELECT t2.class_2,
    #              t3.mng_reg_id,
    #              t1.sale_date AS date_sell,
    #              SUM(t1.qty) AS s
    #       FROM erke_shidian_edw_ai_dev.fct_sales AS t1
    #       JOIN class_info AS t2
    #       ON t1.product_id = t2.prod_id
    #       JOIN reg_info AS t3
    #       ON t1.store_id = t3.org_id
    #       WHERE t1.sale_date BETWEEN '{0}' AND '{1}'
    #       GROUP BY t2.class_2, t3.mng_reg_id, t1.sale_date
    #       """.format((date_start - dt.timedelta(days=7)).strftime('%Y-%m-%d'),
    #                  date_end.strftime('%Y-%m-%d'))

    sql = """
              WITH class_info AS
               (SELECT product_code AS prod_id,
                       tiny_class AS class_2
                FROM erke_shidian_edw_ai_dev.dim_sku),
               reg_info AS
               (SELECT stockorg_code AS org_id,
                       org_code AS mng_reg_id
                FROM erke_shidian_edw_ai_dev.dim_stockorg
                WHERE org_flag = 1)
          SELECT t2.class_2,
                 t3.mng_reg_id,
                 t1.sale_date AS date_sell,
                 SUM(t1.qty) AS s
          FROM erke_shidian_edw_ai_dev.fct_sales AS t1
          JOIN class_info AS t2
          ON t1.product_code = t2.prod_id
             JOIN reg_info AS t3
              ON t1.org_code = t3.org_id
              WHERE t1.sale_date BETWEEN '{0}' AND '{1}'
              GROUP BY t2.class_2, t3.mng_reg_id, t1.sale_date
              """.format((date_start - dt.timedelta(days=7)).strftime('%Y-%m-%d'),
                         date_end.strftime('%Y-%m-%d'))

    data_type = {'dt': ['date_sell'],
                 'int': ['s'],
                 'float': [],
                 'str': ['class_2', 'mng_reg_id']}

    s = spark.sql(sql).toPandas()

    s.dropna(subset=['class_2', 'mng_reg_id', 'date_sell'], inplace=True)
    s = trans_data(s, data_type)
    s.set_index(['class_2', 'mng_reg_id'], inplace=True)
    s.sort_index(inplace=True)

    return s


def cal_mean_sales(s, date_start, date_end):
    """
    Calculating the mean daily sales

    Parameters
    ----------
    s : pd.DataFrame
        Sales data
        index : class_2, mng_reg_id
        columns : date_sell, s

    date_start : date
                 Festival starting date

    date_end : date
               Festival ending date

    Returns
    -------
    s_mean_in_fest : pd.DataFrame
                     Mean daily sales during festivals
                     index : class_2, mng_reg_id
                     columns : s_in_fest

    s_mean_pre_fest : pd.DataFrame
                      Mean daily sales previous to festivals
                      index : class_2, mng_reg_id
                      columns : s_pre_fest
    """

    cols_grp = ['class_2', 'mng_reg_id']

    # Calculating the mean daily sales during the festival
    s_in_fest = s.loc[s['date_sell'] >= date_start, 's'].copy()
    s_in_fest = s_in_fest.reset_index(cols_grp).groupby(cols_grp).sum()
    s_mean_in_fest = s_in_fest / ((date_end - date_start).days + 1)
    s_mean_in_fest.rename(columns={'s': 's_in_fest'}, inplace=True)

    # Calculating the mean daily sales previous to the festival
    s_pre_fest = s.loc[s['date_sell'] < date_start, 's'].copy()
    s_pre_fest = s_pre_fest.reset_index(cols_grp).groupby(cols_grp).sum()
    s_mean_pre_fest = s_pre_fest / 7
    s_mean_pre_fest.rename(columns={'s': 's_pre_fest'}, inplace=True)

    return s_mean_in_fest, s_mean_pre_fest


def cal_fest_sr(s_in_fest, s_pre_fest):
    """
    Calculating ratio of sales during the festival to that previous to the
    festival

    Parameters
    ----------
    s_in_fest : pd.DataFrame
                Mean daily sales during festivals
                index : class_2, mng_reg_id
                columns : s_in_fest

    s_pre_fest : pd.DataFrame
                 Mean daily sales previous to festivals
                 index : class_2, mng_reg_id
                 columns : s_pre_fest

    Returns
    -------
    sr_fest : pd.DataFrame
              Sales ratio in the festival duration
              index : class_2, mng_reg_id
              columns : sr_fest
    """

    cols_grp = ['class_2', 'mng_reg_id']

    sr_fest = s_in_fest.join(s_pre_fest, how='outer').reset_index()
    sr_fest.fillna({'s_in_fest': 0, 's_pre_fest': 0}, inplace=True)
    sr_fest.loc[sr_fest['s_pre_fest'] < 0, 's_pre_fest'] = 0
    sr_fest['s_pre_fest'] += 1
    sr_fest['s_in_fest'] = sr_fest[['s_in_fest', 's_pre_fest']].max(axis=1)
    sr_fest['sr_fest'] = sr_fest['s_in_fest'] / sr_fest['s_pre_fest']

    sr_fest = sr_fest[cols_grp + ['sr_fest']].set_index(cols_grp)

    return sr_fest


def proc_outliers(sr_fest):
    """
    Processing outliers

    Parameters
    ----------
    sr_fest : pd.DataFrame
              Sales ratio in the festival duration
              index : class_2, mng_reg_id
              columns : sr_fest

    Returns
    -------
    sr_fest_p : pd.DataFrame
                Sales ratio in the festival duration after processed
                index : class_2, mng_reg_id
                columns : sr_fest
    """

    def _proc(df):
        df = df[['mng_reg_id', 'sr_fest']].set_index('mng_reg_id')
        sr_all = df['sr_fest'].ravel()
        sr_ub = np.percentile(sr_all, 80)
        if sr_all.shape[0] < 5:
            sr_ub = min(sr_ub, 3)
        df.loc[df['sr_fest'] > sr_ub, 'sr_fest'] = sr_ub
        return df

    sr_fest_p = sr_fest.reset_index().groupby('class_2').apply(_proc)

    return sr_fest_p


def add_date_info(sr_fest, date_start, date_end):
    """
    Adding date information to sales ratio data

    Parameters
    ----------
    sr_fest : pd.DataFrame
              Sales ratio in the festival duration
              index : class_2, mng_reg_id
              columns : sr_fest

    date_start : date
                 Festival starting date

    date_end : date
               Festival ending date

    Returns
    -------
    sr_fest_p : pd.DataFrame
                Sales ratio in the festival duration after processed
                index : position
                columns : class_2, mng_reg_id, date, sr_fest
    """

    idx_sr = pd.MultiIndex.from_product(
        [*(sr_fest.index.unique().levels),
         pd.date_range(date_start, date_end)],
        names=['class_2', 'mng_reg_id', 'date'])

    sr_fest_p = pd.DataFrame(index=idx_sr).reset_index('date') \
        .join(sr_fest['sr_fest']).reset_index().fillna({'sr_fest': 1.0})

    return sr_fest_p


def cal_mean_sr(sr_fest, date_dec):
    """
    Calculating the mean ratio of sales in the decision duration containing
    festivals

    Parameters
    ----------
    sr_fest : pd.DataFrame
              Sales ratio in the festival duration
              index : class_2, mng_reg_id
              columns : date, sr_fest

    date_dec : date
               Decision making date

    Returns
    -------
    sr_mean : pd.DataFrame
              The mean sales ratio in the decision duration containing
              festivals
              index : class_2, mng_reg_id
              columns : dr_fest
    """

    cols_grp = ['class_2', 'mng_reg_id']

    dur_week = 7
    dur_dec_std = 7

    sr_fest_uni = \
        sr_fest['sr_fest'].reset_index(cols_grp).groupby(cols_grp).max()

    date_end = sr_fest['date'].max()
    dur_fest = sr_fest['date'].unique().shape[0]
    dur_dec = (date_end - pd.to_datetime(date_dec)).days + 1
    week_num = np.ceil(dur_dec/dur_week)
    sr_mean = (week_num*dur_week - (1 - sr_fest_uni)*dur_fest)/dur_dec_std
    sr_mean.rename(columns={'sr_fest': 'dr_fest'}, inplace=True)

    return sr_mean


def cal_fest_dr(po, date_dec):
    """
    Calculating demand ratio by festivals

    Parameters
    ----------
    po : pd.DataFrame
         Crossed product and organization information
         index : prod_id, color_id, size_id, org_id
         columns : size_order, year, season_id, brand_id, class_0, class_1,
                   class_2, prov_id, city_id, dist_id, mng_reg_id, qs_min,
                   qd_min, is_store, is_new, is_rep, is_trans, is_ret, not_in,
                   not_out, to_emp

    date_dec : date
               Decision making date

    Returns
    -------
    dr_fest : pd.DataFrame
              Demand ratio by festivals
              index : prod_id, color_id, org_id
              columns : dr_fest
    """

    cols_grp = ['prod_id', 'color_id', 'org_id']
    cols_grp2 = ['class_2', 'mng_reg_id', 'date']

    # Loading festival information
    fest_info = load_fut_fest_info(fest_lst, date_dec)

    sr_fest = pd.DataFrame()
    for fest_name in fest_info.index.unique():
        # Loading the previous selected festival information
        fest_info_pre = load_pre_fest_info(fest_name, date_dec)
        if fest_info_pre.empty:
            continue
        date_pre_fest_start = fest_info_pre.at[fest_name, 'date_fest_start']
        date_pre_fest_end = fest_info_pre.at[fest_name, 'date_fest_end']
        # Loading sales data from the date 7 days before the previous selected
        # festival to the end
        s = load_sales_data(date_pre_fest_start, date_pre_fest_end)

        # Calculating the mean daily sales during the festival
        s_mean_in_fest, s_mean_bf_fest = \
            cal_mean_sales(s, date_pre_fest_start, date_pre_fest_end)

        # Calculating ratio of sales during the festival to that before the
        # festival
        sr_fest_sel = cal_fest_sr(s_mean_in_fest, s_mean_bf_fest)

        # Processing outliers
        sr_fest_sel = proc_outliers(sr_fest_sel)

        # Adding date information
        date_fest_start = fest_info.at[fest_name, 'date_fest_start']
        date_fest_end = fest_info.at[fest_name, 'date_fest_end']
        sr_fest_sel = add_date_info(sr_fest_sel,
                                    max(date_fest_start,
                                        pd.to_datetime(date_dec)),
                                    date_fest_end)

        sr_fest = sr_fest.append(sr_fest_sel, sort=False)

    if sr_fest.empty:
        dr_fest = pd.DataFrame(
            columns=cols_grp + ['dr_fest']).set_index(cols_grp)
        return dr_fest

    # Calculating the maximal sales ratio of each day during festivals
    sr_fest = sr_fest.groupby(cols_grp2).max().reset_index(cols_grp2[-1])

    # Calculating the mean sales ratio in the decision duration
    sr_mean = cal_mean_sr(sr_fest, date_dec)

    po_skcs = po[cols_grp2[:2]].reset_index(cols_grp).dropna() \
        .drop_duplicates().set_index(cols_grp2[:2])

    dr_fest = sr_mean.join(po_skcs, how='inner').set_index(cols_grp)

    return dr_fest


def cal_adj_sr(pi, oi, date_dec):
    """
    Calculating sales adjustment ratio in the previous 2 weeks

    Parameters
    ----------
    pi : pd.DataFrame
         Product information
         index : prod_id, color_id
         columns : size_id, size_order, year, season_id, brand_id, class_0,
                   class_1, class_2

    oi : pd.DataFrame
         Organization information
         index : org_id
         columns : prov_id, city_id, dist_id, mng_reg_id, is_store(1)

    date_dec : date
               Decision making date

    Returns
    -------
    sr_adj : pd.DataFrame
             Sales adjustment ratio in the previous 2 weeks
             index : prod_id, color_id, org_id
             columns : date, sr_fest
    """

    cols_grp = ['prod_id', 'color_id', 'org_id']
    cols_grp2 = ['class_2', 'mng_reg_id', 'date']

    # Loading the past festival information in the previous 2 weeks
    fest_info_past = load_past_fest_info(fest_lst, date_dec)

    sr_fest = pd.DataFrame()
    for fest_name in fest_info_past.index.unique():
        date_past_fest_start = fest_info_past.at[fest_name, 'date_fest_start']
        date_past_fest_end = fest_info_past.at[fest_name, 'date_fest_end']
        date_near_fest_start = \
            max(date_past_fest_start,
                pd.to_datetime(date_dec - dt.timedelta(days=14)))
        date_near_fest_end = \
            min(date_past_fest_end,
                pd.to_datetime(date_dec - dt.timedelta(days=1)))
        # Loading sales data from the day 7 days before the past festival to
        # the end or the day before decision
        s = load_sales_data(date_past_fest_start.date(),
                            date_near_fest_end.date())

        # Calculating the mean daily sales during the festival
        s_mean_in_fest, s_mean_bf_fest = \
            cal_mean_sales(s, date_past_fest_start, date_near_fest_end)

        # Calculating ratio of sales during the festival to that before the
        # festival
        sr_fest_sel = cal_fest_sr(s_mean_in_fest, s_mean_bf_fest)

        # Processing outliers
        sr_fest_sel = proc_outliers(sr_fest_sel)

        # Adding date information
        sr_fest_sel = add_date_info(sr_fest_sel,
                                    date_near_fest_start, date_near_fest_end)

        sr_fest = sr_fest.append(sr_fest_sel, sort=False)

    if sr_fest.empty:
        sr_adj = pd.DataFrame(
            columns=cols_grp + ['date', 'sr_fest']).set_index(cols_grp)
        return sr_adj

    # Calculating the maximal sales ratio of each day during festivals
    sr_adj = sr_fest.groupby(cols_grp2).max().reset_index()

    sr_adj = sr_adj.set_index(cols_grp2[0]).join(
        pi[cols_grp2[0]].reset_index(cols_grp[:2]).drop_duplicates()
        .set_index(cols_grp2[0]), how='inner').reset_index()
    sr_adj = sr_adj.set_index(cols_grp2[1]).join(
        oi.loc[oi['is_store'] == 1, cols_grp2[1]].reset_index()
        .drop_duplicates().set_index(cols_grp2[1]), how='inner').reset_index()
    sr_adj = sr_adj[cols_grp + ['date', 'sr_fest']].set_index(cols_grp)

    return sr_adj
