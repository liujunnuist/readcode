# -*- coding: utf-8 -*-

import datetime as dt

import pandas as pd

from check.TypeAssert import typeassert


class SaleDataPrepare(object):
    def __init__(self):
        pass

    @typeassert(s=pd.DataFrame, date_dec=dt.date)
    def agg_week_sales(self, s, date_dec):
        """
        Aggregating weekly sales

        Parameters
        ----------
        s : pd.DataFrame
            Sales data
            index : prod_id, color_id, size, org_id
            columns : date_sell, s

        date_dec : date
                   Decision date

        Returns
        -------
        s_week : pd.DataFrame
                 Weekly sales
                 index : prod_id, color_id, size, org_id, week_no
                 columns : s
        """

        cols_grp = ['prod_id', 'color_id', 'size', 'org_id', 'week_no']

        # Splitting dates into '7-days' grouped weeks
        s_week = s.reset_index()
        s_week['week_no'] = (s_week['date_sell'] -
                             pd.to_datetime(date_dec)).dt.days // 7

        # Calculating weekly sales
        s_week = s_week[cols_grp + ['s']].groupby(cols_grp).sum()
        s_week.loc[s_week['s'] < 0, 's'] = 0

        return s_week

    @typeassert(pi=pd.DataFrame, oi=pd.DataFrame, i0=pd.DataFrame, s=pd.DataFrame)
    def cal_store_we(self, pi, oi, i0, s):
        """
        Calculating store normalized sales weights

        Parameters
        ----------
        pi : pd.DataFrame
             Product information
             index : prod_id, color_id
             columns : size, size_order, year, season_id, class_0

        oi : pd.DataFrame
             Organization basic information
             index : org_id
             columns : prov_id, city_id, dist_id, mng_reg_id, is_store(1)

        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r

        s : pd.DataFrame
            Weekly sales data
            index : prod_id, color_id, size, org_id, week_no
            columns : s

        Returns
        -------
        sw_store : pd.DataFrame
                   Sales weight of each season/class/store
                   index : season_id, class_0, mng_reg_id, org_id
                   columns : sw_store
        """

        cols_grp = ['prod_id', 'color_id', 'org_id']
        cols_grp2 = ['season_id', 'class_0', 'mng_reg_id', 'org_id']

        # Calculating initial inventory and sales of each skc/store
        i0_org = i0.reset_index(cols_grp).groupby(cols_grp).sum()
        s_org = s.reset_index(cols_grp).groupby(cols_grp).sum()
        si = oi.loc[oi['is_store'] == 1, cols_grp2[2]].copy()

        # Merging with product information and store information
        pi_c = pi[cols_grp2[:2]].reset_index().drop_duplicates()
        sw_store = i0_org.join(s_org, how='outer').fillna(0).reset_index()
        sw_store = sw_store.set_index(cols_grp[:2]).join(
            pi_c.set_index(cols_grp[:2]), how='inner').reset_index()
        sw_store = sw_store.set_index('org_id').join(si, how='inner').reset_index()

        # Marking out valid skc/store with either initial inventory or sales
        # being positive
        sw_store['sn'] = 0
        sw_store.loc[(sw_store['i0'] > 0) | (sw_store['s'] > 0), 'sn'] = 1

        # Calculating sum sales and sum skc number of each season/class/store
        sw_store = sw_store[cols_grp2 + ['s', 'sn']].groupby(cols_grp2).sum()
        sw_store.reset_index(inplace=True)
        sw_store.drop(sw_store[sw_store['sn'] <= 0].index, inplace=True)

        # Calculating mean skc sales of each season/class/store
        sw_store['s_mean'] = sw_store['s'] / sw_store['sn']
        sw_store.loc[sw_store['s_mean'] < 0, 's_mean'] = 0
        sw_store['s_mean'] += 0.001

        # Normalizing the mean sales as the sales weight of each season/class/store
        sw_store.set_index(cols_grp2[:3], inplace=True)
        sw_store['s_max'] = sw_store['s_mean'].max(level=cols_grp2[:3])
        sw_store.reset_index(inplace=True)
        sw_store['sw_store'] = sw_store['s_mean'] / sw_store['s_max']

        sw_store = sw_store[cols_grp2 + ['sw_store']].set_index(cols_grp2)

        return sw_store

    @typeassert(pi=pd.DataFrame, oi=pd.DataFrame, s=pd.DataFrame, sw_store=pd.DataFrame)
    def cal_eqv_sales(self, pi, oi, s, sw_store):
        """
        Calculating equivalent weekly sales

        Parameters
        ----------
        pi : pd.DataFrame
             Product information
             index : prod_id, color_id
             columns : size, size_order, year, season_id, class_0

        oi : pd.DataFrame
             Organization basic information
             index : org_id
             columns : prov_id, city_id, dist_id, mng_reg_id, is_store(1)

        s : pd.DataFrame
            Sales data
            index : prod_id, color_id, size, org_id, week_no
            columns : s

        sw_store : pd.DataFrame
                   Sales weight of each store
                   index : season_id, class_1, mng_reg_id, org_id
                   columns : sw_store

        Returns
        -------
        s_eq : pd.DataFrame
               Equivalent sales data
               index : prod_id, color_id, size, org_id, week_no
               columns : s, sw_store
        """

        cols_grp = ['season_id', 'class_0', 'mng_reg_id', 'org_id']
        cols_grp2 = ['prod_id', 'color_id', 'size', 'org_id', 'week_no']

        si = oi.loc[oi['is_store'] == 1, cols_grp[2]].copy()

        # Merging data
        pi_c = pi[cols_grp[:2]].reset_index().drop_duplicates()
        s_eq = s.reset_index().set_index(['prod_id', 'color_id']).join(
            pi_c.set_index(['prod_id', 'color_id']), how='inner')
        s_eq.reset_index(inplace=True)
        s_eq = s_eq.set_index('org_id').join(si, how='inner').reset_index()
        s_eq = s_eq.set_index(cols_grp).join(sw_store, how='inner').reset_index()

        # Calculating equivalent weekly sales
        s_eq = s_eq.loc[s_eq['sw_store'] > 0].copy()
        s_eq['s'] /= s_eq['sw_store']

        s_eq = s_eq[cols_grp2 + ['s', 'sw_store']].set_index(cols_grp2)

        return s_eq

    @typeassert(oi=pd.DataFrame, s_eq=pd.DataFrame)
    def proc_sales_outlier(self, oi, s_eq):
        """
        Processing weekly sales outliers

        Parameters
        ----------
        oi : pd.DataFrame
             Organization basic information
             index : org_id
             columns : prov_id, city_id, dist_id, mng_reg_id, is_store(1)

        s_eq : pd.DataFrame
               Equivalent sales data
               index : prod_id, color_id, size, org_id, week_no
               columns : s, sw_store

        Returns
        -------
        s_p : pd.DataFrame
              Sales after outliers dropped
              index : prod_id, color_id, size, org_id, week_no
              columns : s
        """

        def _get_s_ub(df):
            # Calculating daily sales frequency
            vc = df['s'].value_counts().reset_index()
            vc.columns = ['s', 'cnt']
            # Calculating percentile point of frequency
            vc['p_cnt'] = vc['cnt'].cumsum() / vc['cnt'].sum()
            s_ub = 5 if vc['cnt'].max() <= 4 else 10
            if vc['p_cnt'].values[0] <= 0.90:
                return min(vc.loc[vc['p_cnt'] <= 0.90, 's'].max(), s_ub)
            else:
                return min(vc['s'].values[0], s_ub)

        cols_grp1 = ['prod_id', 'color_id', 'size', 'dist_id']
        cols_grp2 = ['prod_id', 'color_id', 'size', 'org_id', 'week_no']

        si = oi.loc[oi['is_store'] == 1, 'dist_id'].copy()

        # Calculating percentile point of sales frequency
        s_c = s_eq.loc[s_eq['s'] > 0, 's'].reset_index()
        s_c = s_c.set_index('org_id').join(si)
        s_ub = s_c[cols_grp1 + ['s']].groupby(cols_grp1).apply(_get_s_ub)
        s_ub.name = 's_ub'

        # Replacing sales being higher than the upper-bound with the upper-bound
        s_p = s_eq.reset_index()
        s_p = s_p.set_index('org_id').join(si).reset_index()

        s_p = s_p.set_index(cols_grp1).join(s_ub).reset_index()
        s_p.fillna({'s_ub': 1}, inplace=True)
        s_p['s'] = s_p[['s', 's_ub']].min(axis=1)
        s_p['s'] *= s_p['sw_store']

        s_p = s_p[cols_grp2 + ['s']].set_index(cols_grp2)

        return s_p

    @typeassert(pi=pd.DataFrame, oi=pd.DataFrame, i0=pd.DataFrame, s=pd.DataFrame, date_dec=dt.date)
    def proc_sales_data(self, pi, oi, i0, s, date_dec):
        """
        Pre-processing sales data

        Parameters
        ----------
        pi : pd.DataFrame
             Product information
             index : prod_id, color_id
             columns : size, size_order, year, season_id, class_0

        oi : pd.DataFrame
             Organization basic information
             index : org_id
             columns : prov_id, city_id, dist_id, mng_reg_id, is_store(1)

        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r

        s : pd.DataFrame
            Sales data
            index : prod_id, color_id, size, org_id
            columns : date_sell, s

        date_dec : date
                   Decision making date

        Returns
        -------
        s_p : pd.DataFrame
              Sales data after processed
              index : prod_id, color_id, size, org_id, week_no
              columns : s
        """

        # Aggregating weekly sales
        s_p = self.agg_week_sales(s, date_dec)

        # Calculating store normalized sales weights
        sw_store = self.cal_store_we(pi, oi, i0, s_p)

        # Calculating equivalent weekly sales
        s_eq = self.cal_eqv_sales(pi, oi, s_p, sw_store)

        # Processing weekly sales outliers
        s_p = self.proc_sales_outlier(oi, s_eq)

        return s_p
