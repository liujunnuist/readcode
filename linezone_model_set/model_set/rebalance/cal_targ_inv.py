# coding: utf-8

import math
from functools import partial
from multiprocessing import Pool, cpu_count

import numpy as np
import pandas as pd
import pyscipopt as slv

from check.TypeAssert import typeassert
from core.optimazation import Optimazation
from utils.logger import lz_logger
from model_set.rebalance.settings import model_param
from model_set.rebalance import cal_fest_ratio as cfr


class TargetInventory(object):
    def __init__(self):
        pass

    def cal_basic_dem(self, ms, i0, s, w):
        """
        Calculating basic demand of the next week of each sks/store

        Parameters
        ----------
        ms : pd.DataFrame
             Main sizes
             index : prod_id, color_id, org_id
             columns : size, sp_size

        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r, i0_sum

        s : pd.DataFrame
            Sales data
            index : prod_id, color_id, size, org_id, week_no
            columns : s

        w : dict
            Week sales weights
            key : w_1, w_2
            value : week sales weight

        Returns
        -------
        d_base : pd.DataFrame
                 Basic demand
                 index : prod_id, color_id, size, org_id
                 columns : d
        """

        cols_grp = ['prod_id', 'color_id', 'size', 'org_id']

        # Splitting weekly sales
        s_full = s.reset_index('week_no')
        s_pre2 = s_full.loc[s_full['week_no'] == -2, ['s']].copy()
        s_pre2.rename(columns={'s': 's_2'}, inplace=True)
        s_pre1 = s_full.loc[s_full['week_no'] == -1, ['s']].copy()
        s_pre1.rename(columns={'s': 's_1'}, inplace=True)

        # Adding sales of the previous week with 1 if end inventory is 0, but
        # limited not to be higher than the highest value, for the main sizes
        ms2 = ms[['size']].reset_index()
        ms2['is_ms'] = 1
        ms2.set_index(cols_grp, inplace=True)
        s_pre1 = s_pre1.join([i0[['i0']], ms2]).reset_index('org_id')
        s_pre1['s_ub'] = s_pre1['s_1'].max(level=cols_grp[:3])
        s_pre1 = s_pre1.reset_index().set_index(cols_grp)
        s_pre1.loc[(s_pre1['i0'] == 0) &
                   (s_pre1['s_1'] > 0) &
                   (s_pre1['is_ms'] == 1) &
                   (s_pre1['s_1'] < s_pre1['s_ub']), 's_1'] += 1
        s_pre1.drop(['i0', 'is_ms', 's_ub'], axis=1, inplace=True)

        # Setting weekly sales weights
        d_base = s_pre2.join(s_pre1).reset_index().fillna({'s_1': 0, 's_2': 0})
        d_base['w_2'] = w['w_2']
        d_base['w_1'] = w['w_1']
        d_base.loc[d_base['s_1'] <= 0, 'w_2'] = 0.8
        d_base.loc[d_base['s_2'] <= 0, 'w_1'] = 1.0

        # Calculating basic demand of the next week
        d_base['d'] = (d_base[['s_2', 'w_2']].prod(axis=1) +
                       d_base[['s_1', 'w_1']].prod(axis=1))

        d_base = d_base[cols_grp + ['d']].set_index(cols_grp)

        return d_base

    def adj_basic_dem(self, d_base, dr_fest):
        """
        Adjusting predicted basic demand

        Parameters
        ----------
        d_base : pd.DataFrame
                 Basic demand
                 index : prod_id, color_id, size_id, org_id
                 columns : d

        dr_fest : pd.DataFrame
                  Demand ratio by festivals
                  index : prod_id, color_id, org_id
                  columns : dr_fest

        Returns
        -------
        d_a : pd.DataFrame
              Basic demand after adjusted
              index : prod_id, color_id, size_id, org_id
              columns : d
        """

        cols_grp = ['prod_id', 'color_id', 'size', 'org_id']

        d_a = d_base.reset_index('size').join(dr_fest).reset_index()
        d_a.fillna({'dr_fest': 1.0}, inplace=True)

        d_a['d'] = d_a[['d', 'dr_fest']].prod(axis=1)

        d_a = d_a[cols_grp + ['d']].set_index(cols_grp)

        return d_a

    def cal_dem_bd(self, ms, s, d_base):
        """
        Calculating demand lower- and upper- bounds

        Parameters
        ----------
        ms : pd.DataFrame
             Main sizes
             index : prod_id, color_id, org_id
             columns : size, sp_size

        s : pd.DataFrame
            Sales data
            index : prod_id, color_id, size, org_id, week_no
            columns : s

        d_base : pd.DataFrame
                 Basic demand
                 index : prod_id, color_id, size, org_id
                 columns : d

        Returns
        -------
        d : pd.DataFrame
            Demand lower- and upper- bounds
            index : prod_id, color_id, size, org_id
            columns : d_lb, d_ub
        """

        cols_grp = ['prod_id', 'color_id', 'size', 'org_id']
        cols_grp2 = ['prod_id', 'color_id', 'org_id']

        # Calculating mean sales proportion of each size within the skc/org
        s_cum = s.reset_index(cols_grp).groupby(cols_grp).sum()
        sp_size = ms.reset_index().set_index(cols_grp).join(s_cum).reset_index()
        sp_size.fillna({'s': 0}, inplace=True)
        sp_size.loc[sp_size['s'] < 0, 's'] = 0
        sp_size['s'] += 1
        sp_size.set_index(cols_grp2, inplace=True)
        sp_size['s_sum'] = sp_size['s'].sum(level=cols_grp2)
        sp_size['sp_sum'] = sp_size['sp_size'].sum(level=cols_grp2)
        sp_size.reset_index(inplace=True)
        sp_size['sp_size2'] = sp_size['s'] / sp_size['s_sum']
        sp_size['sp_size'] /= sp_size['sp_sum']
        sp_size['sp_size'] = sp_size[['sp_size', 'sp_size2']].mean(axis=1)

        # Transforming sales proportions within the preset range
        sp_size = sp_size[cols_grp + ['sp_size']].set_index(cols_grp2)
        sp_size['max'] = sp_size['sp_size'].max(level=cols_grp2)
        sp_size['min'] = sp_size['sp_size'].min(level=cols_grp2)
        sp_size.reset_index(inplace=True)
        sp_size['min_ub'] = sp_size['max'] / 2.5
        sp_size['min_a'] = sp_size[['min', 'min_ub']].max(axis=1)
        sp_size['range'] = sp_size['max'] - sp_size['min'] + 1.0E-10
        sp_size['range_a'] = sp_size['max'] - sp_size['min_a']
        sp_size['diff'] = sp_size['max'] - sp_size['sp_size']
        sp_size['sp_size_n'] = (sp_size['max'] -
                                sp_size['range_a'] / sp_size['range'] *
                                sp_size['diff'])
        # Normalizing sales proportion
        sp_size['sp_norm_min'] = sp_size['sp_size_n'] / sp_size['max']
        sp_size['sp_norm_max'] = sp_size['sp_size_n'] / sp_size['min_a']

        # Merging basic demand and sales proportions
        d = sp_size.set_index(cols_grp).join(d_base).reset_index()

        # Calculating demand lower- and upper- bounds of the main sizes
        d.set_index(cols_grp2, inplace=True)
        d['d_full'] = d['d'].mean(level=cols_grp2)
        d.reset_index(inplace=True)
        d['d_lb'] = d[['d_full', 'sp_norm_min']].prod(axis=1)
        d['d_ub'] = d[['d_full', 'sp_norm_max']].prod(axis=1)
        d['d_lb'] = d[['d', 'd_lb']].mean(axis=1)
        d['d_ub'] = d[['d', 'd_ub']].mean(axis=1)
        d = d[cols_grp + ['d_lb', 'd_ub']].set_index(cols_grp)

        # Filling demand lower- and upper- bounds of the non-main sizes
        d = d_base.join(d).reset_index()
        d['d_lb'].fillna(d['d'], inplace=True)
        d['d_ub'].fillna(d['d'], inplace=True)

        d = d[cols_grp + ['d_lb', 'd_ub']].set_index(cols_grp)

        return d

    def cal_dem(self, po, ms, i0, s, w, date_dec, exe_fest_adj):


        """
        Calculating demand

        Parameters
        ----------
        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        ms : pd.DataFrame
             Main sizes
             index : prod_id, color_id, org_id
             columns : size, sp_size

        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r, i0_sum

        s : pd.DataFrame
            Sales data
            index : prod_id, color_id, size, org_id, week_no
            columns : s

        w : dict
            Week sales weights
            key : w_1, w_2
            value : week sales weight

        date_dec : date
               Decision making date

        exe_fest_adj : bool
                       Whether or not executing demand adjustment by festival

        Returns
        -------
        d : pd.DataFrame
            Demand lower- and upper- bounds
            index : prod_id, color_id, size, org_id
            columns : d_lb, d_ub
        """

        cols_grp = ['prod_id', 'color_id', 'org_id']

        # Calculating basic demand of the next week
        d_base = self.cal_basic_dem(ms, i0, s, w)

        # Calculating demand ratio by festivals
        if exe_fest_adj:
            dr_fest = cfr.cal_fest_dr(po, date_dec)
        else:
            dr_fest = pd.DataFrame(
                columns=cols_grp + ['dr_fest']).set_index(cols_grp)

        # Adjusting demand
        d_a = self.adj_basic_dem(d_base, dr_fest)

        # Calculating demand lower- and upper- bounds
        d = self.cal_dem_bd(ms, s, d_a)

        return d

    def cal_safety_stock(self, po, ms, i0, d, sales_we, qss_base):
        """
        Calculating safety stock

        Parameters
        ----------
        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        ms : pd.DataFrame
             Main sizes
             index : prod_id, color_id, org_id
             columns : size, sp_size

        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r, i0_sum

        d : pd.DataFrame
            Predicted demand lower- and upper- bounds
            index : prod_id, color_id, size, org_id
            columns : d_lb, d_ub

        sales_we : pd.DataFrame
                   Sum sales weights of skc, size, and store
                   index : prod_id, color_id, size, org_id
                   columns : skc_we_a, skc_we_d, store_we_a, store_we_d, sks_we_a,
                             sks_we_d, sum_we_a, sum_we_d

        qss_base : float
                   Basic safety stock

        Returns
        -------
        qss : pd.DataFrame
              Safety stock
              index : prod_id, color_id, size, org_id
              columns : qss
        """

        cols_grp = ['prod_id', 'color_id', 'size', 'org_id']
        cols_grp2 = ['prod_id', 'color_id', 'org_id']

        # Calculating initial inventory, demand lower-bound of each skc/org
        i0_skcs = i0['i0'].reset_index(cols_grp2).groupby(cols_grp2).sum()
        d_skcs = d['d_lb'].reset_index(cols_grp2).groupby(cols_grp2).sum()
        ms2 = ms['size'].reset_index()
        ms2['is_ms'] = 1
        ms2.set_index(cols_grp, inplace=True)

        # Merging sum sales weight, the main size markers, initial inventory and
        # demand lower-bound
        idx_sel = (po['is_store'] == 1) & (po['is_new'] == 0)
        qss = pd.DataFrame(index=po[idx_sel].index.drop_duplicates())
        qss = qss.join([sales_we[['sum_we_a']], ms2]).reset_index('size')
        qss = qss.join([i0_skcs, d_skcs], how='inner').reset_index()
        qss.fillna({'sum_we_a': sales_we['sum_we_a'].min(), 'is_ms': 0, 'i0': 0,
                    'd_lb': 0}, inplace=True)

        # Normalizing sum sales weight by which the basic safety stock is
        # multiplied as safety stock of the main sizes
        we_min = sales_we['sum_we_a'].min()
        we_max = sales_we['sum_we_a'].max()
        we_range = we_max - we_min + 1.0E-10
        we_diff_a = qss['sum_we_a'] - we_min
        qss['qss'] = qss_base * we_diff_a / we_range * qss['is_ms']
        # Setting safety stock to be 0 if both initial inventory and demand lower
        # bound of skc is 0
        qss.loc[(qss['i0'] <= 0) & (qss['d_lb'] <= 0), 'qss'] = 0

        qss = qss[cols_grp + ['qss']].set_index(cols_grp)

        return qss

    def cal_basic_it(self, po, i0, s, itp, cal_ti_mode):
        """
        Calculating basic target inventory

        Parameters
        ----------
        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r, i0_sum

        s : pd.DataFrame
            Sales data
            index : prod_id, color_id, size, org_id, week_no
            columns : s

        itp : pd.DataFrame
              Target inventory of the previous day
              index : prod_id, color_id, size, org_id
              columns : itp

        cal_ti_mode : str
                      Mode of calculating target inventory

        Returns
        -------
        it0 : pd.DataFrame
              Basic target inventory
              index : prod_id, color_id, size, org_id
              columns : it_base
        """

        idx_sel = (po['is_store'] == 1) & (po['is_new'] == 0)
        it0 = pd.DataFrame(index=po[idx_sel].index.drop_duplicates())

        # Setting target inventory of the previous day to be the basic target
        # inventory if calculating mode is 'continued', otherwise, setting
        # sellable inventory to be the basic target inventory if calculating mode
        # is 'renewed'
        if cal_ti_mode == 'continued' and not itp.empty:
            it0 = it0.join(itp).fillna(0).rename(columns={'itp': 'it_base'})
        elif cal_ti_mode == 'renewed' or itp.empty:
            # Calculating sellable inventory of the previous week
            s_full = s.reset_index('week_no')
            s_pre1 = s_full.loc[s_full['week_no'] == -1, ['s']].copy()
            is0 = i0[['i0']].join(s_pre1, how='inner').fillna(0)
            is0['is'] = is0[['i0', 's']].sum(axis=1)
            is0.loc[is0['i0'] >= is0['s'], 'is'] = \
                is0.loc[is0['i0'] >= is0['s'], 'i0']
            is0.loc[is0['is'] < 0, 'is'] = 0

            it0 = it0.join(is0['is']).fillna(0).rename(columns={'is': 'it_base'})

        return it0

    def cal_it_bd(self, i0, s, io, d, qss):
        """
        Calculating target inventory lower- and upper- bounds

        Parameters
        ----------
        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r, i0_sum

        s : pd.DataFrame
            Sales data
            index : prod_id, color_id, size, org_id, week_no
            columns : s

        io : pd.DataFrame
             Markers of existed moving -in/-out
             index : prod_id, color_id, size, org_id
             columns : has_in, has_out

        d : pd.DataFrame
            Demand lower- and upper- bounds
            index : prod_id, color_id, size, org_id
            columns : d_lb, d_ub

        qss : pd.DataFrame
              Safety stock
              index : prod_id, color_id, size, org_id
              columns : qss

        Returns
        -------
        it_bd : pd.DataFrame
                Target inventory lower- and upper- bounds
                index : prod_id, color_id, size, org_id
                columns : it_lb, it_ub
        """

        cols_grp = ['prod_id', 'color_id', 'size', 'org_id']
        cols_grp2 = ['prod_id', 'color_id', 'org_id']

        # Calculating sum initial inventory and sum sales of the previous week of
        # each skc/org
        i0_skcs = i0['i0_sum'].reset_index(cols_grp2).groupby(cols_grp2).sum()
        i0_skcs.rename(columns={'i0_sum': 'i0_skcs'}, inplace=True)
        s1_skcs = s.reset_index('week_no')
        s1_skcs = s1_skcs.loc[s1_skcs['week_no'] == -1, 's'].copy()
        s1_skcs = s1_skcs.reset_index(cols_grp2).groupby(cols_grp2).sum()
        s1_skcs.rename(columns={'s': 's_skcs'}, inplace=True)

        # Merging data and calculating sum demand upper-bound of each skc/org
        it_bd = i0[['i0_sum']].join([d, qss, io[['has_in']]], how='inner')
        it_bd = it_bd.reset_index('size').join([i0_skcs, s1_skcs])
        it_bd.fillna({'i0_sum': 0, 'd_lb': 0, 'd_ub': 0, 'qss': 0, 'has_in': 0,
                      'i0_skcs': 0, 's_skcs': 0}, inplace=True)
        it_bd['d_skc'] = it_bd['d_ub'].sum(level=cols_grp2)
        it_bd.reset_index(inplace=True)

        # Setting safety stock to be 0 if sum demand upper-bound is 0
        it_bd.loc[it_bd['d_skc'] <= 0, 'qss'] = 0
        # Calculating basic target inventory lower- and upper- bounds by summing
        # demand bounds and safety stock, and setting it to be 0 if sum initial
        # inventory is 0 and sum sales of the previous week is less than or equal
        # to 1 for each skc/org
        it_bd['is_valid'] = 1
        it_bd.loc[(it_bd['i0_skcs'] <= 0) & (it_bd['s_skcs'] <= 1), 'is_valid'] = 0
        it_bd['it_lb'] = it_bd[['d_lb', 'qss']].sum(axis=1) * it_bd['is_valid']
        it_bd['it_ub'] = it_bd[['d_ub', 'qss']].sum(axis=1) * it_bd['is_valid']
        # Setting basic target inventory lower- and upper- bounds to be 1 if they
        # are between 0 ~ 1
        it_bd.loc[(it_bd['it_lb'] > 0) & (it_bd['it_lb'] < 1), 'it_lb'] = 1
        it_bd['it_ub'] = it_bd[['it_lb', 'it_ub']].max(axis=1)
        # Setting the basic target inventory upper-bound to be 1 at least if the
        # initial inventory is positive
        it_bd.loc[(it_bd['i0_sum'] > 0) & (it_bd['it_ub'] <= 0), 'it_ub'] = 1

        it_bd = it_bd[cols_grp + ['it_lb', 'it_ub']].set_index(cols_grp)

        return it_bd

    def extr_po_ti(self, po, i0, itp, it_bd):
        """
        Extracting target products and organizations for calculating target
        inventory

        Parameters
        ----------
        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r, i0_sum

        itp : pd.DataFrame
              Target inventory of the previous day
              index : prod_id, color_id, size, org_id
              columns : itp

        it_bd : pd.DataFrame
                Target inventory lower- and upper- bounds
                index : prod_id, color_id, size, org_id
                columns : it_lb, it_ub

        Returns
        -------
        po_ti : pd.DataFrame
                Target products and organizations of calculating target inventory
                index : prod_id, color_id, size, org_id
                columns : mng_reg_id
        """

        cols_grp = ['prod_id', 'color_id', 'size', 'org_id']
        cols_grp2 = ['prod_id', 'color_id', 'org_id']

        # Merging initial inventory, receiving, basic target inventory, and target
        # product markers, and calculating sum initial inventory and sum basic
        # target inventory of each skc/org
        idx_sel = (po['is_store'] == 1) & (po['is_new'] == 0)
        po_ti = po.loc[idx_sel, ['mng_reg_id', 'is_new', 'to_emp']].copy()
        po_ti = po_ti.join([i0[['i0_sum']], it_bd]).reset_index()
        po_ti.fillna({'i0_sum': 0, 'it_lb': 0, 'it_ub': 0}, inplace=True)
        po_ti.set_index(cols_grp2, inplace=True)
        po_ti['i0_sum_skcs'] = po_ti['i0_sum'].sum(level=cols_grp2)
        po_ti['it0_skcs'] = po_ti['it_ub'].sum(level=cols_grp2)
        po_ti.reset_index(inplace=True)

        # Marking out skc/org to calculate target inventory
        po_ti['has_ti'] = 0
        # For each skc being marked as the target product, stores to calculate
        # target inventory must
        # 1. have either sum initial inventory or sum basic target inventory being
        #    positive and
        # 2. not be new and
        # 3. not be emptied
        po_ti.loc[((po_ti['i0_sum_skcs'] > 0) | (po_ti['it0_skcs'] > 0)) &
                  (po_ti['is_new'] == 0) &
                  (po_ti['to_emp'] == 0), 'has_ti'] = 1

        # Target skc-org must have sum target inventory of the previous day being
        # positive
        if not itp.empty:
            po_ti = po_ti.set_index(cols_grp).join(itp).reset_index()
            po_ti.set_index(cols_grp2, inplace=True)
            po_ti['itp_skcs'] = po_ti['itp'].fillna(0).sum(level=cols_grp2)
            po_ti.reset_index(inplace=True)
            po_ti.loc[po_ti['itp_skcs'] <= 0, 'has_ti'] = 0

        # Selecting skc/org with moving marker is positive
        po_ti = po_ti.loc[po_ti['has_ti'] == 1, cols_grp + ['mng_reg_id']]
        po_ti = po_ti.set_index(cols_grp).sort_index()

        return po_ti

    def it_opt_solver(self, po, ms, i0, it0, it_bd, cdl, cid):
        """
        Target inventory optimization solver

        Parameters
        ----------
        po : pd.DataFrame
             Crossed products and organizations
             index : prod_id, color_id, size, org_id
             columns : mng_reg_id

        ms : pd.DataFrame
             Main sizes
             index : prod_id, color_id, org_id
             columns : size, sp_size

        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r, i0_sum

        it0 : pd.DataFrame
              Basic target inventory
              index : prod_id, color_id, size, org_id
              columns : it_base

        it_bd : pd.DataFrame
                Target inventory lower- and upper- bounds
                index : prod_id, color_id, size, org_id
                columns : it_lb, it_ub

        cdl : pd.DataFrame
              Unit cost of demand lossing
              index : prod_id, color_id, size, org_id
              columns : cdl_lb, cdl_ub

        cid : pd.DataFrame
              Unit cost of inventory difference
              index : prod_id, color_id, size, org_id
              columns : cid_a, cid_d, cid_rep

        Returns
        -------
        it : dict
             Target inventory
             key : prod_id, color_id, size, org_id
             value : target inventory
        """

        po = po.reset_index('org_id')
        skc_lst = po.reset_index('size').index.unique()
        sku_lst = po.index.unique()

        # Create a new model
        m = slv.Model()

        m = Optimazation(executor=model_param.executor)
        # m.setRealParam('limits/gap', 0.03)
        m.setParam('limits/time', 200)
        M = 10000

        # Creating decision variable
        it = {}
        for prod_id, color_id, size in sku_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            for org_id in po_sel.index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                var_name_it = 'it_' + '_'.join(idx_stc)
                it[idx_stc] = m.addVar(vtype='I', name=var_name_it)

        # Creating assistant variables
        # dlpl : dict
        #        Difference between target inventory and the lower-bound
        #        key : prod_id, color_id, size, org_id
        #        value : quantity of target inventory lower than the lower-bound
        #
        # dlpu : dict
        #        Difference between target inventory and the upper-bound
        #        key : prod_id, color_id, size, org_id
        #        value : quantity of target inventory higher than the upper-bound
        #
        # ida : dict
        #       Absolute difference between target inventory and the basic value
        #       key : prod_id, color_id, size, org_id
        #       value : absolute difference

        dlpl = {}
        dlpu = {}
        ida = {}

        for prod_id, color_id, size in sku_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            for org_id in po_sel.index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                var_name_dlpl = 'dlpl_' + '_'.join(idx_stc)
                var_name_dlpu = 'dlpu_' + '_'.join(idx_stc)
                var_name_ida = 'ida_' + '_'.join(idx_stc)
                dlpl[idx_stc] = m.addVar(vtype='C', name=var_name_dlpl)
                dlpu[idx_stc] = m.addVar(vtype='C', name=var_name_dlpu)
                ida[idx_stc] = m.addVar(vtype='C', name=var_name_ida)

        # Constructing objective function
        # Objective #1
        # Cost by target inventory lower than the lower-bound
        for prod_id, color_id, size in sku_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            for org_id in po_sel.index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                # expr.addTerms(cdl.at[idx_stc, 'cdl_lb'], dlpl[idx_stc])
                m.setObjective(cdl.at[idx_stc, 'cdl_lb'] * dlpl[idx_stc],
                               clear=False)

        # Objective #2
        # Cost by target inventory higher than the upper-bound
        for prod_id, color_id, size in sku_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            for org_id in po_sel.index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                # expr.addTerms(cdl.at[idx_stc, 'cdl_ub'], dlpu[idx_stc])
                m.setObjective(cdl.at[idx_stc, 'cdl_ub'] * dlpu[idx_stc],
                               clear=False)

        # Objective #3
        # Cost by absolute difference between target inventory and the basic value
        for prod_id, color_id, size in sku_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            for org_id in po_sel.index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                # expr.addTerms(cid.at[idx_stc, 'cid_d'], ida[idx_stc])
                m.setObjective(cid.at[idx_stc, 'cid_d'] * ida[idx_stc], clear=False)

        # Setting objective
        m.setMinimize()

        # Constructing assistant equations
        # Equation #1
        # Quantity of target inventory lower than the lower-bound
        for prod_id, color_id, size in sku_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            for org_id in po_sel.index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                dll = m.addVar(lb=None, vtype='C', name='dll')
                dllb = m.addVar(vtype='B', name='dllb')
                m.addCons(dll == it_bd.at[idx_stc, 'it_lb'] - it[idx_stc])
                # dlpl = max(dll, 0)
                m.addCons(dlpl[idx_stc] >= dll)
                m.addCons(dlpl[idx_stc] <= dll + M * (1 - dllb))
                m.addCons(dlpl[idx_stc] <= M * dllb)

        # Equation #2
        # Quantity of target inventory higher than the upper-bound
        for prod_id, color_id, size in sku_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            for org_id in po_sel.index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                dlu = m.addVar(lb=None, vtype='C', name='dlu')
                dlub = m.addVar(vtype='B', name='dlub')
                m.addCons(dlu == it[idx_stc] - it_bd.at[idx_stc, 'it_ub'])
                # dlpu = max(dlu, 0)
                m.addCons(dlpu[idx_stc] >= dlu)
                m.addCons(dlpu[idx_stc] <= dlu + M * (1 - dlub))
                m.addCons(dlpu[idx_stc] <= M * dlub)

        # Equation #3
        # Absolute difference between target inventory and the basic value
        for prod_id, color_id, size in sku_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            for org_id in po_sel.index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                id_ = m.addVar(lb=None, vtype='C', name='id')
                idb = m.addVar(vtype='B', name='idb')
                it_base_sel = max(it0['it_base'].get(idx_stc, 0),
                                  it_bd.at[idx_stc, 'it_lb'])
                m.addCons(id_ == it[idx_stc] - it_base_sel)
                # ida = abs(id_)
                m.addCons(ida[idx_stc] >= id_)
                m.addCons(ida[idx_stc] >= -1 * id_)
                m.addCons(ida[idx_stc] <= id_ + M * (1 - idb))
                m.addCons(ida[idx_stc] <= -1 * id_ + M * idb)

        # Constructing constrains
        # Constrain #1
        # For each skc/store, target inventory of each main size must be positive
        # if that of skc is positive
        for prod_id, color_id in skc_lst:
            po_sel = po.reset_index('size')
            po_sel = po_sel.loc[[(prod_id, color_id)]].set_index('org_id')
            for org_id in po_sel.index.unique():
                idx_skcs = (prod_id, color_id, org_id)
                # Marking out skc/store with positive target inventory
                size_lst = po_sel.loc[org_id, ['size']].values.ravel()
                its = m.addVar(vtype='I', name='its')
                itsb = m.addVar(vtype='B', name='itsb')
                m.addCons(its ==
                          m.quicksum(it[prod_id, color_id, size, org_id]
                                     for size in size_lst))
                m.addCons(its + M * (1 - itsb) >= 1)
                m.addCons(its + M * itsb >= 0)
                m.addCons(its - M * itsb <= 0)
                for size in ms.loc[idx_skcs, ['size']].values.ravel():
                    idx_stc = (prod_id, color_id, size, org_id)
                    if idx_stc in it:
                        m.addCons(it[idx_stc] >= itsb)

        m.optimize()

        it_r = m.get_result(it)
        # m_status = m.getStatus()
        # m_gap = m.getGap()
        # lz_logger.info('=' * 50 + ' ' + m_status + ' ' + '=' * 50)
        # lz_logger.info('-' * 30 + ' Gap: ' + str(m_gap * 100) + '% ' + '-' * 30)
        #
        # it_r = {}
        # if m_status == 'optimal' or m_gap <= 0.05:
        #     for k in it:
        #         it_r[k] = round(m.getVal(it[k]))

        return it_r

    def exec_unit(self, po, ms, i0, it0, it_bd, cdl, cid, mng_reg_id_sel):
        cols_grp = ['prod_id', 'color_id', 'size', 'org_id']

        lz_logger.info('>>>> MNG_REG:{mng_reg_id_sel}'.format(mng_reg_id_sel=mng_reg_id_sel))
        it = {}
        po_reg = po.loc[po['mng_reg_id'] == mng_reg_id_sel].sort_index()
        store_num_reg = \
            po_reg.index.get_level_values('org_id').unique().shape[0]
        lz_logger.info('>>>>> Region store number:{store_num_reg}'.format(store_num_reg=store_num_reg))

        skc_lst = po_reg.reset_index(cols_grp[2:]).index.unique()
        skc_num_sum = skc_lst.shape[0]
        lz_logger.info('>>>>> Region sum SKC number:{skc_num_sum}'.format(skc_num_sum=skc_num_sum))

        skc_num_grp = 30
        skc_num = 0
        for i in range(math.ceil(skc_num_sum / skc_num_grp)):
            i_start = i * skc_num_grp
            i_end = (i + 1) * skc_num_grp
            skc_grp = skc_lst[i_start:i_end]
            lz_logger.info('>>>>> Grouped SKC number:{len_skc_grp}'.format(len_skc_grp=len(skc_grp)))
            po_grp = po_reg.reset_index().set_index(cols_grp[:2])
            po_grp = po_grp.loc[po_grp.index.isin(skc_grp)].copy()
            po_grp = po_grp.reset_index().set_index(cols_grp)

            # Calculating target inventory optimization
            it_grp = self.it_opt_solver(po_grp, ms, i0, it0, it_bd, cdl, cid)
            it.update(it_grp)

            skc_num += len(skc_grp)
            lz_logger.info('>>>>> {0}% ({1}) products have been processed.'
                           .format(round(skc_num / skc_num_sum * 100, 1), skc_num))

        it = pd.DataFrame(it, index=['it']).T.rename_axis(cols_grp)
        it.reset_index(inplace=True)

        return it

    def exec_targ_inv_opt(self, po, ms, i0, it0, it_bd, cdl, cid):
        """
        Executing target inventory optimization

        Parameters
        ----------
        po : pd.DataFrame
             Target products and organizations of calculating target inventory
             index : prod_id, color_id, size, org_id
             columns : mng_reg_id

        ms : pd.DataFrame
             Main sizes
             index : prod_id, color_id, org_id
             columns : size, sp_size

        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r, i0_sum

        it0 : pd.DataFrame
              Basic target inventory
              index : prod_id, color_id, size, org_id
              columns : it_base

        it_bd : pd.DataFrame
                Target inventory lower- and upper- bounds
                index : prod_id, color_id, size, org_id
                columns : it_lb, it_ub

        cdl : pd.DataFrame
              Unit cost of demand lossing
              index : prod_id, color_id, size, org_id
              columns : cdl_lb, cdl_ub

        cid : pd.DataFrame
              Unit cost of inventory difference
              index : prod_id, color_id, size, org_id
              columns : cid_a, cid_d, cid_rep

        Returns
        -------
        it : pd.DataFrame
             Target inventory
             index : prod_id, color_id, size, org_id
             columns : it
        """

        cols_grp = ['prod_id', 'color_id', 'size', 'org_id']

        org_lst = po.index.get_level_values('org_id').unique()
        store_num_sum = org_lst.shape[0]
        lz_logger.info('>>>>> Sum store number:{store_num_sum}'.format(store_num_sum=store_num_sum))

        # it_reg = list()
        # Creating multi-processing pool and task queue
        pool = Pool(cpu_count())
        it_reg = pool.map(partial(self.exec_unit, po, ms, i0, it0, it_bd, cdl, cid),
                          po['mng_reg_id'].drop_duplicates())
        pool.close()
        pool.join()
        # it_reg = list()
        # for mng_reg_id in po['mng_reg_id'].drop_duplicates():
        #     it_temp = self.exec_unit(po, ms, i0, it0, it_bd, cdl, cid, mng_reg_id)
        #     it_reg.append(it_temp)

        if len(it_reg) == 0:
            lz_logger.info('>>>>> Target inventory outputs NULL')
            it = pd.DataFrame(columns=cols_grp + ['it']).set_index(cols_grp)
            return it

        # Concatenating results
        it = pd.concat(it_reg).set_index(cols_grp)

        return it

    def adj_targ_inv(self, po, ms, i0, s, itp, it):
        """
        Adjusting target inventory

        Parameters
        ----------
        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        ms : pd.DataFrame
             Main sizes
             index : prod_id, color_id, org_id
             columns : size, sp_size

        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r, i0_sum

        s : pd.DataFrame
            Sales data
            index : prod_id, color_id, size, org_id, week_no
            columns : s

        itp : pd.DataFrame
              Target inventory of the previous day
              index : prod_id, color_id, size, org_id
              columns : itp

        it : pd.DataFrame
             Target inventory
             index : prod_id, color_id, size, org_id
             columns : it

        Returns
        -------
        it_a : pd.DataFrame
               Target inventory after adjusted
               index : prod_id, color_id, size, org_id
               columns : it
        """

        cols_grp = ['prod_id', 'color_id', 'size', 'org_id']
        cols_grp2 = ['prod_id', 'color_id', 'org_id']

        # Filling target inventory
        it_a = po.loc[po['is_store'] == 1, ['is_new', 'to_emp']].join(it)
        it_a = it_a.reset_index().fillna(0)

        # Setting target inventory of each skc/org to be 0 if that of the previous
        # day is 0
        if not itp.empty:
            it_a = it_a.set_index(cols_grp).join(itp).reset_index()
            it_a.loc[it_a['itp'] == 0, 'it'] = 0

        # Setting target inventory of new skc-org couples to be that of the
        # previous day
        if not itp.empty:
            idx_sel = it_a['is_new'] == 1
            it_a.loc[idx_sel, 'it'] = it_a.loc[idx_sel, 'itp']

        # Setting target inventory of skc/org with only one size having either
        # positive initial inventory or positive sales of the previous week to be 0
        s1 = s.reset_index('week_no')
        s1 = s1.loc[s1['week_no'] == -1, ['s']].copy()
        it_a = it_a.set_index(cols_grp).join([i0[['i0_sum']], s1])
        it_a.reset_index('size', inplace=True)
        it_a['i0_s'] = it_a[['i0_sum', 's']].sum(axis=1)
        it_a['sn'] = it_a['i0_s'].map(np.sign).sum(level=cols_grp2)
        it_a.reset_index(inplace=True)
        it_a.loc[(it_a['is_new'] == 0) & (it_a['sn'] <= 1), 'it'] = 0

        # Calculating the maximal target inventory of the main sizes
        # it_max = it_a[cols_grp + ['it']].set_index(cols_grp)
        # it_max = ms['size'].reset_index().set_index(cols_grp).join(it_max)
        # it_max = it_max['it'].reset_index(cols_grp2).groupby(cols_grp2).max()
        # it_max.rename(columns={'it': 'it_max'}, inplace=True)

        # For each skc/org, setting target inventory of each size not to be higher
        # than the maximal value of the main sizes
        # it_a = it_a.set_index(cols_grp2).join(it_max).reset_index()
        # it_a.fillna({'it_max': 0}, inplace=True)
        # it_a['it'] = it_a[['it', 'it_max']].min(axis=1)

        # For each skc/size/org, setting target inventory to be 1 at least if that
        # of the previous day is positive and emptying is not required
        if not itp.empty:
            it_a.loc[(it_a['itp'] > 0) & (it_a['it'] == 0), 'it'] = 1

        # Setting target inventory of skc/org to be 0 if emptying is required
        it_a.loc[it_a['to_emp'] == 1, 'it'] = 0

        it_a = it_a[cols_grp + ['it']].set_index(cols_grp).dropna()

        return it_a

    def cal_targ_inv(self, po, ms, i0, s, itp, io, sales_we, cdl, cid, w,
                     qss_base, cal_ti_mode, date_dec, exe_fest_adj):
        """
        Calculating target inventory

        Parameters
        ----------
        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        ms : pd.DataFrame
             Main sizes
             index : prod_id, color_id, org_id
             columns : size, sp_size

        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r, i0_sum

        s : pd.DataFrame
            Sales data
            index : prod_id, color_id, size, org_id, week_no
            columns : s

        itp : pd.DataFrame
              Target inventory of the previous day
              index : prod_id, color_id, size, org_id
              columns : itp

        io : pd.DataFrame
             Markers of existed moving -in/-out
             index : prod_id, color_id, size, org_id
             columns : has_in, has_out

        sales_we : pd.DataFrame
                   Sum sales weights of skc, size, and store
                   index : prod_id, color_id, size, org_id
                   columns : skc_we_a, skc_we_d, store_we_a, store_we_d, sks_we_a,
                             sks_we_d, sum_we_a, sum_we_d

        cdl : pd.DataFrame
              Unit cost of demand lossing
              index : prod_id, color_id, size, org_id
              columns : cdl_lb, cdl_ub

        cid : pd.DataFrame
              Unit cost of inventory difference
              index : prod_id, color_id, size, org_id
              columns : cid_a, cid_d, cid_rep

        w : dict
            Week sales weights
            key : w_1, w_2
            value : sales weight

        qss_base : float
                   Basic safety stock

        cal_ti_mode : str
                      Mode of calculating target inventory

        date_dec : date
                   Decision making date

        exe_fest_adj : bool
                       Whether or not executing demand adjustment by festival

        Returns
        -------
        d : pd.DataFrame
            Predicted demand lower- and upper- bounds
            index : prod_id, color_id, size, org_id
            columns : d_lb, d_ub

        qss : pd.DataFrame
              Safety stock
              index : prod_id, color_id, size, org_id
              columns : qss

        it : pd.DataFrame
             Target inventory
             index : prod_id, color_id, size, org_id
             columns : it
        """

        # Calculating demand
        d = self.cal_dem(po, ms, i0, s, w, date_dec, exe_fest_adj)

        # Calculating safety stock
        qss = self.cal_safety_stock(po, ms, i0, d, sales_we, qss_base)

        # Calculating basic target inventory
        it0 = self.cal_basic_it(po, i0, s, itp, cal_ti_mode)

        # Calculating target inventory lower- and upper- bounds
        it_bd = self.cal_it_bd(i0, s, io, d, qss)

        # Extracting target skc/org of calculating target inventory
        po_ti = self.extr_po_ti(po, i0, itp, it_bd)
        # Calculating target inventory of stores
        it = self.exec_targ_inv_opt(po_ti, ms, i0, it0, it_bd, cdl, cid)

        # Adjusting target inventory
        it = self.adj_targ_inv(po, ms, i0, s, itp, it)

        return d, qss, it
