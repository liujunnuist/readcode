# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

from check.TypeAssert import typeassert


class CrossExtract(object):
    def __init__(self):
        pass

    def extr_po_rep(self, po, i0, it):
        """
        Extracting target products and organizations for replenishment

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

        it : pd.DataFrame
             Target inventory
             index : prod_id, color_id, size, org_id
             columns : it, is_valid

        Returns
        -------
        po_rep : pd.DataFrame
                 Target products and organizations of replenishment
                 index : prod_id, color_id, size, org_id
                 columns : to_in, to_out
        """

        cols_grp = ['prod_id', 'color_id', 'size', 'org_id']

        # Merging data
        idx_rep = (po['is_store'] == 0) | ((po['is_store'] == 1) &
                                           (po['is_rep'] == 1))
        po_rep = po.loc[idx_rep].join([i0, it]).reset_index()
        po_rep.fillna({'i0': 0, 'i0_sum': 0, 'it': 0}, inplace=True)

        # Marking out skc/size/org to move -in/-out in replenishment
        po_rep['to_in'] = 0
        po_rep['to_out'] = 0
        # For each skc, orgs to move-in must
        # 1. be a store and
        # 2. have target inventory being higher than sum initial inventory on any
        #    size and
        # 3. not be remained and
        # 4. be valid
        po_rep.loc[(po_rep['is_store'] == 1) &
                   (po_rep['it'] > po_rep['i0_sum']) &
                   (po_rep['not_in'] == 0), 'to_in'] = 1
        # For each skc, orgs to move-out must
        # 1. be a warehouse and
        # 2. have positive initial inventory
        po_rep.loc[(po_rep['is_store'] == 0) &
                   (po_rep['i0'] > 0), 'to_out'] = 1

        # Summing moving -in/-out markers
        po_rep.set_index(cols_grp[:3], inplace=True)
        po_rep['to_in_sks'] = po_rep['to_in'].sum(level=cols_grp[:3])
        po_rep['to_out_sks'] = po_rep['to_out'].sum(level=cols_grp[:3])
        po_rep.reset_index(inplace=True)
        po_rep['to_mov_sks'] = po_rep[['to_in_sks', 'to_out_sks']].prod(axis=1)
        po_rep['to_mov'] = po_rep[['to_in', 'to_out']].sum(axis=1)

        # Selecting valid skc/size/org
        po_rep = po_rep.loc[(po_rep['to_mov_sks'] > 0) & (po_rep['to_mov'] > 0),
                            cols_grp + ['to_in', 'to_out']].copy()
        po_rep = po_rep.set_index(cols_grp).sort_index()

        return po_rep

    def extr_po_trans(self, po, i0, io, it):
        """
        Extracting target products and organizations for transferring

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

        io : pd.DataFrame
             Markers of existed moving -in/-out
             index : prod_id, color_id, size, org_id
             columns : has_in, has_out

        it : pd.DataFrame
             Target inventory
             index : prod_id, color_id, size, org_id
             columns : it

        Returns
        -------
        po_trans : pd.DataFrame
                   Target products and organizations of transferring
                   index : prod_id, color_id, size, org_id
                   columns : season_id, mng_reg_id, is_store, is_new, to_in,
                             to_out, to_emp
        """

        cols_grp = ['season_id', 'mng_reg_id', 'is_store', 'is_new']
        cols_grp2 = ['prod_id', 'color_id', 'org_id']
        cols_grp3 = ['prod_id', 'color_id', 'size', 'mng_reg_id']
        cols_grp4 = ['prod_id', 'color_id', 'mng_reg_id']
        cols_grp5 = ['prod_id', 'color_id', 'size', 'org_id']
        cols_grp6 = ['to_in', 'to_out', 'to_emp']

        # Merging data
        idx_trans = (po['is_store'] == -1) | ((po['is_store'] == 1) &
                                              ((po['is_trans'] == 1) |
                                               (po['is_ret'] == 1)))
        po_trans = po.loc[idx_trans].join([i0, it, io]).reset_index()
        po_trans.fillna({'i0': 0, 'i0_sum': 0, 'it': 0, 'has_in': 0, 'has_out': 0},
                        inplace=True)

        # Marking out skc/size/org to move -in/-out in transferring
        po_trans['to_in'] = 0
        po_trans['to_out'] = 0
        # For each skc/size, orgs to move-in must
        # 1. be a return warehouse or
        # 2. be a store and
        # 3. have positive target inventory and
        # 4. have target inventory being higher than initial inventory plus a
        # buffer and
        # 5. have not been moved out and
        # 6. not be remained and
        # 7. not be emptied and
        # 8. be valid
        po_trans.loc[(po_trans['is_store'] == -1) |
                     ((po_trans['is_store'] == 1) &
                      (po_trans['it'] > 0) &
                      (po_trans['it'] > po_trans['i0_sum']) &
                      (po_trans['has_out'] == 0) &
                      (po_trans['not_in'] == 0) &
                      (po_trans['to_emp'] == 0)), 'to_in'] = 1
        # For each skc, orgs to move-out must
        # 1. be a store and
        # 2. have positive initial inventory and
        # 3. have not been moved in and
        # 4. not be remained and
        # 5. be valid
        po_trans.loc[(po_trans['is_store'] == 1) &
                     (po_trans['i0'] > 0) &
                     (po_trans['has_in'] == 0) &
                     (po_trans['not_out'] == 0), 'to_out'] = 1

        # Summing moving -in/-out markers
        # Marking out moving skc/size/reg
        po_trans.set_index(cols_grp3, inplace=True)
        po_trans['to_in_sksr'] = po_trans['to_in'].sum(level=cols_grp3)
        po_trans['to_out_sksr'] = po_trans['to_out'].sum(level=cols_grp3)
        po_trans.reset_index(inplace=True)
        po_trans['to_mov_sksr'] = po_trans[['to_in_sksr',
                                            'to_out_sksr']].prod(axis=1)
        # Marking out moving skc/reg
        po_trans.set_index(cols_grp4, inplace=True)
        po_trans['to_mov_skcr'] = po_trans['to_mov_sksr'].sum(level=cols_grp4)
        po_trans.reset_index(inplace=True)
        # Marking out moving skc/org
        po_trans.set_index(cols_grp2, inplace=True)
        po_trans['to_mov'] = po_trans[['to_in', 'to_out']].sum(axis=1)
        po_trans['to_mov_skcs'] = po_trans['to_mov'].sum(level=cols_grp2)
        po_trans.reset_index(inplace=True)

        # Selecting valid skc/org
        po_trans = po_trans.loc[(po_trans['to_mov_skcr'] > 0) &
                                (po_trans['to_mov_skcs'] > 0),
                                cols_grp5 + cols_grp + cols_grp6].copy()
        po_trans = po_trans.set_index(cols_grp5).sort_index()

        return po_trans

    def cal_inv_prop_size(self, po, i0, s):
        """
        Calculating inventory proportion of each size

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

        Returns
        -------
        ip_size : pd.DataFrame
                  Inventory proportion of each size in management region
                  index : prod_id, color_id, size
                  columns : ip_size
        """

        cols_grp = ['prod_id', 'color_id', 'size']

        # Calculating sum initial inventory of each skc/size/store
        i0_sum = i0.reset_index(cols_grp).groupby(cols_grp).sum()

        # Calculating sum sales of each skc/size/store
        s_sum = s.reset_index(cols_grp).groupby(cols_grp).sum()

        # Merging all skc/store, calculating sum sales of each skc/size, and
        # transforming negative sales
        ip_size = pd.DataFrame(index=po[po['is_store'] == 1].reset_index('org_id')
                               .index.drop_duplicates())
        ip_size = ip_size.join([i0_sum, s_sum]).reset_index()
        ip_size['is'] = ip_size[['i0_sum', 's']].fillna(0).sum(axis=1)
        ip_size.loc[ip_size['is'] < 0, 'is'] = 0
        ip_size['is'] += 1

        # Calculating sales proportion of each size in skc
        ip_size = ip_size.set_index(cols_grp[:2])
        ip_size['is_sum'] = ip_size['is'].sum(level=cols_grp[:2])
        ip_size.reset_index(inplace=True)
        ip_size['ip_size'] = ip_size['is'] / ip_size['is_sum']

        ip_size = ip_size[cols_grp + ['ip_size']].set_index(cols_grp)

        return ip_size

    def cal_sales_prop_size_reg(self, po, s):
        """
        Calculating sales proportion of each size in management region

        Parameters
        ----------
        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        s : pd.DataFrame
            Sales data
            index : prod_id, color_id, size, org_id, week_no
            columns : s

        Returns
        -------
        sp_size : pd.DataFrame
                  Sales proportion of each size in management region
                  index : prod_id, color_id, mng_reg_id, size
                  columns : sp_size
        """

        cols_grp = ['prod_id', 'color_id', 'size', 'org_id']
        cols_grp2 = ['prod_id', 'color_id', 'mng_reg_id', 'size']

        # Calculating sum sales of each skc/size/store
        s_sum = s.reset_index(cols_grp).groupby(cols_grp).sum()

        # Merging all skc/store, calculating sum sales of each skc/reg/size, and
        # transforming negative sales
        sp_size = po.loc[po['is_store'] == 1, [cols_grp2[2]]].copy()
        sp_size = sp_size.join(s_sum).reset_index().fillna(0)
        sp_size = sp_size[cols_grp2 + ['s']].groupby(cols_grp2).sum()
        sp_size.reset_index(inplace=True)
        sp_size.loc[sp_size['s'] < 0, 's'] = 0
        sp_size['s'] += 1

        # Calculating sales proportion of each size in skc/reg
        sp_size = sp_size.set_index(cols_grp2[:3])
        sp_size['s_sum'] = sp_size['s'].sum(level=cols_grp2[:3])
        sp_size.reset_index(inplace=True)
        sp_size['sp_size'] = sp_size['s'] / sp_size['s_sum']

        sp_size = sp_size[cols_grp2 + ['sp_size']].set_index(cols_grp2)

        return sp_size

    def mark_main_size(self, po, ip_size, sp_size, len_main_sizes):
        """
        Marking the main sizes of each skc/store

        Parameters
        ----------
        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        ip_size : pd.DataFrame
                  Inventory proportion of each size in management region
                  index : prod_id, color_id, size
                  columns : sp_size

        sp_size : pd.DataFrame
                  Sales proportion of each size in management region
                  index : prod_id, color_id, mng_reg_id, size
                  columns : sp_size

        len_main_sizes : int
                         The maximal length of the main size group

        Returns
        -------
        ms : pd.DataFrame
             Main sizes
             index : prod_id, color_id, org_id
             columns : size, sp_size
        """

        cols_grp = ['size_order', 'mng_reg_id']
        cols_grp2 = ['prod_id', 'color_id', 'size']
        cols_grp3 = ['prod_id', 'color_id', 'mng_reg_id']
        cols_grp4 = ['prod_id', 'color_id', 'org_id']

        # Merging and summing inventory proportion and sales proportion
        ms = po.loc[po['is_store'] == 1, cols_grp].reset_index()
        ms = ms.drop('org_id', axis=1).drop_duplicates()
        ms = ms.set_index(cols_grp2).join(ip_size).reset_index()
        ms = ms.set_index(cols_grp3 + ['size']).join(sp_size).reset_index()
        ms.fillna({'ip_size': 0, 'sp_size': 0}, inplace=True)

        ms.sort_values(cols_grp3 + ['size_order'], inplace=True)
        ms['pos_we'] = 0
        ms.loc[(ms['size_order'] >= 2) & (ms['size_order'] <= 5), 'pos_we'] = 1
        ms['size_we'] = ms['sp_size'] + ms['ip_size'] / 10 + ms['pos_we'] / 100

        # Calculating sum weight of each size subgroup
        ms['size_we'] = ms[cols_grp3 + ['size_we']].groupby(cols_grp3) \
            .apply(lambda df: df.rolling(window=len_main_sizes,
                                         min_periods=1).sum())['size_we']

        # Selecting the size subgroup with the highest sun weight
        def _find_max(df):
            idx = df['size_we'].idxmax()
            ms = df.loc[idx - len_main_sizes + 1:idx, ['size', 'sp_size']].copy()
            ms.set_index('size', inplace=True)
            return ms

        ms = ms[cols_grp3 + ['size', 'size_we', 'sp_size']].reset_index(drop=True)
        ms = ms.groupby(cols_grp3).apply(_find_max).reset_index('size')
        ms_f = po.loc[po['is_store'] == 1, cols_grp[-1]].reset_index()
        ms_f = ms_f.drop('size', axis=1).drop_duplicates()
        ms = ms_f.set_index(cols_grp3).join(ms).reset_index()
        ms = ms.drop('mng_reg_id', axis=1).set_index(cols_grp4).sort_index()

        return ms

    def cal_len_fs(self, ms, qsf_base):
        """
        Calculating the minimal continue-size length in a fullsize group of each
        skc/store

        Parameters
        ----------
        ms : pd.DataFrame
             Main sizes
             index : prod_id, color_id, org_id
             columns : size, sp_size

        qsf_base : int
                   Basic continue-size length in a fullsize group

        Returns
        -------
        qsf : pd.DataFrame
              The minimal continue-size length in a fullsize group
              index : prod_id, color_id, org_id
              columns : qsf
        """

        cols_grp = ['prod_id', 'color_id', 'org_id']

        qsf = ms['size'].reset_index().groupby(cols_grp).count()
        qsf.columns = ['qsf']
        qsf.loc[qsf['qsf'] > qsf_base, 'qsf'] = qsf_base

        return qsf

    def mark_fullsize_init(self, ms, i0, qsf):
        """
        Calculating number of fullsize groups of each store/skc

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

        qsf : pd.DataFrame
              The minimal continue-size length in a fullsize group
              index : prod_id, color_id, org_id
              columns : qsf

        Returns
        -------
        qfs0 : pd.DataFrame
               Initial fullsize group number of each skc/store
               index : prod_id, color_id, org_id
               columns : ns, qfs, fsb
        """

        cols_grp = ['prod_id', 'color_id', 'size', 'org_id']
        cols_grp2 = ['prod_id', 'color_id', 'org_id']

        # Marking out positive sum initial inventory
        i0_p = i0['i0_sum'].map(lambda x: 1 if x > 0 else 0)
        i0_p.name = 'is_i0p'

        # Calculating size number of positive sum initial inventory
        ns = i0_p.reset_index(cols_grp2).groupby(cols_grp2).sum()
        ns.rename(columns={'is_i0p': 'ns'}, inplace=True)

        # Calculating size number of positive inventory in each size-group
        i0_s = ms[['size']].join(qsf).reset_index().set_index(cols_grp).join(i0_p)
        i0_s = i0_s.reset_index().fillna(0)
        i0_s['size_num'] = \
            i0_s[cols_grp2 + ['qsf', 'is_i0p']].groupby(cols_grp2) \
                .apply(lambda df: df.rolling(window=df['qsf'].values[0],
                                             min_periods=1).sum())['is_i0p']

        # Marking out fullsize groups
        i0_s['qfs'] = 0
        i0_s.loc[i0_s['size_num'] == i0_s['qsf'], 'qfs'] = 1
        # Calculating number of fullsize groups
        qfs0 = i0_s[cols_grp2 + ['qfs']].groupby(cols_grp2).sum().join(ns)
        # Marking out whether or not an org is fullsize for each skc
        qfs0['fsb'] = qfs0['qfs'].map(np.sign)
        # Setting as fullsize if initial inventory of skc is 0
        qfs0.loc[qfs0['ns'] == 0, 'qfs'] = 1
        qfs0.loc[qfs0['ns'] == 0, 'fsb'] = 1

        return qfs0

    def extr_main_size(self, po, i0, s, len_main_sizes, qsf_base):
        """
        Extracting the main sizes

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

        len_main_sizes : int
                         The maximal length of the main size group

        qsf_base : int
                   Basic continue-size length in a fullsize group

        Returns
        -------
        ms : pd.DataFrame
             Main sizes
             index : prod_id, color_id, org_id
             columns : size, sp_size

        qsf : pd.DataFrame
              The minimal continue-size length in a fullsize group
              index : prod_id, color_id, org_id
              columns : qsf

        qfs0 : pd.DataFrame
               Initial fullsize group number of each skc/store
               index : prod_id, color_id, org_id
               columns : ns, qfs, fsb
        """

        # Calculating inventory proportion of each size
        ip_size = self.cal_inv_prop_size(po, i0, s)

        # Calculating sales proportion of each size in management region
        sp_size = self.cal_sales_prop_size_reg(po, s)

        # Marking the main sizes of each skc/store
        ms = self.mark_main_size(po, ip_size, sp_size, len_main_sizes)

        # Calculating the minimal length of a fullsize group
        qsf = self.cal_len_fs(ms, qsf_base)

        # Calculating number of fullsize groups of each store/skc
        # qfs0 = mark_fullsize_init(ms, i0, qsf)

        return ms, qsf

    def cal_sales_prop_skc_in_reg(self, po, i0, s):
        """
        Calculating sales proportion of each skc in management region

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

        Returns
        -------
        sp_skc_reg : pd.DataFrame
                     Sales proportion of each skc in region
                     index : prod_id, color_id, mng_reg_id
                     columns : sp_skc
        """

        cols_grp = ['prod_id', 'color_id', 'org_id']
        cols_grp2 = ['season_id', 'class_0', 'mng_reg_id']
        cols_grp3 = ['prod_id', 'color_id', 'mng_reg_id']

        # Calculating sum sales of each skc/store
        s_sum = s.reset_index(cols_grp).groupby(cols_grp).sum()

        # Calculating sum initial inventory of each skc/store
        i0_sum = i0['i0'].reset_index(cols_grp).groupby(cols_grp).sum()

        # Merging all skc/store, calculating mean sales of each skc/class/reg, and
        # transforming negative sales
        sp_skc_reg = po.loc[po['is_store'] == 1, cols_grp2].reset_index()
        sp_skc_reg = sp_skc_reg.drop('size', axis=1).drop_duplicates()
        sp_skc_reg = sp_skc_reg.set_index(cols_grp).join([s_sum, i0_sum])
        sp_skc_reg = sp_skc_reg.reset_index().fillna({'s': 0, 'i0': 0})
        sp_skc_reg['n'] = sp_skc_reg[['i0', 's']].max(axis=1).map(np.sign)
        sp_skc_reg = sp_skc_reg[cols_grp[:2] + cols_grp2 + ['s', 'n']] \
            .groupby(cols_grp[:2] + cols_grp2).sum().reset_index()
        sp_skc_reg.loc[sp_skc_reg['n'] == 0, 'n'] = 1
        sp_skc_reg['s'] /= sp_skc_reg['n']
        sp_skc_reg.loc[sp_skc_reg['s'] < 0, 's'] = 0
        sp_skc_reg['s'] += 1

        # Calculating sales proportion of each skc in class/reg
        sp_skc_reg.set_index(cols_grp2, inplace=True)
        sp_skc_reg['s_sum'] = sp_skc_reg['s'].sum(level=cols_grp2)
        sp_skc_reg.reset_index(inplace=True)
        sp_skc_reg['sp_skc'] = sp_skc_reg['s'] / sp_skc_reg['s_sum']

        sp_skc_reg = sp_skc_reg[cols_grp3 + ['sp_skc']].set_index(cols_grp3)

        return sp_skc_reg

    def cal_sales_prop_store_in_skc(self, po, s):
        """
        Calculating sales proportion of each store in skc

        Parameters
        ----------
        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        s : pd.DataFrame
            Sales data
            index : prod_id, color_id, size, org_id, week_no
            columns : s

        Returns
        -------
        sp_store_skc : pd.DataFrame
                       Sales proportion of each store in skc
                       index : prod_id, color_id, org_id
                       columns : sp_store
        """

        cols_grp = ['prod_id', 'color_id', 'org_id']
        cols_grp2 = ['prod_id', 'color_id', 'mng_reg_id']

        # Calculating sum sales of each skc/store
        s_sum = s.reset_index(cols_grp).groupby(cols_grp).sum()

        # Merging all skc/store and transforming negative sales
        sp_store_skc = po.loc[po['is_store'] == 1, cols_grp2[2]].reset_index()
        sp_store_skc = sp_store_skc.drop('size', axis=1).drop_duplicates()
        sp_store_skc = sp_store_skc.set_index(cols_grp).join(s_sum).reset_index()
        sp_store_skc.fillna({'s': 0}, inplace=True)
        sp_store_skc.loc[sp_store_skc['s'] < 0, 's'] = 0
        sp_store_skc['s'] += 1

        # Calculating sales proportion of each store in skc/reg
        sp_store_skc.set_index(cols_grp2, inplace=True)
        sp_store_skc['s_sum'] = sp_store_skc['s'].sum(level=cols_grp2)
        sp_store_skc.reset_index(inplace=True)
        sp_store_skc['sp_store'] = sp_store_skc['s'] / sp_store_skc['s_sum']

        sp_store_skc = sp_store_skc[cols_grp + ['sp_store']].set_index(cols_grp)

        return sp_store_skc

    def cal_sales_prop_store_in_reg(self, po, i0, s):
        """
        Calculating sales proportion of each store in management region

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

        Returns
        -------
        sp_store_reg : pd.DataFrame
                       Sales proportion of each store in region
                       index : season_id, class_0, org_id
                       columns : sp_store
        """

        cols_grp = ['prod_id', 'color_id', 'org_id']
        cols_grp2 = ['season_id', 'class_0', 'mng_reg_id']
        cols_grp3 = ['season_id', 'class_0', 'org_id']

        # Calculating sum sales of each skc/store
        s_sum = s.reset_index(cols_grp).groupby(cols_grp).sum()

        # Calculating sum initial inventory of each skc/store
        i0_sum = i0['i0'].reset_index(cols_grp).groupby(cols_grp).sum()

        # Calculating mean sales of each class/reg/store and transforming negative
        # sales
        sp_store_reg = po.loc[po['is_store'] == 1, cols_grp2].reset_index()
        sp_store_reg = sp_store_reg.drop('size', axis=1).drop_duplicates()
        sp_store_reg = sp_store_reg.set_index(cols_grp).join([s_sum, i0_sum])
        sp_store_reg = sp_store_reg.reset_index().fillna({'s': 0, 'i0': 0})
        sp_store_reg['n'] = sp_store_reg[['i0', 's']].max(axis=1).map(np.sign)
        sp_store_reg = sp_store_reg[cols_grp2 + [cols_grp[2]] + ['s', 'n']] \
            .groupby(cols_grp2 + [cols_grp[2]]).sum().reset_index()
        sp_store_reg.loc[sp_store_reg['n'] == 0, 'n'] = 1
        sp_store_reg['s'] /= sp_store_reg['n']
        sp_store_reg.loc[sp_store_reg['s'] < 0, 's'] = 0
        sp_store_reg['s'] += 1

        # Calculating sales proportion of each store in class/reg
        sp_store_reg.set_index(cols_grp2, inplace=True)
        sp_store_reg['s_sum'] = sp_store_reg['s'].sum(level=cols_grp2)
        sp_store_reg.reset_index(inplace=True)
        sp_store_reg['sp_store'] = sp_store_reg['s'] / sp_store_reg['s_sum']

        sp_store_reg = sp_store_reg[cols_grp3 + ['sp_store']].set_index(cols_grp3)

        return sp_store_reg

    def cal_sales_prop_skc_in_store(self, po, s):
        """
        Calculating sales proportion of each skc in store

        Parameters
        ----------
        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        s : pd.DataFrame
            Sales data
            index : prod_id, color_id, size, org_id, week_no
            columns : s

        Returns
        -------
        sp_skc_store : pd.DataFrame
                       Sales proportion of each skc in store
                       index : prod_id, color_id, org_id
                       columns : sp_skc
        """

        cols_grp = ['prod_id', 'color_id', 'org_id']
        cols_grp2 = ['season_id', 'class_0', 'org_id']

        # Calculating sum sales of each skc/store
        s_sum = s.reset_index(cols_grp).groupby(cols_grp).sum()

        # Merging all skc/store and transforming negative sales
        sp_skc_store = po.loc[po['is_store'] == 1, cols_grp2[:2]].reset_index()
        sp_skc_store = sp_skc_store.drop('size', axis=1).drop_duplicates()
        sp_skc_store = sp_skc_store.set_index(cols_grp).join(s_sum).reset_index()
        sp_skc_store.fillna({'s': 0}, inplace=True)
        sp_skc_store.loc[sp_skc_store['s'] < 0, 's'] = 0
        sp_skc_store['s'] += 1

        # Calculating sales proportion of each skc in class/store
        sp_skc_store.set_index(cols_grp2, inplace=True)
        sp_skc_store['s_sum'] = sp_skc_store['s'].sum(level=cols_grp2)
        sp_skc_store.reset_index(inplace=True)
        sp_skc_store['sp_skc'] = sp_skc_store['s'] / sp_skc_store['s_sum']

        sp_skc_store = sp_skc_store[cols_grp + ['sp_skc']].set_index(cols_grp)

        return sp_skc_store

    def cal_sales_prop_skc_all(self, po, i0, s):
        """
        Calculating sales proportion of each skc in all stores

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

        Returns
        -------
        sp_skc : pd.DataFrame
                 Sales proportion of each skc
                 index : prod_id, color_id
                 columns : sp_skc
        """

        cols_grp = ['prod_id', 'color_id', 'org_id']
        cols_grp2 = ['season_id', 'class_0']

        # Calculating sum sales of each skc/store
        s_sum = s.reset_index(cols_grp).groupby(cols_grp).sum()

        # Calculating sum initial inventory of each skc/store
        i0_sum = i0['i0'].reset_index(cols_grp).groupby(cols_grp).sum()

        # Merging all skc/store, calculating mean sales of each skc/class, and
        # transforming negative sales
        sp_skc = po.loc[po['is_store'] == 1, cols_grp2].reset_index()
        sp_skc = sp_skc.drop('size', axis=1).drop_duplicates()
        sp_skc = sp_skc.set_index(cols_grp).join([s_sum, i0_sum]).reset_index()
        sp_skc.fillna({'s': 0, 'i0': 0}, inplace=True)
        sp_skc['n'] = sp_skc[['i0', 's']].max(axis=1).map(np.sign)
        sp_skc = sp_skc[cols_grp[:2] + cols_grp2 + ['s', 'n']] \
            .groupby(cols_grp[:2] + cols_grp2).sum().reset_index()
        sp_skc.loc[sp_skc['n'] == 0, 'n'] = 1
        sp_skc['s'] /= sp_skc['n']
        sp_skc.loc[sp_skc['s'] < 0, 's'] = 0
        sp_skc['s'] += 1

        # Calculating sales proportion of each skc in a class
        sp_skc.set_index(cols_grp2, inplace=True)
        sp_skc['s_sum'] = sp_skc['s'].sum(level=cols_grp2)
        sp_skc.reset_index(inplace=True)
        sp_skc['sp_skc'] = sp_skc['s'] / sp_skc['s_sum']

        sp_skc = sp_skc[cols_grp[:2] + ['sp_skc']].set_index(cols_grp[:2])

        return sp_skc

    def cal_sales_prop_store_in_skc_all(self, po, s):
        """
        Calculating sales proportion of each store in a skc and all stores

        Parameters
        ----------
        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        s : pd.DataFrame
            Sales data
            index : prod_id, color_id, size, org_id, week_no
            columns : s

        Returns
        -------
        sp_store_skc : pd.DataFrame
                       Sales proportion of each store in a skc
                       index : prod_id, color_id, org_id
                       columns : sp_store
        """

        cols_grp = ['prod_id', 'color_id', 'org_id']
        cols_grp2 = ['prod_id', 'color_id']

        # Calculating sum sales of each skc/store
        s_sum = s.reset_index(cols_grp).groupby(cols_grp).sum()

        # Merging all skc/store and transforming negative sales
        sp_store_skc = pd.DataFrame(
            index=po[po['is_store'] == 1].index.drop_duplicates()).reset_index()
        sp_store_skc = sp_store_skc.drop('size', axis=1).drop_duplicates()
        sp_store_skc = sp_store_skc.set_index(cols_grp).join(s_sum).reset_index()
        sp_store_skc.fillna({'s': 0}, inplace=True)
        sp_store_skc.loc[sp_store_skc['s'] < 0, 's'] = 0
        sp_store_skc['s'] += 1

        # Calculating sales proportion of each store in a skc
        sp_store_skc.set_index(cols_grp2, inplace=True)
        sp_store_skc['s_sum'] = sp_store_skc['s'].sum(level=cols_grp2)
        sp_store_skc.reset_index(inplace=True)
        sp_store_skc['sp_store'] = sp_store_skc['s'] / sp_store_skc['s_sum']

        sp_store_skc = sp_store_skc[cols_grp + ['sp_store']].set_index(cols_grp)

        return sp_store_skc

    def cal_sales_prop_size(self, po, s):
        """
        Calculating sales proportion of each size

        Parameters
        ----------
        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        s : pd.DataFrame
            Sales data
            index : prod_id, color_id, size, org_id, week_no
            columns : s

        Returns
        -------
        sp_size : pd.DataFrame
                  Sales proportion of each size
                  index : prod_id, color_id, org_id, size
                  columns : sp_size
        """

        cols_grp = ['prod_id', 'color_id', 'size', 'org_id']
        cols_grp2 = ['prod_id', 'color_id', 'org_id', 'size']

        # Calculating sum sales of each sks/store
        s_sum = s.reset_index(cols_grp).groupby(cols_grp).sum()

        # Merging all skc/store and transforming negative sales
        sp_size = po.loc[po['is_store'] == 1].join(s_sum).reset_index().fillna(0)
        sp_size.loc[sp_size['s'] < 0, 's'] = 0
        sp_size['s'] += 1

        # Calculating sales proportion of each size in skc/store
        sp_size = sp_size.set_index(cols_grp2[:3]).sort_index()
        sp_size['s_sum'] = sp_size['s'].sum(level=cols_grp2[:3])
        sp_size.reset_index(inplace=True)
        sp_size['sp_size'] = sp_size['s'] / sp_size['s_sum']

        sp_size = sp_size[cols_grp2 + ['sp_size']].set_index(cols_grp2)

        return sp_size

    def cal_sum_sales_we(self, po, sp_skc, sp_store, sp_size, lst_cols, cols_sp):
        """
        Calculating sum sales weights

        Parameters
        ----------
        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        sp_skc : pd.DataFrame
                 Sales proportion of each skc
                 index : prod_id, color_id, mng_reg_id/org_id
                 columns : sp_skc

        sp_store : pd.DataFrame
                   Sales proportion of each store
                   index : season_id/prod_id, class_0/color_id, org_id
                   columns : sp_store

        sp_size : pd.DataFrame
                  Sales proportion of each size
                  index : prod_id, color_id, org_id, size
                  columns : sp_size

        lst_cols : list
                   List of lists of column names

        cols_sp : list
                  List of column names of sales proportions

        Returns
        -------
        sales_we : pd.DataFrame
                   Sum sales weights of skc, size, and store based on store
                   index : prod_id, color_id, size, org_id
                   columns : skc_we_a, skc_we_d, store_we_a, store_we_d,
                             sks_we_a, sks_we_d, sum_we_a, sum_we_d
        """

        def _normalize(sw, cols_grp, col_targ):
            col_norm = col_targ[3:] + '_we'

            sw = sw.set_index(cols_grp)
            sw['min'] = sw[col_targ].min(level=cols_grp)
            sw['max'] = sw[col_targ].max(level=cols_grp)
            sw.reset_index(inplace=True)

            sw['range'] = sw['max'] - sw['min'] + 1.0E-10
            sw['diff_a'] = sw[col_targ] - sw['min']
            sw['diff_d'] = sw['max'] - sw[col_targ]
            sw[col_norm + '_a'] = sw['diff_a'] / sw['range']
            sw[col_norm + '_a'] = sw[col_norm + '_a'].map(lambda x: np.exp(x - 1))
            sw[col_norm + '_d'] = sw['diff_d'] / sw['range']
            sw[col_norm + '_d'] = sw[col_norm + '_d'].map(lambda x: np.exp(x - 1))

            sw.drop(['min', 'max', 'range', 'diff_a', 'diff_d'], axis=1,
                    inplace=True)

            return sw

        cols_grp = ['prod_id', 'color_id', 'org_id', 'size']
        cols_grp2 = ['prod_id', 'color_id', 'size', 'org_id']
        cols_grp3 = ['skc_we_a', 'skc_we_d', 'store_we_a', 'store_we_d',
                     'sks_we_a', 'sks_we_d', 'sum_we_a', 'sum_we_d']

        # Merging sales proportion
        sales_we = po.loc[po['is_store'] == 1, lst_cols[0]].reset_index()
        sales_we = sales_we.set_index(lst_cols[1]).join(sp_skc).reset_index()
        sales_we.fillna(0, inplace=True)
        sales_we = sales_we.set_index(lst_cols[2]).join(sp_store).reset_index()
        sales_we.fillna(0, inplace=True)
        sales_we = sales_we.set_index(cols_grp).join(sp_size).reset_index()
        sales_we.fillna(0, inplace=True)

        # Normalizing with ascending and descending trends respectively
        # 1. Normalizing sales proportion of each skc/org
        sales_we = _normalize(sales_we, lst_cols[0], cols_sp[0])
        # 2. Normalizing sales proportion of each org/skc
        sales_we = _normalize(sales_we, lst_cols[3], cols_sp[1])
        # 3. Normalizing sales proportion of each size in skc/org
        sales_we = _normalize(sales_we, cols_grp[:3], 'sp_size')

        # Constructing composite weights
        sales_we['sks_we_a'] = sales_we[['skc_we_a', 'size_we_a']].prod(axis=1)
        sales_we['sks_we_a'] = sales_we['sks_we_a'].pow(1 / 2)
        sales_we['sks_we_d'] = sales_we[['skc_we_d', 'size_we_d']].prod(axis=1)
        sales_we['sks_we_d'] = sales_we['sks_we_d'].pow(1 / 2)

        sales_we['sum_we_a'] = sales_we[['sks_we_a', 'store_we_a','store_we_a']].prod(axis=1).pow(1 / 3)
        # sales_we['sum_we_a'] = sales_we['sum_we_a'].pow(1 / 3)
        sales_we['sum_we_d'] = sales_we[['sks_we_d', 'store_we_d','store_we_d']].prod(axis=1).pow(1 / 3)
        # sales_we['sum_we_d'] = sales_we['sum_we_d'].pow(1 / 3)

        sales_we = sales_we[cols_grp2 + cols_grp3].set_index(cols_grp2)
        sales_we.sort_index(inplace=True)

        return sales_we

    def cal_sales_we(self, po, i0, s):
        """
        Calculating sales weights

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

        Returns
        -------
        sales_we_p : pd.DataFrame
                     Sum sales weights of skc, size, and store based on skc
                     index : prod_id, color_id, size, org_id
                     columns : skc_we_a, skc_we_d, store_we_a, store_we_d,
                               sks_we_a, sks_we_d, sum_we_a, sum_we_d

        sales_we_s : pd.DataFrame
                     Sum sales weights of skc, size, and store based on stores
                     index : prod_id, color_id, size, org_id
                     columns : skc_we_a, skc_we_d, store_we_a, store_we_d,
                               sks_we_a, sks_we_d, sum_we_a, sum_we_d

        sales_we_all : pd.DataFrame
                       Sum sales weights of skc, size, and store based on skc in
                       all stores
                       index : prod_id, color_id, size, org_id
                       columns : skc_we_a, skc_we_d, store_we_a, store_we_d,
                                 sks_we_a, sks_we_d, sum_we_a, sum_we_d
        """

        # Calculating sales proportion of each skc in management region
        sp_skc_reg = self.cal_sales_prop_skc_in_reg(po, i0, s)

        # Calculating sales proportion of each store in skc/management region
        sp_store_skc = self.cal_sales_prop_store_in_skc(po, s)

        # Calculating sales proportion of each store in management region
        sp_store_reg = self.cal_sales_prop_store_in_reg(po, i0, s)

        # Calculating sales proportion of each skc in store
        sp_skc_store = self.cal_sales_prop_skc_in_store(po, s)

        # Calculating sales proportion of each skc in all stores
        sp_skc_all = self.cal_sales_prop_skc_all(po, i0, s)

        # Calculating sales proportion of each store in a skc and all stores
        sp_store_skc_all = self.cal_sales_prop_store_in_skc_all(po, s)

        # Calculating sales proportion of each size
        sp_size = self.cal_sales_prop_size(po, s)

        # Calculating sum sales weights based on skc in each management region
        sales_we_p = self.cal_sum_sales_we(po, sp_skc_reg, sp_store_skc, sp_size,
                                           [['season_id', 'class_0', 'mng_reg_id'],
                                            ['prod_id', 'color_id', 'mng_reg_id'],
                                            ['prod_id', 'color_id', 'org_id'],
                                            ['prod_id', 'color_id', 'mng_reg_id']],
                                           ['sp_skc', 'sp_store'])

        # Calculating sum sales weights based on stores in each management region
        sales_we_s = self.cal_sum_sales_we(po, sp_skc_store, sp_store_reg, sp_size,
                                           [['season_id', 'class_0', 'mng_reg_id'],
                                            ['prod_id', 'color_id', 'org_id'],
                                            ['season_id', 'class_0', 'org_id'],
                                            ['season_id', 'class_0', 'org_id']],
                                           ['sp_store', 'sp_skc'])

        # Calculating sum sales weights based on skc in all stores
        sales_we_all = self.cal_sum_sales_we(po, sp_skc_all, sp_store_skc_all, sp_size,
                                             [['season_id', 'class_0'],
                                              ['prod_id', 'color_id'],
                                              ['prod_id', 'color_id', 'org_id'],
                                              ['prod_id', 'color_id']],
                                             ['sp_skc', 'sp_store'])

        return sales_we_p, sales_we_s, sales_we_all

    def cal_sr_we(self, oi, sales_we_p, sales_we_s):
        """
        Calculating sending-receiving weights between organizations

        Parameters
        ----------
        oi : pd.DataFrame
             Organization basic information
             index : org_id
             columns : prov_id, city_id, dist_id, mng_reg_id, is_store(1)

        sales_we_p : pd.DataFrame
                     Sum sales weights of skc, size, and store based on skc
                     index : prod_id, color_id, size, org_id
                     columns : skc_we_a, skc_we_d, store_we_a, store_we_d,
                               sks_we_a, sks_we_d, sum_we_a, sum_we_d

        sales_we_s : pd.DataFrame
                     Sum sales weights of skc, size, and store based on stores
                     index : prod_id, color_id, size, org_id
                     columns : skc_we_a, skc_we_d, store_we_a, store_we_d,
                               sks_we_a, sks_we_d, sum_we_a, sum_we_d

        Returns
        -------
        sr_we : pd.DataFrame
                Sending-receiving weights between orgs
                index : org_send_id, org_rec_id
                columns : sr_we_p, sr_we_s
        """

        def _gen_mov_cp(oi):
            # Generating moving organization couples
            oi_s = oi.loc[oi['is_store'] != -1, ['mng_reg_id', 'is_store']].copy()
            oi_s.reset_index(inplace=True)
            oi_s.rename(columns={'org_id': 'org_send_id',
                                 'mng_reg_id': 'reg_send_id',
                                 'is_store': 'is_store_s'}, inplace=True)
            oi_s['col_on'] = 1
            oi_r = oi.loc[oi['is_store'] != 0, ['mng_reg_id', 'is_store']].copy()
            oi_r.reset_index(inplace=True)
            oi_r.rename(columns={'org_id': 'org_rec_id',
                                 'mng_reg_id': 'reg_rec_id',
                                 'is_store': 'is_store_r'}, inplace=True)
            oi_r['col_on'] = 1

            mv_cp = oi_s.set_index('col_on').join(oi_r.set_index('col_on'))
            mv_cp = mv_cp.loc[mv_cp['org_send_id'] != mv_cp['org_rec_id']].copy()
            mv_cp = mv_cp.loc[~((mv_cp['is_store_s'] == 0) &
                                (mv_cp['is_store_r'] == -1))].copy()
            mv_cp.drop(['is_store_s', 'is_store_r'], axis=1, inplace=True)

            return mv_cp

        def _extr_sr_we(mv_cp, sales_we):
            cols_grp = ['org_send_id', 'org_rec_id', 'reg_send_id', 'reg_rec_id']

            # Extracting weights of sending and receiving stores
            store_send_we = sales_we['store_we_a'].reset_index('org_id')
            store_send_we = store_send_we.groupby('org_id').mean()
            store_send_we.rename(columns={'store_we_a': 'org_send_we'},
                                 inplace=True)
            store_send_we = store_send_we.rename_axis('org_send_id')

            store_rec_we = sales_we['store_we_d'].reset_index('org_id')
            store_rec_we = store_rec_we.groupby('org_id').mean()
            store_rec_we.rename(columns={'store_we_d': 'org_rec_we'},
                                inplace=True)
            store_rec_we = store_rec_we.rename_axis('org_rec_id')

            # Merging all moving couples
            sr_we = mv_cp.set_index('org_send_id').join(store_send_we)
            sr_we.reset_index(inplace=True)
            sr_we = sr_we.set_index('org_rec_id').join(store_rec_we)
            sr_we.reset_index(inplace=True)
            sr_we['org_send_we'].fillna(store_send_we['org_send_we'].min(),
                                        inplace=True)
            sr_we['org_rec_we'].fillna(store_rec_we['org_rec_we'].max(),
                                       inplace=True)

            # Calculating sending-receiving weights
            sr_we['sr_we'] = sr_we[['org_send_we', 'org_rec_we']].prod(axis=1)
            sr_we['sr_we'] = sr_we['sr_we'].pow(1 / 2)

            sr_we = sr_we[cols_grp + ['sr_we']].copy()

            return sr_we

        def _normalize(sr_we):
            cols_grp = ['reg_send_id', 'reg_rec_id']
            cols_grp2 = ['org_send_id', 'org_rec_id']

            sr_we = sr_we.set_index(cols_grp)
            sr_we['max'] = sr_we['sr_we'].max(level=cols_grp)
            sr_we['min'] = sr_we['sr_we'].min(level=cols_grp)
            sr_we.reset_index(inplace=True)

            sr_we['range'] = sr_we['max'] - sr_we['min'] + 1.0E-10
            sr_we['diff'] = sr_we['sr_we'] - sr_we['min']
            sr_we['sr_we'] = sr_we['diff'] / sr_we['range']
            sr_we['sr_we'] = sr_we['sr_we'].map(lambda x: np.exp(x - 1))

            sr_we = sr_we[cols_grp2 + ['sr_we']].set_index(cols_grp2)

            return sr_we

        # Generating moving organization couples
        mv_cp = _gen_mov_cp(oi)

        # Extracting sending-receiving weights between organizations
        sr_we_p = _extr_sr_we(mv_cp, sales_we_p)
        sr_we_s = _extr_sr_we(mv_cp, sales_we_s)

        # Normalizing
        sr_we_p = _normalize(sr_we_p)
        sr_we_s = _normalize(sr_we_s)

        sr_we = sr_we_p.join(sr_we_s, lsuffix='_p', rsuffix='_s')

        return sr_we

    def cal_lead_time_we(self, oi):
        """
        Calculating moving leading-time weights between cities

        Parameters
        ----------
        oi : pd.DataFrame
             Organization basic information
             index : org_id
             columns : prov_id, city_id, dist_id, mng_reg_id, is_store(1)

        Returns
        -------
        lt_we : pd.DataFrame
                Moving leading-time weights between cities
                index : city_send_id, city_rec_id
                columns : lt_we
        """

        def _gen_mov_cp(oi):
            # Generating moving organization couples
            oi_s = oi.loc[oi['is_store'] != -1].copy()
            oi_s = oi_s.drop('is_store', axis=1).drop_duplicates()
            oi_s.columns = [x + '_s' for x in oi_s.columns]
            oi_s['col_on'] = 1
            oi_r = oi.loc[oi['is_store'] != 0].copy()
            oi_r = oi_r.drop('is_store', axis=1).drop_duplicates()
            oi_r.columns = [x + '_r' for x in oi_r.columns]
            oi_r['col_on'] = 1

            mv_cp = oi_s.set_index('col_on').join(oi_r.set_index('col_on'))
            mv_cp.drop_duplicates(subset=['city_id_s', 'city_id_r'], inplace=True)

            return mv_cp

        cols_grp = ['city_send_id', 'city_rec_id']

        # Generating moving organization couples
        lt_we = _gen_mov_cp(oi)

        # Setting relative moving leading-time between cities
        lt_we['lt_mv'] = 0
        lt_we.loc[lt_we['city_id_s'] != lt_we['city_id_s'], 'lt_mv'] += 1
        lt_we.loc[lt_we['mng_reg_id_s'] != lt_we['mng_reg_id_s'], 'lt_mv'] += 1
        lt_we.loc[lt_we['prov_id_s'] != lt_we['prov_id_s'], 'lt_mv'] += 1
        lt_we.loc[lt_we['dist_id_s'] != lt_we['dist_id_s'], 'lt_mv'] += 1

        # Normalizing
        lt_we['min'] = lt_we['lt_mv'].min()
        lt_we['max'] = lt_we['lt_mv'].max()
        lt_we['range'] = lt_we['max'] - lt_we['min'] + 1.0E-10
        lt_we['diff'] = lt_we['lt_mv'] - lt_we['min']
        lt_we['lt_we'] = lt_we['diff'] / lt_we['range']

        lt_we.rename(columns={'city_id_s': 'city_send_id',
                              'city_id_r': 'city_rec_id'}, inplace=True)
        lt_we = lt_we[cols_grp + ['lt_we']].set_index(cols_grp)

        return lt_we

    def cal_inv_dev_we(self, ms, i0, s):
        """
        Calculating inventory balance weights

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

        Returns
        -------
        ib_we : pd.DataFrame
                Inventory balance weights
                index : prod_id, color_id
                columns : ib_we
        """

        cols_grp = ['prod_id', 'color_id', 'size']

        # Calculating sum initial inventory and sales of each skc/size
        i0_sum = i0['i0_sum'].reset_index(cols_grp).groupby(cols_grp).sum()
        s_sum = s.reset_index(cols_grp).groupby(cols_grp).sum()

        # Merging data and calculating correlation coefficient between sum initial
        # inventory and sales of each size given skc
        ib_we = ms['size'].reset_index(cols_grp[:2]).drop_duplicates()
        ib_we.set_index(cols_grp, inplace=True)
        ib_we = ib_we.join([i0_sum, s_sum]).reset_index()
        ib_we.dropna(subset=['i0_sum', 's'], how='all', inplace=True)
        ib_we.fillna({'i0_sum': 0, 's': 0}, inplace=True)
        ib_we['i0_s'] = ib_we[['i0_sum', 's']].sum(axis=1)
        ib_we['i0_sum'] += 1
        ib_we['i0_s'] += 1
        ib_we = ib_we[cols_grp[:2] + ['i0_sum', 'i0_s']].groupby(cols_grp[:2]) \
            .apply(lambda df: df[['i0_sum', 'i0_s']].corr().at['i0_sum', 'i0_s'])
        ib_we = ib_we.to_frame(name='cc').fillna({'cc': 1})

        # Calculating inventory balance weights
        ib_we['ib_we'] = ib_we['cc'].map(lambda x: 1 / (1 + np.exp(-10 * (x + 1))))
        ib_we.drop('cc', axis=1, inplace=True)

        return ib_we

    def extr_we(self, po, oi, ms, i0, s):
        """
        Extracting weights

        Parameters
        ----------
        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        oi : pd.DataFrame
             Organization basic information
             index : org_id
             columns : prov_id, city_id, dist_id, mng_reg_id, is_store(1)

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

        Returns
        -------
        sales_we_p : pd.DataFrame
                     Sum sales weights of skc, size, and store based on skc
                     index : prod_id, color_id, size, org_id
                     columns : skc_we_a, skc_we_d, store_we_a, store_we_d,
                               sks_we_a, sks_we_d, sum_we_a, sum_we_d

        sales_we_s : pd.DataFrame
                     Sum sales weights of skc, size, and store based on stores
                     index : prod_id, color_id, size, org_id
                     columns : skc_we_a, skc_we_d, store_we_a, store_we_d,
                               sks_we_a, sks_we_d, sum_we_a, sum_we_d

        sales_we_all : pd.DataFrame
                       Sum sales weights of skc, size, and store based on skc in
                       all stores
                       index : prod_id, color_id, size, org_id
                       columns : skc_we_a, skc_we_d, store_we_a, store_we_d,
                                 sks_we_a, sks_we_d, sum_we_a, sum_we_d

        sr_we : pd.DataFrame
                Sending-receiving weights between orgs
                index : org_send_id, org_rec_id
                columns : sr_we_p, sr_we_s

        ib_we : pd.DataFrame
                Inventory balance weights
                index : prod_id, color_id
                columns : ib_we

        lt_we : pd.DataFrame
                Moving leading-time weights between cities
                index : city_send_id, city_rec_id
                columns : lt_we
        """

        # Calculating sales weights
        sales_we_p, sales_we_s, sales_we_all = self.cal_sales_we(po, i0, s)

        # Calculating sending-receiving weights between organizations
        sr_we = self.cal_sr_we(oi, sales_we_p, sales_we_s)

        # Calculating moving leading-time weights between cities
        lt_we = self.cal_lead_time_we(oi)

        # Calculating inventory balance weights
        ib_we = self.cal_inv_dev_we(ms, i0, s)

        return sales_we_p, sales_we_s, sales_we_all, sr_we, lt_we, ib_we
