# -*- coding: utf-8 -*-

import datetime as dt

import numpy as np
import pandas as pd

from check.TypeAssert import typeassert


class DataProcess(object):

    def __init__(self):
        pass

    @typeassert(i0=pd.DataFrame, io=pd.DataFrame)
    def init_data(self, i0, io):
        """
        Initializing data

        Parameters
        ----------
        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r, i0_sum

        io : pd.DataFrame
             Markers of existed moving -in/-out
             index : prod_id, color_id, size, org_id
             columns : has_in, has_out

        Returns
        -------
        q : pd.DataFrame
            Moving quantity of each sks between organizations
            index : prod_id, color_id, size, org_send_id, org_rec_id
            columns : qty_mov

        i0_aft_mov : pd.DataFrame
                     Inventory data after moving
                     index : prod_id, color_id, size, org_id
                     columns : i0, r, i0_sum

        io_aft_mov : pd.DataFrame
                     Markers of existed moving -in/-out after moving
                     index : prod_id, color_id, size, org_id
                     columns : has_in, has_out
        """

        cols_mov = ['prod_id', 'color_id', 'size', 'org_send_id', 'org_rec_id']

        # Moving records
        q = pd.DataFrame(columns=cols_mov + ['qty_mov']).set_index(cols_mov)

        # Updating initial inventory and io markers
        i0_aft_mov = i0.copy()
        io_aft_mov = io.copy()
        io_aft_mov['has_in'] = 0

        return q, i0_aft_mov, io_aft_mov

    @typeassert(po=pd.DataFrame, po_mov=pd.DataFrame, i0=pd.DataFrame, q=pd.DataFrame)
    def cal_ibd(self, po, po_mov, i0, q):
        """
        Calculating lower bounds of skc number and inventory

        Parameters
        ----------
        po : pd.DataFrame
             Crossed products and organizations
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_toc,
                       is_ds, is_fp, is_rep, is_new

        po_mov : pd.DataFrame
                 Target products and organizations of moving
                 index : prod_id, color_id, size, org_id
                 columns : season_id, to_in, to_out, to_emp

        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r, i0_sum

        q : pd.DataFrame
            Moving quantity of each sks between organizations
            index : prod_id, color_id, size, org_send_id, org_rec_id
            columns : qty_mov

        Returns
        -------
        i_bd : pd.DataFrame
               Lower bounds of skc number and inventory after moving
               index : org_id, season_id
               columns : qs_lb, qd_lb
        """

        def _cal_qid(i0, po):
            cols_grp = ['prod_id', 'color_id', 'org_id', 'season_id']

            qid = i0.reset_index().set_index(cols_grp[:3]).join(po, how='inner')
            qid = qid.reset_index().fillna(0)
            qid = qid[cols_grp + ['i0_sum']].groupby(cols_grp).sum().reset_index()
            qid.rename(columns={'i0_sum': 'qs'}, inplace=True)
            qid['qd'] = qid['qs'].map(np.sign)
            qid = qid[cols_grp[2:] + ['qs', 'qd']].groupby(cols_grp[2:]).sum()

            return qid

        cols_grp = ['prod_id', 'color_id', 'org_id', 'season_id']

        # Calculating sum moving -in/-out quantity of each sks/org
        qis, qos = self.cal_qios(q)

        # Updating inventory after moving
        ia = self.update_inv(i0, qis, qos)
        ia_s = ia.loc[ia.index.isin(po_mov.index)].copy()

        # Extracting valid season and display information
        po_c = po.loc[(po['is_store'] == 1) &
                      (po['to_emp'] == 0), 'season_id'].reset_index(cols_grp[:3])
        po_c = po_c.drop_duplicates().set_index(cols_grp[:3])
        di = po.loc[po['is_store'] == 1,
                    ['season_id', 'qs_min', 'qd_min']].reset_index('org_id')
        di = di.drop_duplicates().set_index(cols_grp[2:])

        # Calculating skc number and inventory of each store/season
        qid = _cal_qid(ia, po_c)
        qid_s = _cal_qid(ia_s, po_c)
        qid_s.rename(columns={'qs': 'qs_s', 'qd': 'qd_s'}, inplace=True)

        i_bd = di.join([qid, qid_s]).reset_index().fillna(0)
        i_bd['qs_r'] = i_bd['qs'] - i_bd['qs_min']
        i_bd['qd_r'] = i_bd['qd'] - i_bd['qd_min']
        i_bd.loc[i_bd['qs_r'] < 0, 'qs_r'] = 0
        i_bd.loc[i_bd['qd_r'] < 0, 'qd_r'] = 0
        i_bd['qs_lb'] = i_bd['qs_s'] - i_bd['qs_r']
        i_bd['qd_lb'] = i_bd['qd_s'] - i_bd['qd_r']

        i_bd = i_bd[cols_grp[2:] + ['qs_lb', 'qd_lb']].set_index(cols_grp[2:])

        return i_bd

    @typeassert(q=dict, cmp=pd.DataFrame)
    def update_cmp(self, q, cmp):
        """
        Updating moving package unit cost

        Parameters
        ----------
        q : dict
            Moving quantity of each sks between organizations
            key : prod_id, color_id, size, org_send_id, org_rec_id
            value : moving quantity

        cmp : pd.DataFrame
              Unit cost of moving packages
              key : org_send_id, org_rec_id
              value : cmp

        Returns
        -------
        q_df : pd.DataFrame
               Moving quantity of each sks between organizations
               index : prod_id, color_id, size, org_send_id, org_rec_id
               columns : qty_mov

        cmp_p : pd.DataFrame
                Updated unit cost of moving packages
                key : org_send_id, org_rec_id
                value : cmp
        """

        cols_mov = ['prod_id', 'color_id', 'size', 'org_send_id', 'org_rec_id']

        q_df = pd.DataFrame()
        cmp_p = cmp.copy()
        if sum(q.values()) > 0:
            q_df = pd.DataFrame(q, index=['qty_mov']).T
            q_df = q_df.rename_axis(cols_mov)
            q_df = q_df.loc[q_df['qty_mov'] > 0].copy()
            q_ptp = q_df['qty_mov'].reset_index(['org_send_id', 'org_rec_id']) \
                .groupby(['org_send_id', 'org_rec_id']).sum()
            cmp_p = cmp_p.join(q_ptp).fillna(0)
            cmp_p.loc[cmp_p['qty_mov'] > 0, 'cmp'] = 0
            cmp_p.drop('qty_mov', axis=1, inplace=True)

        return q_df, cmp_p

    @typeassert(po=pd.DataFrame, q=pd.DataFrame, qmp_max=int)
    def cal_qmp(self, po, q, qmp_max):
        """
        Calculating moving-out package number of each skc/org

        Parameters
        ----------
        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        q : pd.DataFrame
            Moving quantity of each sks between organizations
            index : prod_id, color_id, size, org_send_id, org_rec_id
            columns : qty_mov

        qmp_max : int
                  The maximal moving-out package number of each skc/org

        Returns
        -------
        qmp : pd.DataFrame
              Residual moving-out package number
              index : prod_id, color_id, org_id
              columns : qmp_r
        """

        cols_grp = ['prod_id', 'color_id', 'org_id', 'org_rec_id']

        si = po[po['is_store'] == 1].index.get_level_values('org_id').unique()

        qmp = pd.DataFrame(index=po[po['is_store'] == 1].reset_index('size')
                           .index.drop_duplicates())
        qmp['qmp_r'] = qmp_max

        q_skcs = q.reset_index()
        q_skcs = q_skcs.loc[(q_skcs['org_send_id'].isin(si)) &
                            (q_skcs['org_rec_id'].isin(si))].copy()

        if q_skcs.empty:
            return qmp

        q_skcs.rename(columns={'org_send_id': 'org_id'}, inplace=True)
        q_skcs = q_skcs[cols_grp + ['qty_mov']].groupby(cols_grp).sum()
        q_skcs.reset_index(inplace=True)
        q_skcs['qmp'] = q_skcs['qty_mov'].map(np.sign)
        q_skcs = q_skcs[cols_grp[:3] + ['qmp']].groupby(cols_grp[:3]).sum()
        qmp = qmp.join(q_skcs).fillna({'qmp': 0})
        qmp['qmp_r'] -= qmp['qmp']
        qmp.loc[qmp['qmp_r'] < 0, 'qmp_r'] = 0
        qmp.drop('qmp', axis=1, inplace=True)

        return qmp

    @typeassert(q=pd.DataFrame, q_a=pd.DataFrame)
    def update_q(self, q, q_a):
        """
        Updating moving quantity

        Parameters
        ----------
        q : pd.DataFrame
            Moving quantity of each sks between organizations
            index : prod_id, color_id, size, org_send_id, org_rec_id
            columns : qty_mov

        q_a : pd.DataFrame
              Appended moving quantity of each sks between organizations
              index : prod_id, color_id, size, org_send_id, org_rec_id
              columns : qty_mov

        Returns
        -------
        q_n : pd.DataFrame
              Updated moving quantity of each sks between organizations
              index : prod_id, color_id, size, org_send_id, org_rec_id
              columns : qty_mov
        """

        q_n = q.join(q_a, how='outer', rsuffix='_add', sort=True).fillna(0)
        q_n['qty_mov'] += q_n['qty_mov_add']
        q_n.drop(q_n[q_n['qty_mov'] == 0].index, inplace=True)
        q_n.drop('qty_mov_add', axis=1, inplace=True)

        return q_n

    @typeassert(q=pd.DataFrame)
    def cal_qios(self, q):
        """
        Calculating sum moving -in/-out quantity of each sks/org

        Parameters
        ----------
        q : pd.DataFrame
            Moving quantity of each sks between organizations
            index : prod_id, color_id, size, org_send_id, org_rec_id
            columns : qty_mov

        Returns
        -------
        qis : pd.DataFrame
              Sum moving-in quantity of each sks/org
              index : prod_id, color_id, size, org_id
              columns : qis

        qos : pd.DataFrame
              Sum moving-out quantity of each sks/org
              index : prod_id, color_id, size, org_id
              columns : qos
        """

        cols_send = ['prod_id', 'color_id', 'size', 'org_send_id']
        cols_rec = ['prod_id', 'color_id', 'size', 'org_rec_id']
        cols_stc = ['prod_id', 'color_id', 'size', 'org_id']

        if q.empty:
            qis = pd.DataFrame(columns=cols_stc + ['qis']).set_index(cols_stc)
            qos = pd.DataFrame(columns=cols_stc + ['qos']).set_index(cols_stc)
        else:
            qis = q.reset_index(cols_rec).groupby(cols_rec).sum() \
                .rename(columns={'qty_mov': 'qis'}).rename_axis(cols_stc)
            qos = q.reset_index(cols_send).groupby(cols_send).sum() \
                .rename(columns={'qty_mov': 'qos'}).rename_axis(cols_stc)

        return qis, qos

    @typeassert(i0=pd.DataFrame, qis=pd.DataFrame, qos=pd.DataFrame)
    def update_inv(self, i0, qis, qos):
        """
        Updating inventory after moving

        Parameters
        ----------
        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r, i0_sum

        qis : pd.DataFrame
              Sum moving-in quantity of each sks/org
              index : prod_id, color_id, size, org_id
              columns : qis

        qos : pd.DataFrame
              Sum moving-out quantity of each sks/org
              index : prod_id, color_id, size, org_id
              columns : qos

        Returns
        -------
        ia : pd.DataFrame
             Updated inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r, i0_sum
        """

        ia = i0.join([qis, qos], how='outer').fillna(0)
        ia['i0'] -= ia['qos']
        ia['r'] += ia['qis']
        ia['i0_sum'] = ia[['i0', 'r']].sum(axis=1)
        ia.drop(['qis', 'qos'], axis=1, inplace=True)

        return ia

    @typeassert(io=pd.DataFrame, qis=pd.DataFrame, qos=pd.DataFrame)
    def update_io(self, io, qis, qos):
        """
        Updating io markers

        Parameters
        ----------
        io : pd.DataFrame
             Markers of existed moving -in/-out
             index : prod_id, color_id, size, org_id
             columns : has_in, has_out

        qis : pd.DataFrame
              Sum moving-in quantity of each sks/org
              index : prod_id, color_id, size, org_id
              columns : qis

        qos : pd.DataFrame
              Sum moving-out quantity of each sks/org
              index : prod_id, color_id, size, org_id
              columns : qos

        Returns
        -------
        io_n : pd.DataFrame
               Updated markers of existed moving -in/-out
               index : prod_id, color_id, size, org_id
               columns : has_in, has_out
        """

        io_n = io.join([qis, qos])
        io_n.loc[io_n['qis'] > 0, 'has_in'] = 1
        io_n.loc[io_n['qos'] > 0, 'has_out'] = 1
        io_n.drop(['qis', 'qos'], axis=1, inplace=True)

        return io_n

    @typeassert(q=pd.DataFrame, q_a=pd.DataFrame, i0=pd.DataFrame, io=pd.DataFrame)
    def postp_data(self, q, q_a, i0, io):
        """
        Post-precessing data
        Parameters
        ----------
        q : pd.DataFrame
            Moving quantity of each sks between organizations
            index : prod_id, color_id, size, org_send_id, org_rec_id
            columns : qty_mov

        q_a : pd.DataFrame
              Appended moving quantity of each sks between organizations
              index : prod_id, color_id, size, org_send_id, org_rec_id
              columns : qty_mov

        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r, i0_sum

        io : pd.DataFrame
             Markers of existed moving -in/-out
             index : prod_id, color_id, size, org_id
             columns : has_in, has_out

        Returns
        -------
        q_n : pd.DataFrame
              Updated moving quantity of each sks between organizations
              index : prod_id, color_id, size, org_send_id, org_rec_id
              columns : qty_mov

        ia : pd.DataFrame
             Updated inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r, i0_sum

        io_n : pd.DataFrame
               Updated markers of existed moving -in/-out
               index : prod_id, color_id, size, org_id
               columns : has_in, has_out
        """

        # Updating moving quantity
        q_n = self.update_q(q, q_a)

        # Calculating sum moving -in/-out quantity
        qis, qos = self.cal_qios(q_a)

        # Updating inventory after moving
        ia = self.update_inv(i0, qis, qos)

        # Updating io markers
        io_n = self.update_io(io, qis, qos)

        return q_n, ia, io_n

    @typeassert(po=pd.DataFrame, ms=pd.DataFrame, i0=pd.DataFrame, s=pd.DataFrame, itp=pd.DataFrame, io=pd.DataFrame,
                d=pd.DataFrame, qss=pd.DataFrame, it=pd.DataFrame, q=pd.DataFrame)
    def merge_all_data(self, po, ms, i0, s, itp, io, d, qss, it, q):
        """
        Merging all data

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

        q : pd.DataFrame
            Moving quantity of each sks between organizations
            index : prod_id, color_id, size, org_send_id, org_rec_id
            columns : qty_mov

        Returns
        -------
        all_data : pd.DataFrame
                   All data for decision
                   index : position
                   columns : prod_id, color_id, size, org_id, city_id, mng_reg_id,
                             is_new, is_rep, is_trans, is_ret, not_in, not_out,
                             to_emp, is_ms, i0, r, i0_sum, s_cum, s_b2, s_b1, d_lb,
                             d_ub, qss, it, itp, has_in, has_out, qis, qos, ia
        """

        cols_send = ['prod_id', 'color_id', 'size', 'org_send_id']
        cols_rec = ['prod_id', 'color_id', 'size', 'org_rec_id']
        cols_stc = ['prod_id', 'color_id', 'size', 'org_id']
        cols_grp = ['city_id', 'mng_reg_id', 'is_new', 'is_rep', 'is_trans',
                    'is_ret', 'not_in', 'not_out', 'to_emp']

        # Marking main sizes
        ms2 = ms['size'].reset_index().set_index(cols_stc)
        ms2['is_ms'] = 1

        # Calculating cumulative sales
        s_cum = s.reset_index(cols_stc).groupby(cols_stc).sum()
        s_cum = s_cum.loc[s_cum.index.isin(po.index)].copy()
        s_cum.rename(columns={'s': 's_cum'}, inplace=True)

        # Calculating weekly sales of previous two weeks
        s_c = s.reset_index('week_no')
        s_pre2 = s_c.loc[s_c['week_no'] == -2, ['s']].copy()
        s_pre2 = s_pre2.loc[s_pre2.index.isin(po.index)].copy()
        s_pre2.rename(columns={'s': 's_b2'}, inplace=True)

        s_pre1 = s_c.loc[s_c['week_no'] == -1, ['s']].copy()
        s_pre1 = s_pre1.loc[s_pre1.index.isin(po.index)].copy()
        s_pre1.rename(columns={'s': 's_b1'}, inplace=True)

        # Calculating sum moving -in/-out quantity
        qis = q.reset_index(cols_rec).groupby(cols_rec).sum() \
            .rename(columns={'qty_mov': 'qis'}).rename_axis(cols_stc)
        qos = q.reset_index(cols_send).groupby(cols_send).sum() \
            .rename(columns={'qty_mov': 'qos'}).rename_axis(cols_stc)

        # Calculating available inventory after moving
        ia = i0.join([qis, qos], how='outer').fillna(0)
        ia['qos'] = -1 * ia['qos']
        ia['ia'] = ia[['i0_sum', 'qis', 'qos']].sum(axis=1)
        ia.drop(['i0', 'r', 'i0_sum', 'qis', 'qos'], axis=1, inplace=True)

        # Merging all basic data
        all_data = po[cols_grp].join([ms2, i0, s_cum, s_pre2, s_pre1, d, qss, it,
                                      qis, qos, ia, io]).reset_index()
        all_data.fillna({'is_ms': 0, 'i0': 0, 'r': 0, 'i0_sum': 0, 'd_lb': 0,
                         'd_ub': 0, 'qss': 0, 'qis': 0, 'qos': 0, 'ia': 0,
                         'has_in': 0, 'has_out': 0}, inplace=True)
        all_data.loc[(all_data['i0'] > 0) &
                     (all_data['s_b1'].isnull()), 's_b1'] = 0
        if not itp.empty:
            all_data = all_data.set_index(cols_stc).join(itp).reset_index()
        all_data.sort_values(cols_stc, inplace=True)

        return all_data

    @typeassert(q=pd.DataFrame, oi=pd.DataFrame, date_dec=dt.date)
    def gen_rep_tab(self, q, oi, date_dec):
        """
        Generating replenishment table

        Parameters
        ----------
        q : pd.DataFrame
            Moving quantity of each sks between organizations
            index : prod_id, color_id, size, org_send_id, org_rec_id
            columns : qty_mov

        oi : pd.DataFrame
             Organization basic information
             index : org_id
             columns : prov_id, city_id, dist_id, mng_reg_id, is_store(1)

        date_dec : date
                   Decision making date

        Returns
        -------
        q_rep : pd.DataFrame
                Replenishment table
                index : position
                columns : prod_id, color_id, size, org_send_id, org_rec_id,
                          date_send, qty_mov
        """

        cols_mov = ['prod_id', 'color_id', 'size', 'org_send_id', 'org_rec_id']

        org_rep_s = oi[oi['is_store'] == 0].index.unique()
        org_rep_r = oi[oi['is_store'] == 1].index.unique()
        q_rep = q.reset_index()
        q_rep = q_rep.loc[(q_rep['org_send_id'].isin(org_rep_s)) &
                          (q_rep['org_rec_id'].isin(org_rep_r))].copy()
        q_rep['date_send'] = pd.to_datetime(date_dec)

        if q_rep.empty:
            q_rep = pd.DataFrame(columns=cols_mov + ['date_send', 'qty_mov'])

        return q_rep

    @typeassert(q=pd.DataFrame, oi=pd.DataFrame, date_dec=dt.date)
    def gen_trans_tab(self, q, oi, date_dec):
        """
        Generating transferring table

        Parameters
        ----------
        q : pd.DataFrame
            Moving quantity of each sks between organizations
            index : prod_id, color_id, size, org_send_id, org_rec_id
            columns : qty_mov

        oi : pd.DataFrame
             Organization basic information
             index : org_id
             columns : prov_id, city_id, dist_id, mng_reg_id, is_store(1)

        date_dec : date
                   Decision making date

        Returns
        -------
        q_trans : pd.DataFrame
                  Transferring table
                  index : position
                  columns : prod_id, color_id, size, org_send_id, org_rec_id,
                            date_send, qty_mov
        """

        cols_mov = ['prod_id', 'color_id', 'size', 'org_send_id', 'org_rec_id']

        org_trans_s = oi[oi['is_store'] == 1].index.unique()
        org_trans_r = oi[oi['is_store'] == 1].index.unique()
        q_trans = q.reset_index()
        q_trans = q_trans.loc[(q_trans['org_send_id'].isin(org_trans_s)) &
                              (q_trans['org_rec_id'].isin(org_trans_r))].copy()
        q_trans['date_send'] = pd.to_datetime(date_dec)

        if q_trans.empty:
            q_trans = pd.DataFrame(columns=cols_mov + ['date_send', 'qty_mov'])

        return q_trans

    @typeassert(q=pd.DataFrame, oi=pd.DataFrame, date_dec=dt.date)
    def gen_ret_tab(self, q, oi, date_dec):
        """
        Generating return table

        Parameters
        ----------
        q : pd.DataFrame
            Moving quantity of each sks between organizations
            index : prod_id, color_id, size, org_send_id, org_rec_id
            columns : qty_mov

        oi : pd.DataFrame
             Organization basic information
             index : org_id
             columns : prov_id, city_id, dist_id, mng_reg_id, is_store(1)

        date_dec : date
                   Decision making date

        Returns
        -------
        q_ret : pd.DataFrame
                Return table
                index : position
                columns : prod_id, color_id, size, org_send_id, org_rec_id,
                          date_send, qty_mov
        """

        cols_mov = ['prod_id', 'color_id', 'size', 'org_send_id', 'org_rec_id']

        org_ret_s = oi[oi['is_store'] == 1].index.unique()
        org_ret_r = oi[oi['is_store'] == -1].index.unique()
        q_ret = q.reset_index()
        q_ret = q_ret.loc[(q_ret['org_send_id'].isin(org_ret_s)) &
                          (q_ret['org_rec_id'].isin(org_ret_r))].copy()
        q_ret['date_send'] = pd.to_datetime(date_dec)

        if q_ret.empty:
            q_ret = pd.DataFrame(columns=cols_mov + ['date_send', 'qty_mov'])

        return q_ret

    @typeassert(q=pd.DataFrame, oi=pd.DataFrame, date_dec=dt.date)
    def gen_mov_tab(self, q, oi, date_dec):
        """
        Generating replenishment/transferring/return tables

        Parameters
        ----------
        q : pd.DataFrame
            Moving quantity of each sks between organizations
            index : prod_id, color_id, size, org_send_id, org_rec_id
            columns : qty_mov

        oi : pd.DataFrame
             Organization basic information
             index : org_id
             columns : prov_id, city_id, dist_id, mng_reg_id, is_store(1)

        date_dec : date
                   Decision making date

        Returns
        -------
        q_rep : pd.DataFrame
                Replenishment table
                index : position
                columns : prod_id, color_id, size, org_send_id, org_rec_id,
                          date_send, qty_mov, type_mov

        q_trans : pd.DataFrame
                  Transferring table
                  index : position
                  columns : prod_id, color_id, size, org_send_id, org_rec_id,
                            date_send, qty_mov, type_mov

        q_ret : pd.DataFrame
                Returning table
                index : position
                columns : prod_id, color_id, size, org_send_id, org_rec_id,
                          date_send, qty_mov, type_mov
        """

        cols_mov = ['prod_id', 'color_id', 'size', 'org_send_id', 'org_rec_id']

        if q.empty:
            q_rep = pd.DataFrame(
                columns=cols_mov + ['date_send', 'qty_mov', 'type_mov'])
            q_trans = pd.DataFrame(
                columns=cols_mov + ['date_send', 'qty_mov', 'type_mov'])
            q_ret = pd.DataFrame(
                columns=cols_mov + ['date_send', 'qty_mov', 'type_mov'])
            return q_rep, q_trans, q_ret

        q = q.reset_index()

        wh_rep = oi[oi['is_store'] == 0].index.unique()
        store = oi[oi['is_store'] == 1].index.unique()
        wh_ret = oi[oi['is_store'] == -1].index.unique()

        q['type_mov'] = np.nan
        q.loc[(q['org_send_id'].isin(wh_rep)) &
              (q['org_rec_id'].isin(store)), 'type_mov'] = 'REP'
        q.loc[(q['org_send_id'].isin(store)) &
              (q['org_rec_id'].isin(store)), 'type_mov'] = 'TRANS'
        q.loc[(q['org_send_id'].isin(store)) &
              (q['org_rec_id'].isin(wh_ret)), 'type_mov'] = 'REP'
        q['date_send'] = pd.to_datetime(date_dec)

        q_rep = q.loc[q['type_mov'] == 'REP', :].copy()
        q_trans = q.loc[q['type_mov'] == 'TRANS', :].copy()
        q_ret = q.loc[q['type_mov'] == 'RET', :].copy()

        return q_rep, q_trans, q_ret
