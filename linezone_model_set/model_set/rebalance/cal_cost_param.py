# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

from check.TypeAssert import typeassert


class CostParam(object):
    def __init__(self):
        pass

    def cal_mov_quant_cost(self, po, io, itp, sales_we_p, sales_we_s, spr_we, cmq_base_ws, cmq_base_ss, cmq_base_sw):
        """
        Calculating unit cost of moving quantity

        Parameters
        ----------
        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        io : pd.DataFrame
             Markers of existed moving -in/-out
             index : prod_id, color_id, size, org_id
             columns : has_in, has_out

        itp : pd.DataFrame
              Target inventory of the previous day
              index : prod_id, color_id, size, org_id
              columns : itp

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

        spr_we : float
                 Weight of sales being prior

        cmq_base_ws : float
                      Basic unit cost of moving quantity from warehouse to store

        cmq_base_ss : float
                      Basic unit cost of moving quantity between stores

        cmq_base_sw : float
                      Basic unit cost of moving quantity from store to warehouse

        Returns
        -------
        cmq : pd.DataFrame
              Unit cost of moving quantity
              index : prod_id, color_id, size, org_id
              columns : cmq_rep, cmq_trans_in, cmq_trans_out, cmq_ret
        """

        cols_grp = ['prod_id', 'color_id', 'size', 'org_id']
        cols_grp2 = ['cmq_rep', 'cmq_trans_in', 'cmq_trans_out', 'cmq_ret']

        # Merging all skc-org couples with sales weights
        cmq = po.loc[po['is_store'] == 1, ['to_emp']].copy()
        cmq = cmq.join(sales_we_p[['sum_we_a', 'sum_we_d']])
        cmq.rename(columns={'sum_we_a': 'sum_we_p_a', 'sum_we_d': 'sum_we_p_d'},
                   inplace=True)
        cmq = cmq.join(sales_we_s[['sum_we_a', 'sum_we_d']])
        cmq.rename(columns={'sum_we_a': 'sum_we_s_a', 'sum_we_d': 'sum_we_s_d'},
                   inplace=True)
        cmq = cmq.join(io['has_in']).reset_index()
        cmq.fillna({'sum_we_p_a': sales_we_p['sum_we_a'].min(),
                    'sum_we_p_d': sales_we_p['sum_we_d'].max(),
                    'sum_we_s_a': sales_we_s['sum_we_a'].min(),
                    'sum_we_s_d': sales_we_s['sum_we_d'].max(),
                    'has_in': 0}, inplace=True)
        cmq['sum_we_a'] = cmq['sum_we_p_a'] * spr_we + cmq['sum_we_s_a'] * (1 - spr_we)
        cmq['sum_we_d'] = cmq['sum_we_p_d'] * spr_we + cmq['sum_we_s_d'] * (1 - spr_we)
        cmq['is_itp_0'] = 0
        if not itp.empty:
            cmq.set_index(cols_grp, inplace=True)
            cmq = cmq.join(itp).reset_index()
            cmq['is_itp_0'] = 1 - cmq['itp'].fillna({'itp': 0}).map(np.sign)

        # Calculating cost
        cmq['cmq_rep'] = cmq_base_ws * cmq['sum_we_p_d']
        cmq['cmq_trans_in'] = cmq_base_ss * cmq['sum_we_d']
        cmq['cmq_trans_out'] = cmq_base_ss * (cmq['sum_we_a'] *
                                              (cmq['has_in'] + 1) *
                                              (1 - cmq['to_emp']) *
                                              (1 - cmq['is_itp_0']))
        cmq['cmq_ret'] = cmq_base_sw

        cmq = cmq[cols_grp + cols_grp2].set_index(cols_grp).sort_index()

        return cmq

    def cal_mov_pkg_cost(self, oi, sr_we, spr_we, cmp_base_ws, cmp_base_ss, cmp_base_sw):
        """
        Calculating unit cost of moving packages

        Parameters
        ----------
        oi : pd.DataFrame
             Organization basic information
             index : org_id
             columns : prov_id, city_id, dist_id, mng_reg_id, is_store(1)

        sr_we : pd.DataFrame
                Sending-receiving weights between orgs
                index : org_send_id, org_rec_id
                columns : sr_we_p, sr_we_s

        spr_we : float
                 Weight of sales being prior

        cmp_base_ws : float
                      Basic unit cost of moving packages from warehouse to store

        cmp_base_ss : float
                      Basic unit cost of moving packages between stores

        cmp_base_sw : float
                      Basic unit cost of moving packages from store to warehouse

        Returns
        -------
        cmp : pd.DataFrame
              Unit cost of moving packages
              key : org_send_id, org_rec_id
              value : cmp
        """

        def _gen_mov_cp(oi):
            # Generating moving organization couples
            oi_s = oi.loc[oi['is_store'] != -1, ['is_store']].reset_index()
            oi_s.rename(columns={'org_id': 'org_send_id',
                                 'is_store': 'is_store_s'}, inplace=True)
            oi_s['col_on'] = 1
            oi_r = oi.loc[oi['is_store'] != 0, ['is_store']].reset_index()
            oi_r.rename(columns={'org_id': 'org_rec_id',
                                 'is_store': 'is_store_r'}, inplace=True)
            oi_r['col_on'] = 1

            mv_cp = oi_s.set_index('col_on').join(oi_r.set_index('col_on'))
            mv_cp = mv_cp.loc[mv_cp['org_send_id'] != mv_cp['org_rec_id']].copy()
            mv_cp = mv_cp.loc[~((mv_cp['is_store_s'] == 0) &
                                (mv_cp['is_store_r'] == -1))].copy()

            mv_cp['is_rep'] = 0
            mv_cp['is_trans'] = 0
            mv_cp['is_ret'] = 0
            mv_cp.loc[(mv_cp['is_store_s'] == 0) &
                      (mv_cp['is_store_r'] == 1), 'is_rep'] = 1
            mv_cp.loc[(mv_cp['is_store_s'] == 1) &
                      (mv_cp['is_store_r'] == 1), 'is_trans'] = 1
            mv_cp.loc[(mv_cp['is_store_s'] == 1) &
                      (mv_cp['is_store_r'] == -1), 'is_ret'] = 1

            mv_cp.drop(['is_store_s', 'is_store_r'], axis=1, inplace=True)

            return mv_cp

        cols_grp = ['org_send_id', 'org_rec_id']

        # Generating moving organization couples
        mv_cp = _gen_mov_cp(oi)

        # Merging all moving couples with sending-receiving weights
        cmp = mv_cp.set_index(cols_grp).join(sr_we).reset_index()
        cmp['sr_we_p'].fillna(cmp['sr_we_p'].max(), inplace=True)
        cmp['sr_we_s'].fillna(cmp['sr_we_s'].max(), inplace=True)
        cmp['sr_we'] = cmp['sr_we_p'] * spr_we + cmp['sr_we_s'] * (1 - spr_we)

        # Calculating cost
        cmp['cmp'] = (cmp_base_ws * cmp[['sr_we', 'is_rep']].prod(axis=1) +
                      cmp_base_ss * cmp[['sr_we', 'is_trans']].prod(axis=1) +
                      cmp_base_sw * cmp[['sr_we', 'is_ret']].prod(axis=1))

        cmp = cmp[cols_grp + ['cmp']].set_index(cols_grp).sort_index()

        return cmp

    def cal_mov_time_cost(self, oi, lt_we, cmt_base_ss):
        """
        Calculating unit cost of moving time

        Parameters
        ----------
        oi : pd.DataFrame
             Organization basic information
             index : org_id
             columns : prov_id, city_id, dist_id, mng_reg_id, is_store(1)

        lt_we : pd.DataFrame
                Moving leading-time weights between cities
                index : city_send_id, city_rec_id
                columns : lt_we

        cmt_base_ss : float
                      Basic unit cost of moving time between stores

        Returns
        -------
        cmt : pd.DataFrame
              Unit cost of moving time
              key : org_send_id, org_rec_id
              value : cmt
        """

        def _gen_mov_cp(oi):
            # Generating moving organization couples
            oi_s = oi.loc[oi['is_store'] != -1,
                          ['city_id', 'mng_reg_id', 'is_store']].reset_index()
            oi_s.rename(columns={'org_id': 'org_send_id',
                                 'city_id': 'city_send_id',
                                 'mng_reg_id': 'reg_send_id',
                                 'is_store': 'is_store_s'}, inplace=True)
            oi_s['col_on'] = 1
            oi_r = oi.loc[oi['is_store'] != 0,
                          ['city_id', 'mng_reg_id', 'is_store']].reset_index()
            oi_r.rename(columns={'org_id': 'org_rec_id',
                                 'city_id': 'city_rec_id',
                                 'mng_reg_id': 'reg_rec_id',
                                 'is_store': 'is_store_r'}, inplace=True)
            oi_r['col_on'] = 1

            mv_cp = oi_s.set_index('col_on').join(oi_r.set_index('col_on'))
            mv_cp = mv_cp.loc[mv_cp['org_send_id'] != mv_cp['org_rec_id']].copy()
            mv_cp = mv_cp.loc[~((mv_cp['is_store_s'] == 0) &
                                (mv_cp['is_store_r'] == -1))].copy()
            mv_cp.drop(['is_store_s', 'is_store_r'], axis=1, inplace=True)

            return mv_cp

        cols_grp = ['city_send_id', 'city_rec_id']
        cols_grp2 = ['org_send_id', 'org_rec_id']

        # Generating transferring organization couples
        mv_cp = _gen_mov_cp(oi)

        # Merging all moving couples with moving leading-time weights
        cmt = mv_cp.set_index(cols_grp).join(lt_we).reset_index()
        cmt.set_index(['reg_send_id', 'reg_rec_id'], inplace=True)
        cmt['max'] = cmt['lt_we'].max(level=['reg_send_id', 'reg_rec_id'])
        cmt.reset_index(inplace=True)
        cmt['lt_we'].fillna(cmt['max'], inplace=True)
        cmt['lt_we'].fillna(cmt['lt_we'].max(), inplace=True)

        # Calculating cost
        cmt['cmt'] = cmt_base_ss * cmt['lt_we']

        cmt = cmt[cols_grp2 + ['cmt']].set_index(cols_grp2).sort_index()

        return cmt

    def cal_inv_diff_cost(self, po, itp, sales_we_p, sales_we_s, sales_we_all, spr_we,
                          cid_base):
        """
        Calculating unit cost of inventory difference

        Parameters
        ----------
        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        itp : pd.DataFrame
              Target inventory of the previous day
              index : prod_id, color_id, size, org_id
              columns : itp

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

        spr_we : float
                 Weight of sales being prior

        cid_base : float
                   Basic unit cost of inventory difference

        Returns
        -------
        cid : pd.DataFrame
              Unit cost of inventory difference
              index : prod_id, color_id, size, org_id
              columns : cid_a, cid_d, cid_rep
        """

        cols_grp = ['prod_id', 'color_id', 'size', 'org_id']
        cols_grp2 = ['cid_a', 'cid_d', 'cid_rep']

        # Merging all skc-org couples with sales weights
        cid = pd.DataFrame(index=po[po['is_store'] == 1].index.drop_duplicates())
        cid = cid.join(sales_we_p[['sum_we_a', 'sum_we_d']])
        cid.rename(columns={'sum_we_a': 'sum_we_p_a', 'sum_we_d': 'sum_we_p_d'},
                   inplace=True)
        cid = cid.join(sales_we_s[['sum_we_a', 'sum_we_d']])
        cid.rename(columns={'sum_we_a': 'sum_we_s_a', 'sum_we_d': 'sum_we_s_d'},
                   inplace=True)
        cid = cid.join(sales_we_all['sum_we_a'])
        cid.rename(columns={'sum_we_a': 'sum_we_all_a'}, inplace=True)
        cid.reset_index(inplace=True)
        cid.fillna({'sum_we_p_a': sales_we_p['sum_we_a'].min(),
                    'sum_we_p_d': sales_we_p['sum_we_d'].max(),
                    'sum_we_s_a': sales_we_s['sum_we_a'].min(),
                    'sum_we_s_d': sales_we_s['sum_we_d'].max(),
                    'sum_we_all_a': sales_we_all['sum_we_a'].min()}, inplace=True)
        cid['sum_we_a'] = cid['sum_we_p_a'] * spr_we + cid['sum_we_s_a'] * (1 - spr_we)
        cid['sum_we_d'] = cid['sum_we_p_d'] * spr_we + cid['sum_we_s_d'] * (1 - spr_we)

        # Calculating cost
        cid['cid_a'] = cid_base * cid['sum_we_a']
        cid['cid_d'] = cid_base * cid['sum_we_d']
        cid['cid_rep'] = cid_base * cid['sum_we_all_a']
        if not itp.empty:
            cid.set_index(cols_grp, inplace=True)
            cid = cid.join(itp).reset_index().fillna({'itp': -1})
            cid.loc[cid['itp'] == 0, 'cid_a'] = 0
            cid.loc[cid['itp'] == 0, 'cid_d'] = cid_base
            cid.loc[cid['itp'] == 0, 'cid_rep'] = 0

        cid = cid[cols_grp + cols_grp2].set_index(cols_grp)

        return cid

    def cal_max_inv_diff_cost(self, pi, sales_we_p, sales_we_s, sales_we_all, spr_we, cidx_base):
        """
        Calculating unit cost of the maximal inventory difference

        Parameters
        ----------
        pi : pd.DataFrame
             Product information
             index : prod_id, color_id
             columns : size, size_order, year, season_id, class_0

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

        spr_we : float
                 Weight of sales being prior

        cidx_base : float
                    Basic unit cost of the maximal inventory difference

        Returns
        -------
        cidx : pd.DataFrame
               Unit cost of the maximal inventory difference
               index : prod_id, color_id, size
               columns : cidx_a, cidx_d, cidx_rep
        """

        cols_grp = ['prod_id', 'color_id', 'size']
        cols_grp2 = ['cidx_a', 'cidx_d', 'cidx_rep']

        # Calculating the mean sales weight of each skc/size
        sks_we_p = sales_we_p[['sks_we_a', 'sks_we_d']].reset_index(cols_grp)
        sks_we_p = sks_we_p.groupby(cols_grp).mean()
        sks_we_s = sales_we_s[['sks_we_a', 'sks_we_d']].reset_index(cols_grp)
        sks_we_s = sks_we_s.groupby(cols_grp).mean()
        sks_we_all = sales_we_all['sks_we_a'].reset_index(cols_grp)
        sks_we_all = sks_we_all.groupby(cols_grp).mean()

        # Merging all products with sales weights
        cidx = pi['size'].reset_index().set_index(cols_grp)
        cidx = cidx.join(sks_we_p).rename(columns={'sks_we_a': 'sks_we_p_a',
                                                   'sks_we_d': 'sks_we_p_d'})
        cidx = cidx.join(sks_we_s).rename(columns={'sks_we_a': 'sks_we_s_a',
                                                   'sks_we_d': 'sks_we_s_d'})
        cidx = cidx.join(sks_we_all).rename(columns={'sks_we_a': 'sks_we_all_a'})
        cidx.reset_index(inplace=True)
        cidx.fillna({'sks_we_p_a': sks_we_p['sks_we_a'].min(),
                     'sks_we_p_d': sks_we_p['sks_we_d'].max(),
                     'sks_we_s_a': sks_we_s['sks_we_a'].min(),
                     'sks_we_s_d': sks_we_s['sks_we_d'].max(),
                     'sks_we_all_a': sks_we_all['sks_we_a'].min()}, inplace=True)
        cidx['sks_we_a'] = (cidx['sks_we_p_a'] * spr_we +
                            cidx['sks_we_s_a'] * (1 - spr_we))
        cidx['sks_we_d'] = (cidx['sks_we_p_d'] * spr_we +
                            cidx['sks_we_s_d'] * (1 - spr_we))

        # Calculating cost
        cidx['cidx_a'] = cidx_base * cidx['sks_we_a']
        cidx['cidx_d'] = cidx_base * cidx['sks_we_d']
        cidx['cidx_rep'] = cidx_base * cidx['sks_we_all_a']

        cidx = cidx[cols_grp + cols_grp2].set_index(cols_grp)

        return cidx

    def cal_dem_loss_cost(self, po, itp, sales_we_p, sales_we_s, spr_we, cdl_base):
        """
        Calculating unit cost of demand lossing

        Parameters
        ----------
        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        itp : pd.DataFrame
              Target inventory of the previous day
              index : prod_id, color_id, size, org_id
              columns : itp

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

        spr_we : float
                 Weight of sales being prior

        cdl_base : float
                   Basic unit cost of demand lossing

        Returns
        -------
        cdl : pd.DataFrame
              Unit cost of demand lossing
              index : prod_id, color_id, size, org_id
              columns : cdl_lb, cdl_ub
        """

        cols_grp = ['prod_id', 'color_id', 'size', 'org_id']
        cols_grp2 = ['cdl_lb', 'cdl_ub']

        # Merging all skc-org couples with sales weights
        cdl = pd.DataFrame(index=po[po['is_store'] == 1].index.drop_duplicates())

        cdl = cdl.join(sales_we_p[['sum_we_a', 'sum_we_d']])
        cdl.rename(columns={'sum_we_a': 'sum_we_p_a', 'sum_we_d': 'sum_we_p_d'},
                   inplace=True)
        cdl = cdl.join(sales_we_s[['sum_we_a', 'sum_we_d']])
        cdl.rename(columns={'sum_we_a': 'sum_we_s_a', 'sum_we_d': 'sum_we_s_d'},
                   inplace=True)
        cdl.reset_index(inplace=True)
        cdl.fillna({'sum_we_p_a': sales_we_p['sum_we_a'].min(),
                    'sum_we_p_d': sales_we_p['sum_we_d'].max(),
                    'sum_we_s_a': sales_we_s['sum_we_a'].min(),
                    'sum_we_s_d': sales_we_s['sum_we_d'].max()}, inplace=True)
        cdl['sum_we_a'] = cdl['sum_we_p_a'] * spr_we + cdl['sum_we_s_a'] * (1 - spr_we)
        cdl['sum_we_d'] = cdl['sum_we_p_d'] * spr_we + cdl['sum_we_s_d'] * (1 - spr_we)

        # Calculating cost
        cdl['cdl_lb'] = 2 * cdl_base * cdl['sum_we_a']
        cdl['cdl_ub'] = cdl_base * cdl['sum_we_d']
        if not itp.empty:
            cdl.set_index(cols_grp, inplace=True)
            cdl = cdl.join(itp).reset_index().fillna({'itp': -1})
            cdl.loc[cdl['itp'] == 0, 'cdl_lb'] = 0
            cdl.loc[cdl['itp'] == 0, 'cdl_ub'] = cdl_base

        cdl = cdl[cols_grp + cols_grp2].set_index(cols_grp)

        return cdl

    def cal_shortsize_cost(self, pi, ib_we, css_base):
        """
        Calculating unit cost of shortsize

        Parameters
        ----------
        pi : pd.DataFrame
             Product information
             index : prod_id, color_id
             columns : size, size_order, year, season_id, class_0

        ib_we : pd.DataFrame
                Inventory balance weights
                index : prod_id, color_id
                columns : ib_we

        css_base : list of float
                   Basic unit cost lower- and upper- bounds of shortsize

        Returns
        -------
        css : pd.DataFrame
              Unit cost of shortsize
              index : prod_id, color_id
              columns : css
        """

        css = pd.DataFrame(index=pi.index.drop_duplicates())
        css = css.join(ib_we).fillna({'ib_we': ib_we['ib_we'].max()})
        we_max, we_min = ib_we['ib_we'].max(), ib_we['ib_we'].min()
        we_diff = we_max - we_min + 1.0E-10
        css_diff = css_base[1] - css_base[0]
        css['css'] = css_base[0] + css_diff / we_diff * (css['ib_we'] - we_min)
        css.drop('ib_we', axis=1, inplace=True)

        return css

    def cal_cost_params(self, pi, oi, po, itp, io, sales_we_p, sales_we_s, sales_we_all,
                        sr_we, lt_we, ib_we, spr_we, cmq_base_ws, cmq_base_ss,
                        cmq_base_sw, cmp_base_ws, cmp_base_ss, cmp_base_sw,
                        cmt_base_ss, cid_base, cidx_base, cdl_base, css_base):
        """
        Calculating optimization cost parameters

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

        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        itp : pd.DataFrame
              Target inventory of the previous day
              index : prod_id, color_id, size, org_id
              columns : itp

        io : pd.DataFrame
             Markers of existed moving -in/-out
             index : prod_id, color_id, size, org_id
             columns : has_in, has_out

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

        lt_we : pd.DataFrame
                Moving leading-time weights between cities
                index : city_send_id, city_rec_id
                columns : lt_we

        ib_we : pd.DataFrame
                Inventory balance weights
                index : prod_id, color_id
                columns : ib_we

        spr_we : float
                 Weight of sales being prior

        cmq_base_ws : float
                      Basic unit cost of moving quantity from warehouse to store

        cmq_base_ss : float
                      Basic unit cost of moving quantity between stores

        cmq_base_sw : float
                      Basic unit cost of moving quantity from store to warehouse

        cmp_base_ws : float
                      Basic unit cost of moving packages from warehouse to store

        cmp_base_ss : float
                      Basic unit cost of moving packages between stores

        cmp_base_sw : float
                      Basic unit cost of moving packages from store to warehouse

        cmt_base_ss : float
                      Basic unit cost of moving time between stores

        cid_base : float
                   Basic unit cost of inventory difference

        cidx_base : float
                    Basic unit cost of the maximal inventory difference

        cdl_base : float
                   Basic unit cost of demand lossing

        css_base : list of float
                   Basic unit cost lower- and upper- bounds of shortsize

        Returns
        -------
        cmq : pd.DataFrame
              Unit cost of moving quantity
              index : prod_id, color_id, size, org_id
              columns : cmq_rep, cmq_trans_in, cmq_trans_out, cmq_ret

        cmp : pd.DataFrame
              Unit cost of moving packages
              key : org_send_id, org_rec_id
              value : cmp

        cmt : pd.DataFrame
              Unit cost of moving time
              key : org_send_id, org_rec_id
              value : cmt

        cid : pd.DataFrame
              Unit cost of inventory difference
              index : prod_id, color_id, size, org_id
              columns : cid_a, cid_d, cid_rep

        cidx : pd.DataFrame
               Unit cost of the maximal inventory difference
               index : prod_id, color_id, size
               columns : cidx_a, cidx_d, cidx_rep

        cdl : pd.DataFrame
              Unit cost of demand lossing
              index : prod_id, color_id, size, org_id
              columns : cdl_lb, cdl_ub

        css : pd.DataFrame
              Unit cost of shortsize
              index : prod_id, color_id
              columns : css
        """

        # Calculating unit cost of moving quantity
        cmq = self.cal_mov_quant_cost(po, io, itp, sales_we_p, sales_we_s, spr_we,
                                      cmq_base_ws, cmq_base_ss, cmq_base_sw)

        # Calculating unit cost of moving package
        cmp = self.cal_mov_pkg_cost(oi, sr_we, spr_we, cmp_base_ws, cmp_base_ss,
                                    cmp_base_sw)

        # Calculating unit cost of moving time
        cmt = self.cal_mov_time_cost(oi, lt_we, cmt_base_ss)

        # Calculating unit cost of inventory difference
        cid = self.cal_inv_diff_cost(po, itp, sales_we_p, sales_we_s, sales_we_all,
                                     spr_we, cid_base)

        # Calculating unit cost of the maximal inventory difference
        cidx = self.cal_max_inv_diff_cost(pi, sales_we_p, sales_we_s, sales_we_all,
                                          spr_we, cidx_base)

        # Calculating unit cost of demand lossing
        cdl = self.cal_dem_loss_cost(po, itp, sales_we_p, sales_we_s, spr_we, cdl_base)

        # Calculating unit cost of shortsize
        css = self.cal_shortsize_cost(pi, ib_we, css_base)

        return cmq, cmp, cmt, cid, cidx, cdl, css
