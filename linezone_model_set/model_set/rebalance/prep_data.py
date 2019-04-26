# -*- coding: utf-8 -*-

import datetime as dt

import pandas as pd

from check.TypeAssert import typeassert


class DataPrepare(object):
    def __init__(self):
        pass

    @typeassert(pi=pd.DataFrame, i0=pd.DataFrame, itp=pd.DataFrame, mss=pd.DataFrame)
    def proc_prod_info(self, pi, i0, itp, mss):
        """
        Processing product information

        Parameters
        ----------
        pi : pd.DataFrame
             Product information
             index : prod_id, color_id
             columns : size, size_order, year, season_id, class_0

        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r

        s : pd.DataFrame
            Weekly sales data
            index : prod_id, color_id, size, org_id, week_no
            columns : s

        itp : pd.DataFrame
              Target inventory of the previous day
              index : prod_id, color_id, size, org_id
              columns : itp

        mss : pd.DataFrame
              Special moving states
              index : prod_id, color_id, org_id
              columns : not_in(0), not_out(0), to_emp(0)

        Returns
        -------
        pi_p : pd.DataFrame
               Product information after precessed
               index : prod_id, color_id
               columns : size, size_order, year, season_id, class_0
        """

        cols_grp = ['prod_id', 'color_id', 'size']

        # Calculating the maximal initial inventory of each skc/size, and selecting
        # skc/size with positive initial inventory to be valid
        i0_max = i0['i0'].reset_index(cols_grp).groupby(cols_grp).max()
        sks_valid = i0_max[i0_max['i0'] > 0].index.unique()

        # Filtering products with the maximal initial inventory of each skc/size
        # being positive
        pi_p = pi.reset_index().set_index(cols_grp)
        pi_p = pi_p.loc[pi_p.index.isin(sks_valid)].reset_index()
        pi_p.set_index(cols_grp[:2], inplace=True)

        # Filtering products with valid target inventory of the previous day if
        # required
        if not itp.empty:
            skc_valid = itp.reset_index().set_index(cols_grp[:2]).index.unique()
            pi_p = pi_p.loc[pi_p.index.isin(skc_valid)].copy()

        # Filtering products with those being admitted to move if required
        if not mss.empty:
            mss_skc = mss.reset_index(cols_grp[:2]).groupby(cols_grp[:2]).prod()
            mss_skc['not_io'] = mss_skc[['not_in', 'not_out']].prod(axis=1)
            skc_invalid = mss_skc[mss_skc['not_io'] > 0].index.unique()
            pi_p = pi_p.loc[~(pi_p.index.isin(skc_invalid))].copy()

        # ----------------------------- test -----------------------------
        # # Calculating sum sales of each skc
        # s_skc = s.reset_index(cols_grp[:2]).groupby(cols_grp[:2]).sum()
        #
        # # Sorting skc by sales
        # pi_p = pi_p.join(s_skc).fillna(0).reset_index()
        # pi_p.sort_values(['season_id', 's', 'prod_id', 'color_id', 'size_order'],
        #                  ascending=[True, False, True, True, True], inplace=True)
        # pi_p.drop('s', axis=1, inplace=True)
        # pi_p.set_index(cols_grp[:2], inplace=True)

        # setting skc num for test
        # pi_p = pi_p.loc[pi_p.index.isin(pi_p.index.unique()[:20])].copy()
        # ----------------------------- end -----------------------------

        return pi_p

    @typeassert(i0=pd.DataFrame)
    def proc_neg_inv(self, i0):
        """
        Processing negative initial inventory

        Parameters
        ----------
        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r

        Returns
        -------
        i0_p : pd.DataFrame
               Initial inventory data after processed
               index : prod_id, color_id, size, org_id
               columns : i0, r
        """

        cols_grp = ['prod_id', 'color_id', 'org_id']
        cols_grp2 = ['prod_id', 'color_id', 'size', 'org_id']

        # If sum initial inventory of a skc/org is 0, setting inventory of each
        # size of this skc/org to be 0
        i0_p = i0.reset_index('size')
        i0_p['i0_skcs'] = i0_p['i0'].sum(level=cols_grp)
        i0_p = i0_p.reset_index().set_index(cols_grp2)
        i0_p.loc[i0_p['i0_skcs'] == 0, 'i0'] = 0

        # Setting all negative inventory to be 0
        i0_p.loc[i0_p['i0'] <= 0, 'i0'] = 0
        i0_p.loc[i0_p['r'] <= 0, 'r'] = 0

        i0_p.drop('i0_skcs', axis=1, inplace=True)

        return i0_p

    @typeassert(oi=pd.DataFrame, i0=pd.DataFrame, s=pd.DataFrame, itp=pd.DataFrame, mpa=pd.DataFrame, mss=pd.DataFrame)
    def proc_org_info(self, oi, i0, s, itp, mpa, mss):
        """
        Processing organization information

        Parameters
        ----------
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

        Returns
        -------
        oi_p : pd.DataFrame
               Organization basic information after processed
               index : org_id
               columns : prov_id, city_id, dist_id, mng_reg_id, is_store(1)
        """

        # Calculating sum initial inventory of each store, and selecting those
        # with positive initial inventory to be valid
        i0_org = i0.reset_index('org_id').groupby('org_id').sum()
        i0_org['i0_sum'] = i0_org[['i0', 'r']].sum(axis=1)
        org_valid_i0 = i0_org[i0_org['i0_sum'] > 0].index.unique()

        # Calculating sum sales of the previous week of each store, and selecting
        # those with positive sales to be valid
        s_org = s.reset_index('week_no')
        s_org = s_org.loc[s_org['week_no'] == -1, 's'].copy()
        s_org = s_org.reset_index('org_id').groupby('org_id').sum()
        org_valid_s = s_org[s_org['s'] > 0].index.unique()

        # Filtering valid stores with either sum initial inventory or sum sales of
        # the previous week being positive
        org_valid = set(org_valid_i0) | set(org_valid_s)

        # Filtering valid stores with valid target inventory of the previous day
        # if required
        if not itp.empty:
            org_valid_itp = itp.reset_index().set_index('org_id').index.unique()
            org_valid = org_valid & set(org_valid_itp)

        # Filtering valid stores with those being decided to move if required
        if not mpa.empty:
            mpa_org = mpa.reset_index().groupby('org_id').sum()
            mpa_org['is_dec'] = mpa_org.sum(axis=1)
            org_valid_mpa = mpa_org[mpa_org['is_dec'] > 0].index.unique()
            org_valid = org_valid & set(org_valid_mpa)

        # Filtering valid stores with those being admitted to move if required
        if not mss.empty:
            mss_org = mss.reset_index('org_id').groupby('org_id').prod()
            mss_org['not_io'] = mss_org[['not_in', 'not_out']].prod(axis=1)
            org_invalid_mss = mss_org[mss_org['not_io'] > 0].index.unique()
            org_valid = org_valid - set(org_invalid_mss)

        oi_p = oi.loc[(oi['is_store'] != 1) | (oi.index.isin(org_valid))].copy()
        oi_p['dist_id'].fillna('china', inplace=True)
        oi_p['mng_reg_id'].fillna('china', inplace=True)

        return oi_p

    @typeassert(pi=pd.DataFrame, oi=pd.DataFrame, di=pd.DataFrame, i0=pd.DataFrame, s=pd.DataFrame, itp=pd.DataFrame,
                mpa=pd.DataFrame, mss=pd.DataFrame)
    def cross_prod_org(self, pi, oi, di, i0, s, itp, mpa, mss):
        """
        Crossing product and organization information

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

        di : pd.DataFrame
             Display information
             index : org_id, season_id
             columns : qs_min(0), qd_min(0)

        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r

        s : pd.DataFrame
            Weekly sales data
            index : prod_id, color_id, size, org_id, week_no
            columns : s

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

        Returns
        -------
        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp
        """

        def _cross_pi_oi(pi, oi):
            pi_c = pi.reset_index()
            pi_c['col_on'] = 1
            pi_c.set_index('col_on', inplace=True)

            oi_c = oi.reset_index()
            oi_c['col_on'] = 1
            oi_c.set_index('col_on', inplace=True)

            # Crossing product and organization information
            po = pi_c.join(oi_c)

            return po

        cols_grp = ['prod_id', 'color_id', 'size', 'org_id']
        cols_grp2 = ['prod_id', 'color_id', 'org_id']
        cols_grp3 = ['org_id', 'season_id']

        # Crossing product and organization information
        po = _cross_pi_oi(pi, oi)
        po['qs_min'] = 0  # the minimal sum inventory of each store
        po['qd_min'] = 0  # the minimal skc number of each store
        po['is_new'] = 0  # whether or not being a new couple
        po['is_rep'] = 1  # whether or not being permitted to replenish
        po['is_trans'] = 1  # whether or not being permitted to transfer
        po['is_ret'] = 0  # whether or not being permitted to return
        po['not_in'] = 0  # whether or not being restricted to move-in
        po['not_out'] = 0  # whether or not being restricted to move-out
        po['to_emp'] = 0  # whether or not being required to empty
        po.loc[po['is_store'] == 0, 'is_rep'] = 1  # Replenishment warehouse
        po.loc[po['is_store'] == -1, 'is_ret'] = 1  # Return warehouse

        # Calculating sum initial inventory of each skc/size/org, and selecting
        # those with positive sum initial inventory to be valid
        po_valid_i0 = i0[i0[['i0', 'r']].fillna(0).sum(axis=1) > 0].index.unique()

        # Calculating the maximal sales of each skc/size/org of the previous 2
        # weeks, and selecting those with positive sales to be valid
        sp = s.reset_index('week_no')
        sp = sp.loc[(sp['week_no'] >= -2) &
                    (sp['week_no'] <= -1), ['s']].reset_index()
        sp = sp.groupby(cols_grp).max()
        po_valid_s = sp[sp['s'] > 0].index.unique()

        # Filtering valid skc/size-org couples with either sum initial inventory or
        # the maximal sales of the previous 2 weeks being positive
        po_valid = set(po_valid_i0) | set(po_valid_s)

        # Filtering valid skc/size-org couples with valid target inventory of the
        # previous day if required
        if not itp.empty:
            po_valid_itp_v = itp.index.unique()
            po_valid_itp_p = itp[itp['itp'] > 0].index.unique()
            po_valid = (po_valid | set(po_valid_itp_p)) & po_valid_itp_v

        # Selecting valid skc/size-org couples with any of sum initial inventory,
        # the maximal sales of the previous 2 weeks, or target inventory of the
        # previous day being positive
        po.set_index(cols_grp, inplace=True)
        po = po.loc[(po['is_store'] != 1) | (po.index.isin(po_valid))].copy()
        po.reset_index(inplace=True)

        # Filtering valid skc/size-org couples with stores being decided to move if
        # required
        if not mpa.empty:
            po.set_index('org_id', inplace=True)
            po.update(mpa.reset_index().groupby('org_id').sum())
            po.reset_index(inplace=True)
            po['is_dec'] = po[['is_rep', 'is_trans', 'is_ret']].sum(axis=1)
            po = po.loc[(po['is_store'] != 1) | (po['is_dec'] > 0)].copy()
            po.drop('is_dec', axis=1, inplace=True)

        # Filtering valid skc/size-org couples with skc/stores being permitted to
        # move if required
        if not mss.empty:
            po.set_index(cols_grp2, inplace=True)
            po.update(mss.reset_index().groupby(cols_grp2).prod())
            po.reset_index(inplace=True)
            po['not_io'] = po[['not_in', 'not_out']].prod(axis=1)
            po = po.loc[(po['is_store'] != 1) | (po['not_io'] == 0)].copy()
            po.drop('not_io', axis=1, inplace=True)

        # Marking skc/size-org couples to be newly distributed if both sum initial
        # inventory and the maximal sales of the previous 2 weeks being 0 but
        # target inventory of the previous day being positive
        if not itp.empty:
            po_invalid_i0 = \
                i0[i0[['i0', 'r']].fillna(0).sum(axis=1) <= 0].index.unique()
            po_invalid_s = sp[sp['s'] <= 0].index.unique()
            po_valid_itp_p = itp[itp['itp'] > 0].index.unique()
            po_new = set(po_invalid_i0) & set(po_invalid_s) & set(po_valid_itp_p)
            po.set_index(cols_grp, inplace=True)
            po.loc[po.index.isin(po_new), 'is_new'] = 1
            po.reset_index(inplace=True)

        # Merging with display information
        if not di.empty:
            po.set_index(cols_grp3, inplace=True)
            po.update(di.reset_index().groupby(cols_grp3).sum())
            po.reset_index(inplace=True)

        po = po.set_index(cols_grp).sort_index()

        return po

    @typeassert(po=pd.DataFrame, i0=pd.DataFrame)
    def fill_init_inv(self, po, i0):
        """
        Filling initial inventory data

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
             columns : i0, r

        Returns
        -------
        i0_f : pd.DataFrame
               Initial inventory data after filled
               index : prod_id, color_id, size, org_id
               columns : i0, r, i0_sum
        """

        # Filling initial inventory data
        i0_f = pd.DataFrame(index=po.index.drop_duplicates()).join(i0).fillna(0)
        i0_f['i0_sum'] = i0_f[['i0', 'r']].sum(axis=1)

        return i0_f

    @typeassert(po=pd.DataFrame, s=pd.DataFrame, per_sales=int)
    def fill_sales_data(self, po, s, per_sales):
        """
        Filling sales data

        Parameters
        ----------
        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        s : pd.DataFrame
            Weekly sales data
            index : prod_id, color_id, size, org_id, week_no
            columns : s

        per_sales : int
                    Period of sales data (weeks)

        Returns
        -------
        s_f : pd.DataFrame
              Full weekly sales data
              index : prod_id, color_id, size, org_id, week_no
              columns : s
        """

        cols_grp = ['prod_id', 'color_id', 'size', 'org_id', 'week_no']

        # Crossing po and dates
        idx_sel = (po['is_store'] == 1) & (po['is_new'] == 0)
        s_f = pd.DataFrame(index=po[idx_sel].index.drop_duplicates()).reset_index()
        s_f['col_on'] = 1
        s_f.set_index('col_on', inplace=True)

        week_no = pd.DataFrame(list(range(-per_sales, 0)), columns=['week_no'])
        week_no['col_on'] = 1
        week_no.set_index('col_on', inplace=True)

        s_f = s_f.join(week_no)

        # Merging and filling sales
        s_f = s_f.set_index(cols_grp).join(s).fillna({'s': 0})

        return s_f

    @typeassert(po=pd.DataFrame, mv=pd.DataFrame, date_dec=dt.date, prt_per_in=int)
    def mark_io(self, po, mv, date_dec, prt_per_in):
        """
        Marking out moving -in/-out of each skc/size/store

        Parameters
        ----------
        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        mv : pd.DataFrame
             Moving data
             index : prod_id, color_id, size
             columns : org_send_id, org_rec_id, date_send, date_rec, qty_send,
                       qty_rec

        date_dec : date
                   Decision making date

        prt_per_in : int
                     Moving in protecting period

        Returns
        -------
        io : pd.DataFrame
             Markers of existed moving -in/-out
             index : prod_id, color_id, size, org_id
             columns : has_in, has_out
        """

        cols_grp = ['prod_id', 'color_id', 'size', 'org_id']

        io = pd.DataFrame(index=po[po['is_store'] == 1].index.drop_duplicates())
        io['has_in'] = 0
        io['has_out'] = 0

        if mv.empty:
            return io

        date_cut = pd.to_datetime(date_dec - dt.timedelta(days=prt_per_in))
        # Calculating moving-in quantity by sending in the moving-in protecting
        # period
        mv_in_s = \
            mv.loc[mv['date_send'] >= date_cut, ['org_rec_id', 'qty_send']] \
                .reset_index().groupby(cols_grp[:3] + ['org_rec_id']).sum()
        # Calculating moving-in quantity by receiving in the moving-in protecting
        # period
        mv_in_r = \
            mv.loc[mv['date_rec'] >= date_cut, ['org_rec_id', 'qty_rec']] \
                .reset_index().groupby(cols_grp[:3] + ['org_rec_id']).sum()
        # Summing all moving-in quantity
        mv_in = mv_in_s.join(mv_in_r, how='outer').fillna(0).rename_axis(cols_grp)
        mv_in['qty_in'] = mv_in[['qty_send', 'qty_rec']].sum(axis=1)

        # Marking out skc/size/org with positive moving-in quantity
        io = io.join(mv_in['qty_in']).fillna({'qty_in': 0})
        io.loc[io['qty_in'] > 0, 'has_in'] = 1
        io.drop('qty_in', axis=1, inplace=True)

        return io

    @typeassert(pi=pd.DataFrame, oi=pd.DataFrame, di=pd.DataFrame, i0=pd.DataFrame, s=pd.DataFrame, mv=pd.DataFrame,
                itp=pd.DataFrame, mpa=pd.DataFrame, mss=pd.DataFrame, date_dec=dt.date, prt_per_in=int)
    def prep_data(self, pi, oi, di, i0, s, mv, itp, mpa, mss, date_dec, prt_per_in,
                  per_sales):
        """
        Pre-processing data

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

        date_dec : date
                   Decision making date

        prt_per_in : int
                     Moving in protecting period

        per_sales : int
                    Period of sales data (weeks)

        Returns
        -------
        pi_p : pd.DataFrame
               Product information after precessed
               index : prod_id, color_id
               columns : size, size_order, year, season_id, class_0

        oi_p : pd.DataFrame
               Organization basic information after processed
               index : org_id
               columns : prov_id, city_id, dist_id, mng_reg_id, is_store(1)

        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        i0_f : pd.DataFrame
               Initial inventory data after filled
               index : prod_id, color_id, size, org_id
               columns : i0, r, i0_sum

        s_f : pd.DataFrame
              Sales data after filled
              index : prod_id, color_id, size, org_id, week_no
              columns : s

        io : pd.DataFrame
             Markers of existed moving -in/-out
             index : prod_id, color_id, size, org_id
             columns : has_in, has_out
        """

        # Processing negative initial inventory
        i0_p = self.proc_neg_inv(i0)

        # Processing product information
        pi_p = self.proc_prod_info(pi, i0_p, itp, mss)
        if pi_p.empty:
            raise Exception('No valid products')

        # Processing organization information
        oi_p = self.proc_org_info(oi, i0_p, s, itp, mpa, mss)
        if oi_p.empty:
            raise Exception('No valid organizations')

        # Crossing products and organizations
        po = self.cross_prod_org(pi_p, oi_p, di, i0_p, s, itp, mpa, mss)
        if po.empty:
            raise Exception('No valid product-organization couples')

        # Filling initial inventory data
        i0_f = self.fill_init_inv(po, i0_p)

        # Filling sales data
        s_f = self.fill_sales_data(po, s, per_sales)

        # Marking out moving -in/-out of each skc/size/store
        io = self.mark_io(po, mv, date_dec, prt_per_in)

        return pi_p, oi_p, po, i0_f, s_f, io


if __name__ == '__main__':
    pass
