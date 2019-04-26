# -*- coding: utf-8 -*-

import math

import pandas as pd

from check.TypeAssert import typeassert
from model_set.rebalance.postp_data import DataProcess
from model_set.rebalance.rep_opt_solver import ReplenishOptimization
from model_set.rebalance.trans_opt_solver_core import TransferOptimization
from utils.logger import lz_logger


class Optimization(object):
    def __init__(self):
        self.ros = ReplenishOptimization()
        self.psp = DataProcess()
        self.tos = TransferOptimization()

    @typeassert(po_rep=pd.DataFrame, i0=pd.DataFrame, it=pd.DataFrame, cmp=pd.DataFrame, cid=pd.DataFrame,
                cidx=pd.DataFrame)
    def exec_rep(self, po_rep, i0, it, cmp, cid, cidx):
        """
        Executing replenishment

        Parameters
        ----------
        po_rep : pd.DataFrame
                 Target products and organizations of replenishment
                 index : prod_id, color_id, size, org_id
                 columns : to_in, to_out

        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r, i0_sum

        it : pd.DataFrame
             Target inventory
             index : prod_id, color_id, size, org_id
             columns : it, is_valid

        cmp : pd.DataFrame
              Unit cost of moving packages
              key : org_send_id, org_rec_id
              value : cmp

        cid : pd.DataFrame
              Unit cost of inventory difference
              index : prod_id, color_id, size, org_id
              columns : cid_a, cid_d, cid_rep

        cidx : pd.DataFrame
               Unit cost of the maximal inventory difference
               index : prod_id, color_id, size
               columns : cidx_a, cidx_d, cidx_rep

        Returns
        -------
        q_rep : pd.DataFrame
                Replenishment quantity of each sks from warehousrs to stores
                index : prod_id, color_id, size, org_send_id, org_rec_id
                columns : quant_mov

        cmp : pd.DataFrame
              Unit cost of moving packages
              key : org_send_id, org_rec_id
              value : cmp
        """

        cols_grp = ['prod_id', 'color_id', 'size', 'org_id']

        org_targ = po_rep.index.get_level_values(cols_grp[-1]).unique()
        store_num_sum = org_targ.shape[0]
        lz_logger.info('>>>>> Sum org number:{sum_org_num}'.format(sum_org_num=store_num_sum))
        skc_targ = po_rep.reset_index(cols_grp[2:]).index.unique()
        skc_num_sum = skc_targ.shape[0]
        lz_logger.info('>>>>> Sum SKC number:{sum_skc_num}'.format(sum_skc_num=skc_num_sum))

        q_rep = pd.DataFrame()
        skc_num_grp = 100
        skc_num = 0

        for i in range(math.ceil(skc_num_sum / skc_num_grp)):
            i_start = i * skc_num_grp
            i_end = (i + 1) * skc_num_grp
            skc_grp = skc_targ[i_start:i_end]
            lz_logger.info('Grouped product number:{skc_grp_len}'.format(skc_grp_len=len(skc_grp)))

            po_grp = po_rep.reset_index().set_index(cols_grp[:2])
            po_grp = po_grp.loc[po_grp.index.isin(skc_grp)].copy()
            po_grp = po_grp.reset_index().set_index(cols_grp)

            # Replenishment optimization
            q_grp = {}
            if not po_grp.empty:
                q_grp = self.ros.rep_opt_solver(po_grp, i0, it, cmp, cid, cidx)

            # Updating moving package unit cost
            q_grp_df, cmp = self.psp.update_cmp(q_grp, cmp)
            q_rep = q_rep.append(q_grp_df)

            skc_num += len(skc_grp)
            lz_logger.info('{0}% ({1}) products have been processed.'
                           .format(round(skc_num / skc_num_sum * 100, 1), skc_num))
            lz_logger.info('Cumulative replenishment quantity:{cum_rep_qty}'.format(cum_rep_qty
                                                                                    =q_rep.values.sum()))

        return q_rep, cmp

    @typeassert(po=pd.DataFrame, po_trans=pd.DataFrame, ms=pd.DataFrame, i0=pd.DataFrame, io=pd.DataFrame,
                it=pd.DataFrame, qsf=pd.DataFrame, qmp=pd.DataFrame, cmq=pd.DataFrame, cmp=pd.DataFrame,
                cmt=pd.DataFrame, cid=pd.DataFrame, cidx=pd.DataFrame, css=pd.DataFrame)
    def exec_trans_unit(self, po, po_trans, ms, i0, io, it, qsf, qmp, cmq, cmp, cmt, cid,
                        cidx, css, mng_reg_id_sel):
        cols_grp = ['prod_id', 'color_id', 'size', 'org_id']

        lz_logger.info('>>>>> MNG_REG:{mng_reg_id}'.format(mng_reg_id=mng_reg_id_sel))
        q_trans = pd.DataFrame()
        po_reg = po_trans.loc[(po_trans['is_store'] == -1) |
                              (po_trans['mng_reg_id'] == mng_reg_id_sel),
                              ['season_id', 'is_store', 'is_new',
                               'to_in', 'to_out', 'to_emp']].copy()

        # Splitting package unit cost of each region for ease of parallelization
        org_out = po_reg[po_reg['to_out'] == 1].index \
            .get_level_values('org_id').unique()
        org_in = po_reg[po_reg['to_in'] == 1].index \
            .get_level_values('org_id').unique()
        org_crs = pd.MultiIndex.from_product([org_out, org_in], names=['org_send_id', 'org_rec_id'])
        cmp_reg = cmp.loc[cmp.index.isin(org_crs)].copy()

        store_num_reg = po_reg[po_reg['is_store'] == 1].index \
            .get_level_values(cols_grp[-1]).unique().shape[0]
        lz_logger.info('>>>>> Region store number:{store_num_reg}'.format(store_num_reg=store_num_reg))
        skc_lst = po_reg.reset_index(cols_grp[2:]).index.unique()
        skc_num_sum_reg = skc_lst.shape[0]
        lz_logger.info('>>>>> Region SKC number:{region_skc_num}'.format(region_skc_num=skc_num_sum_reg))

        skc_num_grp = 30
        skc_num = 0
        for i in range(math.ceil(skc_num_sum_reg / skc_num_grp)):
            i_start = i * skc_num_grp
            i_end = (i + 1) * skc_num_grp
            skc_grp = skc_lst[i_start:i_end]
            lz_logger.info('Grouped product number:{len_skc_grp}'.format(len_skc_grp=len(skc_grp)))

            po_grp = po_reg.reset_index().set_index(cols_grp[:2])
            po_grp = po_grp.loc[po_grp.index.isin(skc_grp)].copy()
            po_grp['to_out_skc'] = po_grp['to_out'].sum(level=cols_grp[:2])
            po_grp = po_grp.loc[po_grp['to_out_skc'] > 0].copy()
            po_grp.drop('to_out_skc', axis=1, inplace=True)
            po_grp = po_grp.reset_index().set_index(cols_grp)

            # Calculating lower bounds of skc number and inventory after moving
            i_bd = self.psp.cal_ibd(po, po_grp, i0, q_trans)

            # Transferring optimization
            q_grp = {}
            if not po_grp.empty:
                q_grp = self.tos.trans_opt_solver(po_grp, ms, i0, io, it, qsf, i_bd,
                                                  qmp, cmq, cmp_reg, cmt, cid, cidx,
                                                  css)

            # Updating moving package unit cost
            q_grp_df, cmp_reg = self.psp.update_cmp(q_grp, cmp_reg)
            q_trans = q_trans.append(q_grp_df)

            skc_num += len(skc_grp)
            lz_logger.info('>>>>> {0}% ({1}) products have been processed.'
                           .format(round(skc_num / skc_num_sum_reg * 100, 1), skc_num))

        return q_trans, cmp_reg

    @typeassert(po=pd.DataFrame, po_trans=pd.DataFrame, ms=pd.DataFrame, i0=pd.DataFrame, io=pd.DataFrame,
                it=pd.DataFrame, qsf=pd.DataFrame, qmp=pd.DataFrame, cmq=pd.DataFrame, cmp=pd.DataFrame,
                cmt=pd.DataFrame, cid=pd.DataFrame, cidx=pd.DataFrame, css=pd.DataFrame)
    def exec_trans(self, po, po_trans, ms, i0, io, it, qsf, qmp, cmq, cmp, cmt, cid,
                   cidx, css):
        """
        Executing transferring

        Parameters
        ----------
        po : pd.DataFrame
             Crossed product and organization information
             index : prod_id, color_id, size, org_id
             columns : size_order, year, season_id, class_0, prov_id, city_id,
                       dist_id, mng_reg_id, qs_min, qd_min, is_store, is_new,
                       is_rep, is_trans, is_ret, not_in, not_out, to_emp

        po_trans : pd.DataFrame
                   Target products and organizations of transferring
                   index : prod_id, color_id, size, org_id
                   columns : season_id, mng_reg_id, is_store, is_new, to_in,
                             to_out, to_emp

        ms : pd.DataFrame
             Main sizes
             index : prod_id, color_id, org_id
             columns : size, sp_size

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

        qsf : pd.DataFrame
              The minimal continue-size length in a fullsize group
              index : prod_id, color_id, org_id
              columns : qsf

        qmp : pd.DataFrame
              Residual moving-out package number
              index : prod_id, color_id, org_id
              columns : qmp_r

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

        css : pd.DataFrame
              Unit cost of shortsize
              index : prod_id, color_id
              columns : css

        Returns
        -------
        q_trans : pd.DataFrame
                  Transferring quantity of each sks between stores
                  index : prod_id, color_id, size, org_send_id, org_rec_id
                  columns : qty_mov

        cmp : pd.DataFrame
              Unit cost of moving packages
              key : org_send_id, org_rec_id
              value : cmp
        """

        cols_stc = ['prod_id', 'color_id', 'size', 'org_id']
        cols_mov = ['prod_id', 'color_id', 'size', 'org_send_id', 'org_rec_id']

        org_targ = po_trans[po_trans['is_store'] == 1].index \
            .get_level_values(cols_stc[-1]).unique()
        store_num_sum = org_targ.shape[0]
        lz_logger.info('>>>>> Sum org number:{store_num_sum}'.format(store_num_sum=store_num_sum))
        skc_targ = po_trans.reset_index(cols_stc[2:]).index.unique()
        skc_num_sum = skc_targ.shape[0]
        lz_logger.info('>>>>> Sum SKC number:{skc_num_sum}'.format(skc_num_sum=skc_num_sum))
        reg_lst = po_trans.loc[po_trans['is_store'] == 1,
                               'mng_reg_id'].dropna().drop_duplicates()

        rst = list()
        # Creating multi-processing pool and task queue
        # pool = Pool(cpu_count())
        # rst = pool.map(partial(self.exec_trans_unit,
        #                        po, po_trans, ms, i0, io, it, qsf, qmp, cmq, cmp,
        #                        cmt, cid, cidx, css),
        #                reg_lst)
        # pool.close()
        # pool.join()
        for mng in reg_lst:
            result = self.exec_trans_unit(po, po_trans, ms, i0, io, it, qsf, qmp, cmq, cmp, cmt, cid, cidx, css, mng)
            rst.append(result)
        if len(rst) == 0:
            lz_logger.info('>>>>> Transferring outputs NULL')
            q_trans = pd.DataFrame(columns=cols_mov + ['quant_mov']).set_index(cols_mov)
            return q_trans, cmp

        # Concatenating results
        q_trans = pd.concat(item[0] for item in rst)
        cmp_list = pd.concat(item[1] for item in rst)
        cmp.update(cmp_list)

        return q_trans, cmp

    @typeassert(po=pd.DataFrame, oi=pd.DataFrame, i0=pd.DataFrame)
    def exec_ret(self, po, oi, i0):
        """
        Executing returning

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

        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r, i0_sum

        Returns
        -------
        q_ret : pd.DataFrame
                Returning quantity of each sks from stores to warehouses
                index : prod_id, color_id, size, org_send_id, org_rec_id
                columns : qty_mov
        """

        cols_mov = ['prod_id', 'color_id', 'size', 'org_send_id', 'org_rec_id']

        idx_sel = po[po['to_emp'] == 1].index.unique()
        q_ret = i0.loc[i0['i0'] > 0, ['i0']].copy()
        q_ret = q_ret.loc[q_ret.index.isin(idx_sel)].reset_index()
        q_ret.rename(columns={'org_id': 'org_send_id', 'i0': 'qty_mov'}, inplace=True)
        q_ret['org_rec_id'] = oi[oi['is_store'] == -1].index[0]
        q_ret.set_index(cols_mov, inplace=True)

        return q_ret
