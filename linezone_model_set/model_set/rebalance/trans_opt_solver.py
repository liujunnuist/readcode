# -*- coding: utf-8 -*-

import pandas as pd
import pyscipopt as slv

from check.TypeAssert import typeassert
from utils.logger import lz_logger


class TransferOptimization(object):
    def __init__(self):
        pass

    @typeassert(po=pd.DataFrame, ms=pd.DataFrame, i0=pd.DataFrame, io=pd.DataFrame, it=pd.DataFrame, qsf=pd.DataFrame,
                i_bd=pd.DataFrame, qmp=pd.DataFrame, cmq=pd.DataFrame, cmp=pd.DataFrame, cmt=pd.DataFrame,
                cid=pd.DataFrame, cidx=pd.DataFrame, css=pd.DataFrame)
    def trans_opt_solver(self, po, ms, i0, io, it, qsf, i_bd, qmp, cmq, cmp, cmt, cid,
                         cidx, css):
        """
        Transferring optimization solver

        Parameters
        ----------
        po : pd.DataFrame
             Target products and organizations
             index : prod_id, color_id, size, org_id
             columns : season_id, is_store, is_new, to_in, to_out, to_emp

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

        i_bd : pd.DataFrame
               Lower bounds of skc number and inventory after moving
               index : org_id, season_id
               columns : qs_lb, qd_lb

        qmp : pd.DataFrame
              Residual moving-out package number
              index : prod_id, color_id, org_id
              columns : qmp_r

        cmq : pd.DataFrame
              Unit cost of moving quantity
              index : prod_id, color_id, size, org_id
              columns : cmq_rep, cmq_trans_in, cmq_trans_out

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
        q : dict
            Moving quantity of each sks between organizations
            key : prod_id, color_id, size, org_send_id, org_rec_id
            value : moving quantity
        """

        # TODO: return to the original org if cannot be fullsize

        pi = po.loc[(po['is_store'] == 1) &
                    (po['to_emp'] == 0), 'season_id'].reset_index()
        pi = pi.drop('size', axis=1).drop_duplicates()
        pi.set_index(['org_id', 'season_id'], inplace=True)
        po = po.reset_index('org_id')
        sks_lst = po.index.unique()
        skc_lst = po.reset_index('size').index.unique()
        org_mo_lst = po.loc[po['to_out'] == 1, 'org_id'].drop_duplicates().values
        org_mi_lst = po.loc[po['to_in'] == 1, 'org_id'].drop_duplicates().values
        io_skcs = io.reset_index(['prod_id', 'color_id', 'org_id']) \
            .groupby(['prod_id', 'color_id', 'org_id']).sum()

        # Creating a new model
        m = slv.Model()
        m.setRealParam('limits/gap', 0.01)
        m.setRealParam('limits/time', 200)
        M = 10000

        # Creating decision variable
        q = {}
        for prod_id, color_id, size in sks_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            for org_send_id in po_sel[po_sel['to_out'] == 1].index.unique():
                for org_rec_id in po_sel[po_sel['to_in'] == 1].index.unique():
                    if org_send_id != org_rec_id:
                        idx_mov = (prod_id, color_id, size,
                                   org_send_id, org_rec_id)
                        var_name = 'q_' + '_'.join(idx_mov)
                        q[idx_mov] = m.addVar(vtype='I', name=var_name)

        # Creating assistant variables
        # i : dict
        #     Final inventory after moving of each sks/org
        #     key : prod_id, color_id, size, org_id
        #     value : final inventory
        #
        # ib : dict
        #      Whether or not final inventory is positive of each sks/org
        #      key : prod_id, color_id, size, org_id
        #      value : bool
        #
        # qis : dict
        #       Sum moving-in quantity of each sks/org
        #       key : prod_id, color_id, size, org_id
        #       value : sum moving-in quantity
        #
        # qos : dict
        #       Sum moving-out quantity of each sks/org
        #       key : prod_id, color_id, size, org_id
        #       value : sum moving-out quantity
        #
        # qib : dict
        #       Whether or not sum moving-in quantity is positive of each sks/org
        #       key : prod_id, color_id, size, org_id
        #       value : bool
        #
        # qob : dict
        #       Whether or not sum moving-out quantity is positive of each sks/org
        #       key : prod_id, color_id, size, org_id
        #       value : bool
        #
        # idg : dict
        #       Quantity of final- greater than target- inventory of each sks/org
        #       key : prod_id, color_id, size, org_id
        #       value : inventory difference
        #
        # idl : dict
        #       Quantity of final- less than target- inventory of each sks/org
        #       key : prod_id, color_id, size, org_id
        #       value : inventory difference
        #
        # idgx : dict
        #        The maximal quantity of final- greater than target- inventory of
        #        each sks
        #        key : prod_id, color_id, size
        #        value : the maximal inventory difference
        #
        # idlx : dict
        #        The maximal quantity of final- less than target- inventory of each
        #        sks
        #        key : prod_id, color_id, size
        #        value : the maximal inventory difference
        #
        # is_ : dict
        #       Final inventory after moving of each skc/org
        #       key : prod_id, color_id, org_id
        #       value : final inventory
        #
        # isb : dict
        #       Whether or not final inventory after moving is positive of each
        #       skc/org
        #       key : prod_id, color_id, org_id
        #       value : bool
        #
        # fmsb : dict
        #        Whether or not all of the main sizes are full of each skc/org
        #        key : prod_id, color_id, org_id
        #        value : bool
        #
        # fsb : dict
        #       Whether or not any of the main size groups is full of each skc/org
        #       key : prod_id, color_id, org_id
        #       value : bool
        #
        # iss : dict
        #       Final inventory after moving of each skc/org being of shortsize
        #       key : prod_id, color_id, org_id
        #       value : final inventory
        #
        # qssp : dict
        #        Sum moving quantity of each skc/package
        #        key : prod_id, color_id, org_send_id, org_rec_id
        #        value : sum moving quantity
        #
        # qspb : dict
        #        Whether or not moving quantity is positive of each skc/package
        #        key : prod_id, color_id, org_send_id, org_rec_id
        #        value : bool
        #
        # qsp : dict
        #       Sum moving quantity of each package
        #       key : org_send_id, org_rec_id
        #       value : sum moving quantity
        #
        # qpb : dict
        #       Whether or not moving quantity is positive of each package
        #       key : org_send_id, org_rec_id
        #       value : bool

        i = {}
        ib = {}
        qis = {}
        qos = {}
        qib = {}
        qob = {}
        idg = {}
        idl = {}
        idgx = {}
        idlx = {}
        is_ = {}
        isb = {}
        fmsb = {}
        fsb = {}
        iss = {}
        qssp = {}
        qspb = {}
        qsp = {}
        qpb = {}

        for prod_id, color_id, size in sks_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            for org_id in po_sel[po_sel['is_store'] == 1].index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                var_name_i = 'i_' + '_'.join(idx_stc)
                var_name_ib = 'ib_' + '_'.join(idx_stc)
                var_name_idg = 'idg_' + '_'.join(idx_stc)
                var_name_idl = 'idl_' + '_'.join(idx_stc)
                i[idx_stc] = m.addVar(vtype='I', name=var_name_i)
                ib[idx_stc] = m.addVar(vtype='B', name=var_name_ib)
                idg[idx_stc] = m.addVar(vtype='I', name=var_name_idg)
                idl[idx_stc] = m.addVar(vtype='I', name=var_name_idl)
            for org_id in po_sel[po_sel['to_out'] == 1].index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                var_name_qos = 'qos_' + '_'.join(idx_stc)
                var_name_qob = 'qob_' + '_'.join(idx_stc)
                qos[idx_stc] = m.addVar(vtype='I', name=var_name_qos)
                qob[idx_stc] = m.addVar(vtype='B', name=var_name_qob)
            for org_id in po_sel[po_sel['to_in'] == 1].index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                var_name_qis = 'qis_' + '_'.join(idx_stc)
                var_name_qib = 'qib_' + '_'.join(idx_stc)
                qis[idx_stc] = m.addVar(vtype='I', name=var_name_qis)
                qib[idx_stc] = m.addVar(vtype='B', name=var_name_qib)

        for prod_id, color_id, size in sks_lst:
            idx_sks = (prod_id, color_id, size)
            var_name_idgx = 'idgx_' + '_'.join(idx_sks)
            var_name_idlx = 'idlx_' + '_'.join(idx_sks)
            idgx[idx_sks] = m.addVar(vtype='I', name=var_name_idgx)
            idlx[idx_sks] = m.addVar(vtype='I', name=var_name_idlx)

        for prod_id, color_id in skc_lst:
            po_sel = po.reset_index('size')
            po_sel = po_sel.loc[[(prod_id, color_id)]].set_index('org_id')
            for org_id in po_sel[(po_sel['is_store'] == 1) &
                                 (po_sel['to_emp'] == 0)].index.unique():
                idx_skcs = (prod_id, color_id, org_id)
                var_name_is = 'is_' + '_'.join(idx_skcs)
                var_name_isb = 'isb' + '_'.join(idx_skcs)
                var_name_fmsb = 'fmsb_' + '_'.join(idx_skcs)
                var_name_fsb = 'fsb_' + '_'.join(idx_skcs)
                var_name_iss = 'iss_' + '_'.join(idx_skcs)
                is_[idx_skcs] = m.addVar(vtype='I', name=var_name_is)
                isb[idx_skcs] = m.addVar(vtype='B', name=var_name_isb)
                fmsb[idx_skcs] = m.addVar(vtype='B', name=var_name_fmsb)
                fsb[idx_skcs] = m.addVar(vtype='B', name=var_name_fsb)
                iss[idx_skcs] = m.addVar(vtype='I', name=var_name_iss)

        for prod_id, color_id in skc_lst:
            po_sel = po.reset_index('size')
            po_sel = po_sel.loc[[(prod_id, color_id)]].set_index('org_id')
            for org_send_id in po_sel[po_sel['to_out'] == 1].index.unique():
                for org_rec_id in po_sel[(po_sel['is_store'] == 1) &
                                         (po_sel['to_in'] == 1)].index.unique():
                    if org_send_id != org_rec_id:
                        idx_soto = (prod_id, color_id, org_send_id, org_rec_id)
                        var_name_qssp = 'qssp_' + '_'.join(idx_soto)
                        var_name_qspb = 'qspb_' + '_'.join(idx_soto)
                        qssp[idx_soto] = m.addVar(vtype='I', name=var_name_qssp)
                        qspb[idx_soto] = m.addVar(vtype='B', name=var_name_qspb)

        for org_send_id in org_mo_lst:
            for org_rec_id in org_mi_lst:
                if org_send_id != org_rec_id:
                    idx_ptp = (org_send_id, org_rec_id)
                    var_name_qsp = 'qsp_' + '_'.join(idx_ptp)
                    var_name_qpb = 'qpb_' + '_'.join(idx_ptp)
                    qsp[idx_ptp] = m.addVar(vtype='I', name=var_name_qsp)
                    qpb[idx_ptp] = m.addVar(vtype='B', name=var_name_qpb)

        # Constructing objective function
        # Objective #1
        # Cost by moving quantity
        for prod_id, color_id, size in sks_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            po_in = po_sel.loc[po_sel['to_in'] == 1].copy()
            for org_send_id in po_sel[po_sel['to_out'] == 1].index.unique():
                idx_stco = (prod_id, color_id, size, org_send_id)
                for org_rec_id in po_in[po_in['is_store'] == 1].index.unique():
                    idx_ptp = (org_send_id, org_rec_id)
                    idx_stci = (prod_id, color_id, size, org_rec_id)
                    if org_send_id != org_rec_id:
                        idx_mov = (prod_id, color_id, size,
                                   org_send_id, org_rec_id)
                        cmq_in = cmq.at[idx_stci, 'cmq_trans_in']
                        cmq_out = cmq.at[idx_stco, 'cmq_trans_out']
                        cmt_sel = cmt.at[idx_ptp, 'cmt']
                        cmq_mov = 2 * cmq_in * cmq_out / (cmq_in + cmq_out) + cmt_sel
                        m.setObjective(cmq_mov * q[idx_mov], clear=False)
                for org_rec_id in po_in[po_in['is_store'] == -1].index.unique():
                    if org_send_id != org_rec_id:
                        idx_mov = (prod_id, color_id, size,
                                   org_send_id, org_rec_id)
                        m.setObjective(cmq.at[idx_stco, 'cmq_ret'] * q[idx_mov],
                                       clear=False)

        # Objective #2
        # Cost by moving packages
        for org_send_id in org_mo_lst:
            for org_rec_id in org_mi_lst:
                if org_send_id != org_rec_id:
                    idx_ptp = (org_send_id, org_rec_id)
                    m.setObjective(cmp.at[idx_ptp, 'cmp'] * qpb[idx_ptp],
                                   clear=False)

        # Objective #3
        # Cost by difference between target- and final- inventory
        for prod_id, color_id, size in sks_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            for org_id in po_sel[po_sel['is_store'] == 1].index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                m.setObjective(cid.at[idx_stc, 'cid_d'] * idg[idx_stc], clear=False)
                m.setObjective(cid.at[idx_stc, 'cid_a'] * idl[idx_stc], clear=False)

        # Objective #4
        # Cost by the maximal difference between target- and final- inventory
        for prod_id, color_id, size in sks_lst:
            idx_sks = (prod_id, color_id, size)
            m.setObjective(cidx.at[idx_sks, 'cidx_d'] * idgx[idx_sks], clear=False)
            m.setObjective(cidx.at[idx_sks, 'cidx_a'] * idlx[idx_sks], clear=False)

        # Objective #5
        # Cost by the main sizes not being full
        for prod_id, color_id in skc_lst:
            po_sel = po.reset_index('size')
            po_sel = po_sel.loc[[(prod_id, color_id)]].set_index('org_id')
            for org_id in po_sel[(po_sel['is_store'] == 1) &
                                 (po_sel['to_emp'] == 0)].index.unique():
                idx_skcs = (prod_id, color_id, org_id)
                m.setObjective(-1 * fmsb[idx_skcs], clear=False)

        # Objective #6
        # Cost by the main size groups not being full
        for prod_id, color_id in skc_lst:
            idx_skc = (prod_id, color_id)
            po_sel = po.reset_index('size')
            po_sel = po_sel.loc[[(prod_id, color_id)]].set_index('org_id')
            for org_id in po_sel[(po_sel['is_store'] == 1) &
                                 (po_sel['to_emp'] == 0)].index.unique():
                idx_skcs = (prod_id, color_id, org_id)
                m.setObjective(-1 * css.at[idx_skc, 'css'] * iss[idx_skcs],
                               clear=False)

        # Setting objective
        m.setMinimize()

        # Constructing assistant equations
        # Equation #1
        # Sum moving-in quantity of each sks/org
        for prod_id, color_id, size in sks_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            org_mi = po_sel[po_sel['to_in'] == 1].index.unique()
            org_mo = po_sel[po_sel['to_out'] == 1].index.unique()
            for org_id in org_mi:
                idx_stc = (prod_id, color_id, size, org_id)
                m.addCons(qis[idx_stc] ==
                          slv.quicksum(q.get((prod_id, color_id, size, org_send_id,
                                              org_id), 0)
                                       for org_send_id in org_mo
                                       if org_send_id != org_id))
                # qib = sign(qis)
                m.addCons(qis[idx_stc] + M * (1 - qib[idx_stc]) >= 1)
                m.addCons(qis[idx_stc] + M * qib[idx_stc] >= 0)
                m.addCons(qis[idx_stc] - M * qib[idx_stc] <= 0)

        # Equation #2
        # Sum moving-out quantity of each sks/org
        for prod_id, color_id, size in sks_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            org_mo = po_sel[po_sel['to_out'] == 1].index.unique()
            org_mi = po_sel[po_sel['to_in'] == 1].index.unique()
            for org_id in org_mo:
                idx_stc = (prod_id, color_id, size, org_id)
                m.addCons(qos[idx_stc] ==
                          slv.quicksum(q.get((prod_id, color_id, size, org_id,
                                              org_rec_id), 0)
                                       for org_rec_id in org_mi
                                       if org_rec_id != org_id))
                # qob = sign(qos)
                m.addCons(qos[idx_stc] + M * (1 - qob[idx_stc]) >= 1)
                m.addCons(qos[idx_stc] + M * qob[idx_stc] >= 0)
                m.addCons(qos[idx_stc] - M * qob[idx_stc] <= 0)

        # Equation #3
        # Final inventory after moving of each sks/org
        for prod_id, color_id, size in sks_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            for org_id in po_sel[po_sel['is_store'] == 1].index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                m.addCons(i[idx_stc] ==
                          i0.at[idx_stc, 'i0_sum'] +
                          qis.get(idx_stc, 0) - qos.get(idx_stc, 0))
                # ib = sign(i)
                m.addCons(i[idx_stc] + M * (1 - ib[idx_stc]) >= 1)
                m.addCons(i[idx_stc] + M * ib[idx_stc] >= 0)
                m.addCons(i[idx_stc] - M * ib[idx_stc] <= 0)

        # Equation #4
        # Sum final inventory after moving of each skc/org
        for prod_id, color_id in skc_lst:
            po_sel = po.reset_index('size')
            po_sel = po_sel.loc[[(prod_id, color_id)]].set_index('org_id')
            for org_id in po_sel[(po_sel['is_store'] == 1) &
                                 (po_sel['to_emp'] == 0)].index.unique():
                idx_skcs = (prod_id, color_id, org_id)
                size_lst = po_sel.loc[org_id, ['size']].values.ravel()
                m.addCons(is_[idx_skcs] ==
                          slv.quicksum(i[prod_id, color_id, size, org_id]
                                       for size in size_lst))
                # isb = sign(is_)
                m.addCons(is_[idx_skcs] + M * (1 - isb[idx_skcs]) >= 1)
                m.addCons(is_[idx_skcs] + M * isb[idx_skcs] >= 0)
                m.addCons(is_[idx_skcs] - M * isb[idx_skcs] <= 0)

        # Equation #5
        # Difference between target- and final- inventory
        for prod_id, color_id, size in sks_lst:
            idx_sks = (prod_id, color_id, size)
            po_sel = po.loc[[idx_sks]].set_index('org_id')
            idgb, idlb = {}, {}
            for org_id in po_sel[po_sel['is_store'] == 1].index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                id1 = m.addVar(lb=None, vtype='I', name='id1')
                id2 = m.addVar(lb=None, vtype='I', name='id2')
                idb1 = m.addVar(vtype='B', name='idb1')
                idb2 = m.addVar(vtype='B', name='idb2')
                idgb[org_id] = m.addVar(vtype='B', name='idgb' + org_id)
                idlb[org_id] = m.addVar(vtype='B', name='idlb' + org_id)
                m.addCons(id1 == i[idx_stc] - it.at[idx_stc, 'it'])
                m.addCons(id2 == it.at[idx_stc, 'it'] - i[idx_stc])
                # idg = max(id1, 0)
                m.addCons(idg[idx_stc] >= id1)
                m.addCons(idg[idx_stc] <= id1 + M * (1 - idb1))
                m.addCons(idg[idx_stc] <= M * idb1)
                # idl = max(id2, 0)
                m.addCons(idl[idx_stc] >= id2)
                m.addCons(idl[idx_stc] <= id2 + M * (1 - idb2))
                m.addCons(idl[idx_stc] <= M * idb2)
                # idgx = max(idg)
                m.addCons(idgx[idx_sks] >= idg[idx_stc])
                m.addCons(idgx[idx_sks] <= idg[idx_stc] + M * (1 - idgb[org_id]))
                # idlx = max(idl)
                m.addCons(idlx[idx_sks] >= idl[idx_stc])
                m.addCons(idlx[idx_sks] <= idl[idx_stc] + M * (1 - idlb[org_id]))
            m.addCons(slv.quicksum(idgb.values()) == 1)
            m.addCons(slv.quicksum(idlb.values()) == 1)

        # Equation #6
        # Marking out fullsize orgs of each skc
        for prod_id, color_id in skc_lst:
            po_sel = po.reset_index('size')
            po_sel = po_sel.loc[[(prod_id, color_id)]].set_index('org_id')
            for org_id in po_sel[(po_sel['is_store'] == 1) &
                                 (po_sel['to_emp'] == 0)].index.unique():
                idx_skcs = (prod_id, color_id, org_id)

                ms_lst = ms.loc[idx_skcs, ['size']].values.ravel()

                # Number of the main sizes with positive inventory
                nms = m.addVar(vtype='I', name='nms')
                m.addCons(nms ==
                          slv.quicksum(ib.get((prod_id, color_id, size, org_id), 0)
                                       for size in ms_lst))
                # Marking out full main sizes
                nmsb = m.addVar(vtype='B', name='nmsb')
                len_ms = ms_lst.shape[0]
                m.addCons(nms - M * nmsb <= len_ms - 1)
                m.addCons(nms + M * (1 - nmsb) >= len_ms)
                m.addCons(nms - M * (1 - nmsb) <= len_ms)
                # Marking out orgs with full main sizes after moving of each skc
                m.addCons(fmsb[idx_skcs] == 1 - isb[idx_skcs] + nmsb)

                qfb = {}
                # Traversing all size groups
                grp_num = ms_lst.shape[0] - qsf.at[idx_skcs, 'qsf'] + 1
                for i_s in range(grp_num):
                    i_e = i_s + qsf.at[idx_skcs, 'qsf']
                    size_grp = ms_lst[i_s:i_e]
                    var_name_qfb = 'qfb_' + str(i_s)
                    qfb[i_s] = m.addVar(vtype='B', name=var_name_qfb)
                    # Number of the main sizes with positive inventory in a group
                    ns = m.addVar(vtype='I', name='ns')
                    m.addCons(ns == slv.quicksum(ib.get((prod_id, color_id, size,
                                                         org_id), 0)
                                                 for size in size_grp))
                    # Marking out fullsize groups
                    qsf_sel = qsf.at[idx_skcs, 'qsf']
                    m.addCons(ns - M * qfb[i_s] <= qsf_sel - 1)
                    m.addCons(ns + M * (1 - qfb[i_s]) >= qsf_sel)
                    m.addCons(ns - M * (1 - qfb[i_s]) <= qsf_sel)

                # Number of fullsize groups
                qfs = m.addVar(vtype='I', name='qfs')
                qfsb = m.addVar(vtype='B', name='qfsb')
                m.addCons(qfs == slv.quicksum(qfb.values()))
                # qfsb = sign(qfs)
                m.addCons(qfs + M * (1 - qfsb) >= 1)
                m.addCons(qfs + M * qfsb >= 0)
                m.addCons(qfs - M * qfsb <= 0)
                # Marking out orgs with fullsize groups after moving of each skc
                m.addCons(fsb[idx_skcs] == 1 - isb[idx_skcs] + qfsb)

        # Equation #7
        # Sum final inventory after moving of each skc/org being of shortsize
        for prod_id, color_id in skc_lst:
            po_sel = po.reset_index('size')
            po_sel = po_sel.loc[[(prod_id, color_id)]].set_index('org_id')
            for org_id in po_sel[(po_sel['is_store'] == 1) &
                                 (po_sel['to_emp'] == 0)].index.unique():
                idx_skcs = (prod_id, color_id, org_id)
                m.addCons(iss[idx_skcs] == is_[idx_skcs] * fsb[idx_skcs])

        # Equation #8
        # Sum moving quantity of each skc/package
        for prod_id, color_id in skc_lst:
            po_sel = po.reset_index('size')
            po_sel = po_sel.loc[[(prod_id, color_id)]].set_index('org_id')
            for org_send_id in po_sel[po_sel['to_out'] == 1].index.unique():
                for org_rec_id in po_sel[(po_sel['is_store'] == 1) &
                                         (po_sel['to_in'] == 1)].index.unique():
                    if org_send_id != org_rec_id:
                        idx_soto = (prod_id, color_id, org_send_id, org_rec_id)
                        m.addCons(qssp[idx_soto] ==
                                  slv.quicksum(q.get((prod_id, color_id, size,
                                                      org_send_id, org_rec_id), 0)
                                               for size in po_sel['size'].values))
                        # qspb = sign(qssp)
                        m.addCons(qssp[idx_soto] + M * (1 - qspb[idx_soto]) >= 1)
                        m.addCons(qssp[idx_soto] + M * qspb[idx_soto] >= 0)
                        m.addCons(qssp[idx_soto] - M * qspb[idx_soto] <= 0)

        # Equation #9
        # Sum moving quantity of each package
        for org_send_id in org_mo_lst:
            for org_rec_id in org_mi_lst:
                if org_send_id != org_rec_id:
                    idx_ptp = (org_send_id, org_rec_id)
                    m.addCons(qsp[idx_ptp] ==
                              slv.quicksum(q.get((prod_id, color_id, size,
                                                  org_send_id, org_rec_id), 0)
                                           for prod_id, color_id, size in sks_lst))
                    # qpb = sign(qsp)
                    m.addCons(qsp[idx_ptp] + M * (1 - qpb[idx_ptp]) >= 1)
                    m.addCons(qsp[idx_ptp] + M * qpb[idx_ptp] >= 0)
                    m.addCons(qsp[idx_ptp] - M * qpb[idx_ptp] <= 0)

        # Constructing constrains
        # Constrain #1
        # For each sks/org, total moving-out quantity cannot be higher than initial
        # inventory
        for prod_id, color_id, size in sks_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            for org_id in po_sel[po_sel['to_out'] == 1].index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                m.addCons(qos[idx_stc] <= max(i0.at[idx_stc, 'i0'], 0))

        # Constrain #2
        # For each sks/org, moving-in cannot lead to final inventory being higher
        # than the target inventory
        for prod_id, color_id, size in sks_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            for org_id in po_sel[(po_sel['to_in'] == 1) &
                                 (po_sel['is_store'] == 1)].index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                i0_sum_sel = i0.at[idx_stc, 'i0_sum']
                it_sel = it.at[idx_stc, 'it']
                m.addCons(qis[idx_stc] <= max(0, it_sel - i0_sum_sel))

        # Constrain #3
        # For each sks/org, moving-out and move-in cannot co-exist
        for prod_id, color_id, size in sks_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            for org_id in po_sel[po_sel['is_store'] == 1].index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                if (idx_stc in qib) and (idx_stc in qob):
                    m.addCons(qib[idx_stc] + qob[idx_stc] <= 1)

        # Constrain #4
        # For each skc/org, any main size cannot be moved out to be empty if skc
        # has been moved-in but not fullsize
        for prod_id, color_id in skc_lst:
            po_sel = po.reset_index('size')
            po_sel = po_sel.loc[[(prod_id, color_id)]].set_index('org_id')
            for org_id in po_sel[(po_sel['is_store'] == 1) &
                                 (po_sel['to_emp'] == 0)].index.unique():
                idx_skcs = (prod_id, color_id, org_id)
                io_sel = io_skcs.at[idx_skcs, 'has_in']
                if io_sel > 0:
                    ms_lst = ms.loc[idx_skcs, ['size']].values.ravel()
                    size_lst = po_sel.loc[org_id, ['size']].values.ravel()
                    for size in set(ms_lst) & set(size_lst):
                        idx_stc = (prod_id, color_id, size, org_id)
                        i0_sum_sel = i0.at[idx_stc, 'i0_sum']
                        if i0_sum_sel > 0:
                            m.addCons(i[idx_stc] >= 1 - fsb[idx_skcs])

        # Constrain #5
        # For each skc/org, moving-out package number cannot be higher than the
        # upper-bound
        for prod_id, color_id in skc_lst:
            po_sel = po.reset_index('size')
            po_sel = po_sel.loc[[(prod_id, color_id)]].set_index('org_id')
            org_rec = po_sel[(po_sel['is_store'] == 1) &
                             (po_sel['to_in'] == 1)].index.unique()
            for org_id in po_sel[po_sel['to_out'] == 1].index.unique():
                idx_skcs = (prod_id, color_id, org_id)
                m.addCons(slv.quicksum(qspb[prod_id, color_id, org_id, org_rec_id]
                                       for org_rec_id in org_rec
                                       if org_rec_id != org_id) <=
                          qmp.at[idx_skcs, 'qmp_r'])

        # Constrain #6
        # For each org/season, skc number and inventory after moving cannot be
        # lower than the lower bounds
        for org_id, season_id in pi.index.unique():
            skc_lst = pi.loc[[(org_id, season_id)]].drop_duplicates().values
            is_s = m.addVar(vtype='I', name='is_s')
            isbs = m.addVar(vtype='I', name='isbs')
            m.addCons(is_s == slv.quicksum(is_[prod_id, color_id, org_id]
                                           for prod_id, color_id in skc_lst))
            m.addCons(isbs == slv.quicksum(isb[prod_id, color_id, org_id]
                                           for prod_id, color_id in skc_lst))
            m.addCons(is_s >= i_bd.at[(org_id, season_id), 'qs_lb'])
            m.addCons(isbs >= i_bd.at[(org_id, season_id), 'qd_lb'])

        m.optimize()

        m_status = m.getStatus()
        m_gap = m.getGap()
        lz_logger.info('=' * 50 + ' ' + m_status + ' ' + '=' * 50)
        lz_logger.info('-' * 30 + ' Gap: ' + str(m_gap * 100) + '% ' + '-' * 30)

        q_r = {}
        if m_status == 'optimal' or m_gap <= 0.05:
            for k in q:
                q_r[k] = round(m.getVal(q[k]))

        return q_r
