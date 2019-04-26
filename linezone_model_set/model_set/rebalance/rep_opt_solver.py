# -*- coding: utf-8 -*-

import pandas as pd
from core.optimazation import Optimazation
from model_set.rebalance.settings import model_param

from check.TypeAssert import typeassert


class ReplenishOptimization(object):
    def __init__(self):
        pass

    @typeassert(po=pd.DataFrame, i0=pd.DataFrame, it=pd.DataFrame, cmp=pd.DataFrame,
                cid=pd.DataFrame, cidx=pd.DataFrame)
    def rep_opt_solver(self, po, i0, it, cmp, cid, cidx):
        """
        Replenishment optimization solver

        Parameters
        ----------
        po : pd.DataFrame
             Target products and organizations
             index : prod_id, color_id, size, org_id
             columns : to_in, to_out

        i0 : pd.DataFrame
             Initial inventory data
             index : prod_id, color_id, size, org_id
             columns : i0, r, i0_sum

        it : pd.DataFrame
             Target inventory
             index : prod_id, color_id, size, org_id
             columns : it

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
        q : dict
            Moving quantity of each sks between organizations
            key : prod_id, color_id, size, org_send_id, org_rec_id
            value : moving quantity
        """

        po = po.reset_index('org_id')
        sks_lst = po.index.unique()
        org_mo_lst = po.loc[po['to_out'] == 1, 'org_id'].drop_duplicates().values
        org_mi_lst = po.loc[po['to_in'] == 1, 'org_id'].drop_duplicates().values

        m = Optimazation(executor=model_param.executor)
        # m.setRealParam('limits/gap', 0.03)
        m.setParam('limits/time', 200)
        M = 10000

        q = {}
        for prod_id, color_id, size in sks_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            org_mo = po_sel[po_sel['to_out'] == 1].index.unique()
            org_mi = po_sel[po_sel['to_in'] == 1].index.unique()
            for org_send_id in org_mo:
                for org_rec_id in org_mi:
                    if org_send_id != org_rec_id:
                        idx_mov = (prod_id, color_id, size,
                                   org_send_id, org_rec_id)
                        var_name = 'q_' + '_'.join(idx_mov)
                        q[idx_mov] = m.addVar(vtype='I', name=var_name)

        # Creating assistant variables
        # i : dict
        #     Final inventory after moving of each sks/org
        #     key : prod_id, color_id, size, org_id
        #     value : end inventory
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
        # idl : dict
        #       Quantity of final inventory lower than target inventory of each
        #       sks/org
        #       key : prod_id, color_id, size, org_id
        #       value : inventory difference
        #
        # idlx : dict
        #        The maximal quantity of final inventory lower than target
        #        inventory of each sks
        #        key : prod_id, color_id, size
        #        value : the maximal inventory difference
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
        qis = {}
        qos = {}
        idl = {}
        idlx = {}
        # qsp = {}
        # qpb = {}

        for prod_id, color_id, size in sks_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            for org_id in po_sel.index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                var_name_i = 'i_' + '_'.join(idx_stc)
                i[idx_stc] = m.addVar(vtype='I', name=var_name_i)

            for org_id in po_sel[po_sel['to_in'] == 1].index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                var_name_idl = 'idl_' + '_'.join(idx_stc)
                var_name_qis = 'qis_' + '_'.join(idx_stc)
                idl[idx_stc] = m.addVar(vtype='I', name=var_name_idl)
                qis[idx_stc] = m.addVar(vtype='I', name=var_name_qis)

            for org_id in po_sel[po_sel['to_out'] == 1].index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                var_name_qos = 'qos_' + '_'.join(idx_stc)
                qos[idx_stc] = m.addVar(vtype='I', name=var_name_qos)

        for prod_id, color_id, size in sks_lst:
            idx_sks = (prod_id, color_id, size)
            var_name_idlx = 'idlx_' + '_'.join(idx_sks)
            idlx[idx_sks] = m.addVar(vtype='I', name=var_name_idlx)

        # for org_send_id in org_mo_lst:
        #     for org_rec_id in org_mi_lst:
        #         if org_rec_id != org_send_id:
        #             idx_ptp = (org_send_id, org_rec_id)
        #             var_name_qsp = 'qsp_' + '_'.join(idx_ptp)
        #             var_name_qpb = 'qpb_' + '_'.join(idx_ptp)
        #             qsp[idx_ptp] = m.addVar(vtype='I', name=var_name_qsp)
        #             qpb[idx_ptp] = m.addVar(vtype='B', name=var_name_qpb)

        # Constructing objective function
        # Objective #1
        # Cost by moving quantity
        # for prod_id, color_id, size in sks_lst:
        #     po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
        #     for org_send_id in po_sel[po_sel['to_out'] == 1].index.unique():
        #         for org_rec_id in po_sel[po_sel['to_in'] == 1].index.unique():
        #             idx_stc = (prod_id, color_id, size, org_rec_id)
        #             if org_send_id != org_rec_id:
        #                 idx_mov = (prod_id, color_id, size,
        #                            org_send_id, org_rec_id)
        #                 expr.addTerms(cmq.at[idx_stc, 'cmq_rep'], q[idx_mov])

        # Objective #2
        # Cost by moving packages
        # for org_send_id in org_mo_lst:
        #     for org_rec_id in org_mi_lst:
        #         if org_send_id != org_rec_id:
        #             idx_ptp = (org_send_id, org_rec_id)
        #             expr.addTerms(cmp.at[idx_ptp, 'cmp'], qpb[idx_ptp])

        # Objective #3
        # Cost by quantity of final inventory lower than target inventory
        for prod_id, color_id, size in sks_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            for org_id in po_sel[po_sel['to_in'] == 1].index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                m.setObjective(cid.at[idx_stc, 'cid_rep'] * idl[idx_stc],
                               clear=False)

        # Objective #4
        # Cost by the maximal quantity of final inventory lower than target
        # inventory
        for prod_id, color_id, size in sks_lst:
            idx_sks = (prod_id, color_id, size)
            m.setObjective(cidx.at[idx_sks, 'cidx_a'] * idlx[idx_sks], clear=False)

        # Setting objective
        m.setMinimize()

        # Constructing assistant equations
        # Equation #1
        # Sum moving-in quantity of each sks/org
        for prod_id, color_id, size in sks_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            for org_id in po_sel[po_sel['to_in'] == 1].index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                org_s_lst_sel = po_sel[po_sel['to_out'] == 1].index.unique()
                m.addCons(qis[idx_stc] ==
                          m.quicksum(q.get((prod_id, color_id, size,
                                            org_send_id, org_id), 0)
                                     for org_send_id in org_s_lst_sel
                                     if org_send_id != org_id))

        # Equation #2
        # Sum moving-out quantity of each sks/org
        for prod_id, color_id, size in sks_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            for org_id in po_sel[po_sel['to_out'] == 1].index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                org_r_lst_sel = po_sel[po_sel['to_in'] == 1].index.unique()
                m.addCons(qos[idx_stc] ==
                          m.quicksum(q.get((prod_id, color_id, size,
                                            org_id, org_rec_id), 0)
                                     for org_rec_id in org_r_lst_sel
                                     if org_rec_id != org_id))

        # Equation #3
        # Final inventory after moving of each sks/org
        for prod_id, color_id, size in sks_lst:
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            for org_id in po_sel[po_sel['to_out'] == 1].index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                m.addCons(i[idx_stc] == i0.at[idx_stc, 'i0'] - qos[idx_stc])
            for org_id in po_sel[po_sel['to_in'] == 1].index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                m.addCons(i[idx_stc] == i0.at[idx_stc, 'i0_sum'] + qis[idx_stc])

        # Equation #4
        # Quantity of final inventory lower than target inventory
        for prod_id, color_id, size in sks_lst:
            idx_sks = (prod_id, color_id, size)
            po_sel = po.loc[[(prod_id, color_id, size)]].set_index('org_id')
            idlb = {}
            for org_id in po_sel[po_sel['to_in'] == 1].index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                id_ = m.addVar(lb=None, vtype='I', name='id')
                idb = m.addVar(vtype='B', name='idb')
                idlb[org_id] = m.addVar(vtype='B', name='idlb' + org_id)
                m.addCons(id_ == it.at[idx_stc, 'it'] - i[idx_stc])
                # idl = max(id_, 0)
                m.addCons(idl[idx_stc] >= id_)
                m.addCons(idl[idx_stc] <= id_ + M * (1 - idb))
                m.addCons(idl[idx_stc] <= M * idb)
                # idlx = max(idl)
                m.addCons(idlx[idx_sks] >= idl[idx_stc])
                m.addCons(idlx[idx_sks] <= idl[idx_stc] + M * (1 - idlb[org_id]))
            m.addCons(m.quicksum(idlb.values()) == 1)

        # Equation #5
        # Sum moving quantity of each package
        # for org_send_id in org_mo_lst:
        #     for org_rec_id in org_mi_lst:
        #         if org_send_id != org_rec_id:
        #             idx_ptp = (org_send_id, org_rec_id)
        #             m.addCons(qsp[idx_ptp] ==
        #                       scip.quicksum(q.get((prod_id, color_id, size,
        #                                            org_send_id, org_rec_id), 0)
        #                                     for prod_id, color_id, size in
        #                                     sks_lst))
        #             # qpb = sign(qsp)
        #             m.addCons(qsp[idx_ptp] + M * (1 - qpb[idx_ptp]) >= 1)
        #             m.addCons(qsp[idx_ptp] + M * qpb[idx_ptp] >= 0)
        #             m.addCons(qsp[idx_ptp] - M * qpb[idx_ptp] <= 0)

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
            for org_id in po_sel[po_sel['to_in'] == 1].index.unique():
                idx_stc = (prod_id, color_id, size, org_id)
                i0_sum_sel = i0.at[idx_stc, 'i0_sum']
                it_sel = it.at[idx_stc, 'it']
                m.addCons(qis[idx_stc] <= max(0, it_sel - i0_sum_sel))

        m.optimize()

        q_r = m.get_result(q)
        # m_status = m.getStatus()
        # m_gap = m.getGap()
        # lz_logger.info('=' * 50 + ' ' + m_status + ' ' + '=' * 50)
        # lz_logger.info('-' * 30 + ' Gap: ' + str(m_gap * 100) + '% ' + '-' * 30)
        #
        # q_r = {}
        # if m_status == 'optimal' or m_gap <= 0.05:
        #     for k in q:
        #         q_r[k] = round(m.getVal(q[k]))

        return q_r


if __name__ == '__main__':
    pass
