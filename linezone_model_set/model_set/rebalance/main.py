# -*- coding: utf-8 -*-

__version__ = 'V2.0.1'
__project__ = 'oz_app'
__author__ = ['StrPi', 'redpoppet', 'panrui1994']
__updated__ = '2019-02-14'
__kernel__ = 'V6.10.2b'

import datetime as dt
import random
from multiprocessing import freeze_support

import pandas as pd

from check.TypeAssert import typeassert
from model_set.rebalance.settings import model_param
from model_set import spark
from model_set.rebalance.cal_cost_param import CostParam
from model_set.rebalance.cal_targ_inv import TargetInventory
from model_set.rebalance.extr_dec_targ import CrossExtract
from model_set.rebalance.load_data import DataLayer
from model_set.rebalance.mov_opt import Optimization
from model_set.rebalance.postp_data import DataProcess
from model_set.rebalance.prep_data import DataPrepare
from model_set.rebalance.prep_sales_data import SaleDataPrepare
from utils.logger import lz_logger
# from pack.authorization_check import Authorized

# @Authorized()
def run():
    time_start = dt.datetime.now()
    lz_logger.info('Program running starts')
    freeze_support()
    q_rep, q_trans, q_ret = execute()
    result_analysis(q_rep, q_trans, q_ret)
    output_to_hive(q_rep, q_trans, q_ret)
    time_end = dt.datetime.now()
    lz_logger.info('Program running ends')
    lz_logger.info('Total running time:{total_time}'.format(total_time=time_end - time_start))


@typeassert(q_rep=pd.DataFrame, q_trans=pd.DataFrame)
def result_analysis(q_rep, q_trans, q_ret):
    # Statistical analysis
    lz_logger.info('\n' + '=' * 50)
    lz_logger.info('Version:{version}'.format(version=__version__))
    lz_logger.info('Decision date:{date_dec}'.format(date_dec=model_param.DECISION_DATE))

    lz_logger.info('\n' + '=' * 20 + ' REP ' + '=' * 20)
    lz_logger.info('Sum REP quantity:{rep_qty}'.format(rep_qty=q_rep['qty_mov'].sum()))
    lz_logger.info('Sum REP package number:{rep_package_qty}'.format(rep_package_qty=q_rep[[
        'org_send_id', 'org_rec_id']].drop_duplicates().shape[0]))
    lz_logger.info('Sum REP skc number:{rep_skc_number}'.format(
        rep_skc_number=q_rep[['prod_id', 'color_id']].drop_duplicates().shape[0]))
    lz_logger.info('Sum REP out warehouse number:{rep_out_org}'.format(
        rep_out_org=q_rep['org_send_id'].drop_duplicates().shape[0]))
    lz_logger.info(
        'Sum REP in store number:{rep_store_num}'.format(rep_store_num=q_rep['org_rec_id'].drop_duplicates().shape[0]))

    lz_logger.info('\n' + '=' * 20 + ' TRANS ' + '=' * 20)
    lz_logger.info('Sum TRANS quantity:{trans_qty}'.format(trans_qty=q_trans['qty_mov'].sum()))
    lz_logger.info('Sum TRANS package number:{trans_package_num}'.format(
        trans_package_num=q_trans[['org_send_id', 'org_rec_id']].drop_duplicates().shape[0]))
    lz_logger.info('Sum TRANS skc number:{trans_skc_num}'.format(
        trans_skc_num=q_trans[['prod_id', 'color_id']].drop_duplicates().shape[0]))
    lz_logger.info('Sum TRANS out store number:{trans_store_num}'.format(
        trans_store_num=q_trans['org_send_id'].drop_duplicates().shape[0]))
    lz_logger.info('Sum TRANS in store number:{trans_store_num}'.format(trans_store_num=q_trans[
        'org_rec_id'].drop_duplicates().shape[0]))
    lz_logger.info('\n' + '=' * 20 + ' RET ' + '=' * 20)
    lz_logger.info('Sum RET quantity:{ret_qty}'.format(ret_qty=q_ret['qty_mov'].sum()))
    lz_logger.info('Sum RET package number:{ret_package}'.format(
        ret_package=q_ret[['org_send_id', 'org_rec_id']].drop_duplicates().shape[0]))
    lz_logger.info('Sum RET skc number:'.format(ret_skc_num=q_ret[['prod_id', 'color_id']].drop_duplicates().shape[0]))
    lz_logger.info('Sum RET out store number:{ret_out_store_number}'.format(
        ret_out_store_number=q_ret['org_send_id'].drop_duplicates().shape[0]))
    lz_logger.info('Sum RET in warehouse number:{ret_warehouse_number}'.format(
        ret_warehouse_number=q_ret['org_rec_id'].drop_duplicates().shape[0]))
    lz_logger.info('\n' + '=' * 50)


@typeassert(rep=pd.DataFrame, trans=pd.DataFrame)
def output_to_hive(rep, trans, ret):
    """
        columns : prod_id, color_id, size, org_send_id, org_rec_id, qty_mov, date_send, date_rec_pred
    :param rep:
    :param trans:
    :param ret:
    :return:
    """
    rep = rep.rename(columns={"prod_id": "product_code", "color_id": "color_code", "size": "size_code",
                              "org_send_id": "send_org_code",
                              "org_rec_id": "receive_store_code", "date_send": "date_send_sug",
                              "qty_mov": "send_qty"})

    trans = trans.rename(columns={"prod_id": "product_code", "color_id": "color_code", "size": "size_code",
                                  "org_send_id": "send_store_code",
                                  "org_rec_id": "receive_store_code", "date_send": "date_send_sug",
                                  "qty_mov": "send_qty"})

    ret = ret.rename(columns={"prod_id": "product_code", "color_id": "color_code", "size": "size_code",
                              "org_send_id": "send_store_code",
                              "org_rec_id": "receive_store_code", "date_send": "date_send_sug",
                              "qty_mov": "send_qty"})

    trans.drop(['date_send_sug'], axis=1, inplace=True)

    trans['send_qty'] = trans['send_qty'].astype('int64')
    ret['send_qty'] = ret['send_qty'].astype('int64')

    rep["scene"] = random.randint(1, 5)
    rep["scene_name"] = rep["scene"].map(lambda x: model_param.SCENE_DICT[x])
    # rep['date_rec_pred'] = dt.time.strftime("%Y--%m--%d", rep['date_send_sug'])
    # rep['date_rec_pred'] = rep['date_send_sug']

    # rep['date_send_sug'] = rep['date_send_sug'].dt.strftime("%Y-%m-%d")
    # rep['date_rec_pred'] = rep['date_send_sug'].dt.strftime("%Y-%m-%d")
    rep['date_rec_pred'] = (rep['date_send_sug']+dt.timedelta(days=1)).dt.strftime("%Y-%m-%d")

    rep['date_send_sug'] = rep['date_rec_pred']
    # print('**************************************************************')
    # print(type(rep['date_send_sug']))


    trans["scene"] = random.randint(1, 5)
    trans["scene_name"] = trans["scene"].map(lambda x: model_param.SCENE_DICT[x])
    # trans['date_rec_pred'] = trans['date_send_sug']

    ret["scene"] = random.randint(1, 5)
    ret["scene_name"] = trans["scene"].map(lambda x: model_param.SCENE_DICT[x])
    # ret['date_rec_pred'] = ret['date_send_sug']

    rep["dec_day_date"] = (model_param.DECISION_DATE+ dt.timedelta(days=1)).strftime("%Y-%m-%d")
    rep["etl_time"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    trans["dec_day_date"] = (model_param.DECISION_DATE+dt.timedelta(days=1)).strftime("%Y-%m-%d")
    trans["etl_time"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ret["dec_day_date"] = (model_param.DECISION_DATE + dt.timedelta(days=1)).strftime("%Y-%m-%d")
    ret["etl_time"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    rep_df = spark.createDataFrame(rep)
    rep_df.write.saveAsTable("erke_shidian_edw_ai_dev.mod_sku_replenish_current", mode="overwrite")

    trans_df = spark.createDataFrame(trans)
    trans_df.write.saveAsTable("erke_shidian_edw_ai_dev.mod_sku_allot_current", mode="overwrite")

    # ret_df = spark.createDataFrame(ret)
    # ret_df.write.saveAsTable("erke_shidian_edw_ai_dev.mod_sku_return_current", mode="overwrite")

    return True


def execute():
    db_layer = DataLayer()
    # Loading data
    print(model_param.DECISION_DATE)
    pi, oi, di, i0, s, mv, itp, mpa, mss = db_layer.load_data(date_dec=model_param.DECISION_DATE)
    lz_logger.info('Loading data ends')

    # Pre-processing sales data
    sale_data_prepare = SaleDataPrepare()
    s = sale_data_prepare.proc_sales_data(pi, oi, i0, s, model_param.DECISION_DATE)
    lz_logger.info('Pre-processing sales data ends')


    prp = DataPrepare()
    pi, oi, po, i0, s, io = \
        prp.prep_data(pi, oi, di, i0, s, mv, itp, mpa, mss, model_param.DECISION_DATE,
                      model_param.prt_per_in, model_param.per_sales)
    lz_logger.info('Pre-processing data ends')

    # Extracting the main sizes
    data_extract = CrossExtract()
    ms, qsf = data_extract.extr_main_size(po, i0, s, model_param.len_main_sizes, model_param.qsf_base)
    lz_logger.info('Extracting the main sizes ends')

    # Extracting weights
    sales_we_p, sales_we_s, sales_we_all, sr_we, lt_we, ib_we = \
        data_extract.extr_we(po, oi, ms, i0, s)
    lz_logger.info('Extracting weights ends')

    # Calculating cost parameters
    cost_param = CostParam()
    cmq, cmp, cmt, cid, cidx, cdl, css = \
        cost_param.cal_cost_params(pi, oi, po, itp, io, sales_we_p, sales_we_s,
                                   sales_we_all, sr_we, lt_we, ib_we, model_param.spr_we,
                                   model_param.cmq_base_ws, model_param.cmq_base_ss,
                                   model_param.cmq_base_sw,
                                   model_param.cmp_base_ws, model_param.cmp_base_ss,
                                   model_param.cmp_base_sw,
                                   model_param.cmt_base_ss, model_param.cid_base,
                                   model_param.cidx_base,
                                   model_param.cdl_base, model_param.css_base)

    lz_logger.info('Calculating cost parameters ends')

    # Calculating target inventory
    time_start_it = dt.datetime.now()
    lz_logger.info('\n' + '=' * 20 + ' Calculating target inventory starts ' + '=' * 20)
    target_inventory = TargetInventory()
    d, qss, it = target_inventory.cal_targ_inv(po, ms, i0, s, itp, io, sales_we_p, cdl,
                                               cid, model_param.w, model_param.dr_base,
                                               model_param.cal_ti_mode,
                                               model_param.DECISION_DATE,
                                               model_param.exe_fest_adj)

    time_end_it = dt.datetime.now()
    lz_logger.info('\n' + '=' * 20 + ' Calculating target inventory ends ' + '=' * 20)
    lz_logger.info(
        'Target inventory optimization running time:{total_time}'.format(total_time=time_end_it - time_start_it))

    # output Target inventory
    it_copy = it.reset_index()
    output_targ_inv_to_hive(it_copy)
    lz_logger.info('Saving target inventory ends')

    # Initializing data
    psp = DataProcess()
    q, i0_aft_mov, io_aft_mov = psp.init_data(i0, io)

    opt = Optimization()
    # Running replenishment
    if model_param.exe_rep:
        # Extracting target skc/org for replenishment
        po_rep = data_extract.extr_po_rep(po, i0_aft_mov, it)
        lz_logger.info('Extracting target skc/org for replenishment ends')

        time_start_rep = dt.datetime.now()
        lz_logger.info('\n' + '=' * 20 + ' Replenishment optimization starts ' + '=' * 20)
        # Executing replenishment

        q_rep, cmp = opt.exec_rep(po_rep, i0_aft_mov, it, cmp, cid, cidx)
        time_end_rep = dt.datetime.now()
        lz_logger.info('=' * 20 + ' Replenishment optimization ends ' + '=' * 20)
        lz_logger.info(
            'Replenishment optimization running time:{total_time}'.format(total_time=time_end_rep - time_start_rep))
        lz_logger.info('Sum replenishment quantity:{rep_qty}'.format(rep_qty=q_rep.values.sum()))

        # Updating moving quantity, initial inventory of moving-out orgs, io
        # markers, and moving quantity unit cost
        if not q_rep.empty:
            q, i0_aft_mov, io_aft_mov = psp.postp_data(q, q_rep, i0_aft_mov, io_aft_mov)
            lz_logger.info('Updating data ends')

    # Generating and saving replenishment table
    q_rep = psp.gen_rep_tab(q, oi, model_param.DECISION_DATE)
    lz_logger.info('Generating and saving replenishment table ends')

    # Running transferring optimization
    if model_param.exe_trans:
        # Extracting target skc/org for transferring
        po_trans = data_extract.extr_po_trans(po, i0_aft_mov, io_aft_mov, it)
        lz_logger.info('Extracting target skc/org for transferring ends')
        # Calculating residual moving-out package number of each skc/org

        qmp = psp.cal_qmp(po, q, model_param.qmp_max)
        lz_logger.info('Calculating residual moving-out package number ends')

        time_start_trans = dt.datetime.now()
        lz_logger.info('\n' + '=' * 20 + ' Transferring optimization starts ' + '=' * 20)
        # Executing transferring
        q_trans, cmp = opt.exec_trans(po, po_trans, ms, i0_aft_mov, io_aft_mov,
                                      it, qsf, qmp, cmq, cmp, cmt, cid, cidx,
                                      css)
        time_end_trans = dt.datetime.now()
        lz_logger.info('=' * 20 + ' Transferring optimization ends ' + '=' * 20)
        lz_logger.info(
            'Transferring optimization running time:{total_time}'.format(total_time=time_end_trans - time_start_trans))
        lz_logger.info('Sum transferring quantity:{trans_qty}'.format(trans_qty=q_trans.values.sum()))

        # Updating moving quantity, initial inventory of moving-out orgs, io
        # makers, and moving quantity unit cost
        if not q_trans.empty:
            q, i0_aft_mov, io_aft_mov = psp.postp_data(q, q_trans, i0_aft_mov,
                                                       io_aft_mov)
            lz_logger.info('Updating data ends')

    # Generating and saving transferring table
    q_trans = psp.gen_trans_tab(q, oi, model_param.DECISION_DATE)
    # if pl.is_online:
    #     svd.save_mov_tab(q_trans, pl.date_dec, pl.is_test,
    #                      'edw.mod_sku_day_allot_model')
    lz_logger.info('Generating and saving transferring table ends')

    # Running return
    if model_param.exe_ret:
        # Emptying selected stores by returning to warehouses
        q_ret = opt.exec_ret(po, oi, i0_aft_mov)
        lz_logger.info('Sum return quantity:{ret_qty}'.format(ret_qty=q_ret.values.sum()))

        if not q_ret.empty:
            q, i0_aft_mov, io_aft_mov = psp.postp_data(q, q_ret, i0_aft_mov, io_aft_mov)
            lz_logger.info('Updating data ends')

    # Generating and saving return table
    q_ret = psp.gen_ret_tab(q, oi, model_param.DECISION_DATE)

    lz_logger.info('Generating and saving return table ends')

    # Merging and saving all data
    # all_data = psp.merge_all_data(po, ms, i0, s, itp, io, d, qss, it, q)
    # print('>>>>> Merging and saving all data ends.')

    return q_rep, q_trans, q_ret


def output_targ_inv_to_hive(it):
    # it: pd.DataFrame
    # Target
    # inventory
    # index: prod_id, color_id, size, org_id
    # columns: it

    it = it.rename(columns={"prod_id": "product_code", "color_id": "color_code", "size": "size_code",
                              "org_id": "org_code", "it": "targ_inv"})
    it['targ_inv'] = it['targ_inv'].astype('int64')

    it["dec_day_date"] = (model_param.DECISION_DATE+ dt.timedelta(days=1)).strftime("%Y-%m-%d")
    it["etl_time"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    rep_df = spark.createDataFrame(it)
    rep_df.write.saveAsTable("erke_shidian_edw_ai_dev.mod_sku_targ_inv_current", mode="overwrite")
    return True

# def output_to_hive(it):
#     # it: pd.DataFrame
#     # Target
#     # inventory
#     # index: prod_id, color_id, size, org_id
#     # columns: it
#
#
#
#     rep_df = spark.createDataFrame(it)
#     rep_df.write.saveAsTable("erke_shidian_edw_ai_dev.mod_sku_targ_inv_current1", mode="overwrite")
#     return True