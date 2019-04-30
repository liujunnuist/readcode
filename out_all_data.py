def merge_all_data(po, ms, i0, s, itp, io, d, qss, it, q):
    """
    Merging all data

    Parameters
    ----------
    po : pd.DataFrame
         Crossed product and organization information
         index : prod_id, color_id, size_id, org_id
         columns : size_order, year, season_id, brand_id, class_0, class_1,
                   class_2, prov_id, city_id, dist_id, mng_reg_id, qs_min,
                   qd_min, is_store, is_new_skc, is_new_store, is_dist,
                   is_rep, is_trans, is_ret, not_in, not_out, to_emp

    ms : pd.DataFrame
         Main sizes
         index : prod_id, color_id, org_id
         columns : size_id

    i0 : pd.DataFrame
         Initial inventory data
         index : prod_id, color_id, size_id, org_id
         columns : i0, r, i0_sum

    s : pd.DataFrame
        Sales data
        index : prod_id, color_id, size_id, org_id, week_no
        columns : s

    itp : pd.DataFrame
          Target inventory of the previous day
          index : prod_id, color_id, size_id, org_id
          columns : itp

    io : pd.DataFrame
         Markers of existed moving -in/-out
         index : prod_id, color_id, size_id, org_id
         columns : has_in, has_out

    d : pd.DataFrame
        Predicted demand lower- and upper- bounds
        index : prod_id, color_id, size_id, org_id
        columns : d_lb, d_ub

    qss : pd.DataFrame
          Safety stock
          index : prod_id, color_id, size_id, org_id
          columns : qss

    it : pd.DataFrame
         Target inventory
         index : prod_id, color_id, size_id, org_id
         columns : it

    q : pd.DataFrame
        Moving quantity of each sks between organizations
        index : prod_id, color_id, size_id, org_send_id, org_rec_id
        columns : qty_mov

    Returns
    -------
    all_data : pd.DataFrame
               All data for decision
               index : position
               columns : prod_id, color_id, size_id, org_id, city_id,
                         mng_reg_id, is_new_skc, is_new_store, is_dist,
                         is_rep, is_trans, is_ret, not_in, not_out, to_emp,
                         is_ms, i0, r, i0_sum, s_cum, s_b2, s_b1, d_lb, d_ub,
                         qss, it, itp, has_in, has_out, qis, qos, ia
    """

    cols_send = ['prod_id', 'color_id', 'size_id', 'org_send_id']
    cols_rec = ['prod_id', 'color_id', 'size_id', 'org_rec_id']
    cols_stc = ['prod_id', 'color_id', 'size_id', 'org_id']
    cols_grp = ['city_id', 'mng_reg_id', 'is_new_skc', 'is_new_store',
                'is_dist', 'is_rep', 'is_trans', 'is_ret', 'not_in',
                'not_out', 'to_emp']

    # Marking main sizes
    ms2 = ms['size_id'].reset_index().set_index(cols_stc)
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