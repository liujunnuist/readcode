一. Loading data	加载数据
    pi : Product information
    si : Store basic information
    wi : Warehouse basic information	仓库基本信息
    ms : Main sizes
    i0 : Initial inventory data      期初存货
    s  : Sales data 
    mv : Moving data  （组织的发送方、接收方、日期、数量等）
    mr : Moving restriction states  （not_in, not_out 门店因为装修等原因不能进出）
    mp :  Store moving period  （index : org_id	columns : is_rep, is_trans ？）

二. Pre-processing data  预处理数据
    pi, si, po, i0, s, qsf, io = prp.prep_data(pi, si, wi, ms, i0, s, mv, mr, mp, pl.date_dec, pl.qsf_base,pl.prt_per_in)
    return pi_p, si_p, po, i0_f, s_f, qsf, io

    date_dec : Decision making date (date)
    qsf_base : Basic continue-size length in a fullsize group (int)
    prt_per_in : Moving in protecting period (int)

    1.期初库存(i0)
        1）将skc或org初始库存之和为0， 设置skc或org的sku的库存 都为0  (为什么要有置零操作？)
        2）将所有为负数的库存都置为0
    2. 统计每周总销售额（s, date_dec） Aggregating weekly sales 
        1) 按7天 销售数据分组
        2）分组求和，计算每周的销量
        3）将销量<0,置零
    3. 处理产品信息(pi, i0, mr)
        1）计算sku的最大期初库存max，max大于0的才认为有效
        2）计算每个skc的移动限制，选择能够移动的
        3）选择满足1）和2）的商品
    4. 处理门店信息(si, i0, s, mr, mp)
        1）计算每个组织的累加期初库存，选出 大于0的组织
        2）计算每个组织的上一周(week_no == -1)的销量总和, 选出 大于0的组织
        3）选择 满足两者之一 (与或条件)的组织
        4）计算每个门店的移动状态，选择能够进出的门店
        5）选择满足3）4）的组织
    5. 交叉商品和门店信息 (pi, si, wi, i0, s, mr)
        1) 门店信息(is_store=0) 与 仓库信息（is_store=1）
        2) 合并数据 ？
        3）选择skc和组织 满足 期初库存 或 前两周最大销量 大于 0 的
    6. 填充 5）得到的表中的无效的数据 (pi_p, si_p, wi, i0_p, s_week, mr)
        1）将5的数据表 去重后 join 初始库存表
        2）填充NaN 为 0
    7) 填充销售数据 (po, i0)
        1）产品组织表去重
        2) 生成周表。week_no = [-4, -3, -2, -1]
        3) 合并 生成完整的每周销售数据
    8) 计算最小的连码数 (ms, qsf_base)
        1) 设置基本的连码数
        2）对于大于基本连码数的 设置 = 基本连码数（qsf_base）
    9) 标记一种内是否有移入移出 (po, mv, date_dec, prt_per_in)
        1) 七天的保护周期
        2）计算当前时间的一周内 累积的发出量 之和
        3）计算当前时间的一周内 累计的接收量 之和
        4）计算当前时间的一周内 发出与接收之和
        5）去重 将NA置0， 如果有库存 >0 has_in=1, else has_in= 0

三. 获取权重 extr_we.py  
    sales_we, sr_we, ib_we = etw.extr_we(po, si, wi, ms, i0, s)

    1. 计算销售权重 cal_sales_we(po, i0, s)
        计算权重 是为了让销量好的 移出的成本更大
        1.1 计算在区域内的每个skc的权重  cal_sales_prop_skc(po, i0, s)
            1.1.1 计算每个门店的skc 销量之和
            1.1.2 计算每个门店的skc 期初库存之和
            1.1.3 将po 从大类筛选、删除size、去重，与 1) 和 2) 合并，去除无效值
            1.1.4 计算每个skc在 大类和区域的 权重
  
        1.2 计算在skc中每个门店的权重 cal_sales_prop_store(po, s)
            1.2.1 产生一对移动组织
            1.2.2 计算组织间的权重
            1.2.3 归一化

        1.3 计算每个size的权重  cal_sales_prop_size(po, s)
            1.3.1
            1.3.2
            1.3.3

        1.4 计算销售权重之和 cal_sum_sales_we(po, sp_skc, sp_store, sp_size)
    2. 计算组织之间的发送和接收权重 cal_sr_we(si, wi, sales_we)
    3. 计算期初库存权重 cal_inv_dev_we(ms, i0, s)

四、计算成本参数
    cmq, cmp, cid, cdl, cbs = \
    ccp.cal_cost_params(pi, si, wi, po, io, sales_we, sr_we, ib_we,
                        pl.cmq_base_ws, pl.cmq_base_ss, pl.cmp_base_ws,
                        pl.cmp_base_ss, pl.cid_base, pl.cdl_base, pl.cbs_base)
