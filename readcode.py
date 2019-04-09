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
            1.2.1 
            1.2.2
            1.2.3 

        1.3 计算每个size的权重  cal_sales_prop_size(po, s)
            1.3.1
            1.3.2
            1.3.3

        1.4 计算销售权重之和 cal_sum_sales_we(po, sp_skc, sp_store, sp_size)
            1.4.1 Sum sales weights of skc, size, and store based on store
   
    2. 计算组织之间的发送和接收权重 cal_sr_we(si, wi, sales_we)
        2.1 产生一对移动组织 _gen_mov_cp(si, wi)
        2.2 计算发送和接收的门店的权重 _extr_sr_we(mv_cp, sales_we)
        2.3 归一化 _normalize(sr_we)
            最大最小归一化 max - min + 1.0 E -10

    3. 计算库存平衡权重 cal_inv_dev_we(ms, i0, s)
        3.1 计算初始库存与 每个skc的销量之和
        3.2 合并数据并计算各规模初始库存和销售额之和的相关系数。
        3.3 计算库存平衡权重

四、计算成本参数 cal_cost_params.py
    cmq, cmp, cid, cdl, cbs = \
    ccp.cal_cost_params(pi, si, wi, po, io, sales_we, sr_we, ib_we,
                        pl.cmq_base_ws, pl.cmq_base_ss, pl.cmp_base_ws,
                        pl.cmp_base_ss, pl.cid_base, pl.cdl_base, pl.cbs_base)
    
    1. 计算移动数量的单位成本 cal_mov_quant_cost(po, io, sales_we, cmq_base_ws, cmq_base_ss)
        1.1 根据销售权重--> 计算 补货、进入、进出的cmp
        1.2 将无效的值分别填充为最小值，最大值

    2. 计算移动包裹的单位成本 cal_mov_pkg_cost(si, wi, sr_we, cmp_base_ws, cmp_base_ss)
        2.1 根据组织间的发送和接收权重-> 计算
        2.2 产生成对的移动组织、 补货组织
        2.3 合并表,用最大值填充无效值
        2.4 计算移动包裹的单位成本（包括仓库到门店、门店之间）

    3. 计算库存差异的单位成本 cid （库存优化中，计算目标库存与库存的差异）
        3.1 根据销售权重之和-> 计算
        3.2 将无效的值分别填充为最小值，最大值
        3.3 用权重之和 乘以 单位库存差异成本（cid_base）得到最小值 cid_a 及最大值 cid_d

    4. 计算需求损失的单位成本 cdl 
        4.1 根据销售权重之和 -> 计算
        4.2 将无效的值分别填充为最小值，最大值
        4.3 用权重之和 乘以单位库存差异成本 得到一个上限和下限 （下限*2）cdl_lb, cdl_ub

    5. 计算断码的单位成本
        5.1 根据 库存平衡权重-> 计算
        5.2 产品信息表 与 库存平衡权重表 合并，NA用最大值清洗
        5.3 用权重乘以单位断码率的成本

五、计算目标库存 cal_targ_inv.py
    d, qss, it = cti.cal_targ_inv(po, ms, i0, s, sales_we, cdl, cid, pl.w,
                              pl.qss_base)

    1. 计算预测需求 # Calculating demand
        d = cal_dem(po, ms, s, w)
        1.1 计算下周的基本需求
            1.1.1 设置周销量的权重  w = {'w_1': 0.7, 'w_2': 0.3}，对于NA设置为0
            1.1.2 将前两周的销量 按照权重 相加，返回下周的基本需求

        1.2 计算每个size在区域的销售比例
            1.2.1 计算每个skc/size的 销量之和
            1.2.2 将mng_reg_id 合并到 销量之和的表，并将NA置0，并按照 sku + ,mng_reg_id 求和
                  将 <0 的销量置0
            1.2.3 计算每个每个size在skc和区域的销售比例
                  计算skc 在 mng_reg_id 的销售之和，将销售除以总和

        1.3 计算需求的上下界 cal_dem_bd(ms, d_base, sp_size)
            1.3.1 标准化销售比例 
                  norm_min = sp / max  norm_max = sp /min  #?????
            1.3.2 合并 基本需求表与销售比例表
            1.3.3 计算主码的需求上下界 ：上界: 基本需求的平均值 * 标准化后的比例上界 
                                       下界：基本需求的平均值 * 标准化后的比例下界
                   填充非主码都为基本需求
                  

    2. 计算安全库存 # Calculating safety stock
        qss = cal_safety_stock(po, ms, i0, d, sales_we, qss_base)
        2.1 计算每个skc的 初始库存 及 预测需求的下界
        2.2 合并： 销售权重表、 主码表、 初始库存表、 预测需求的下界 ,并清洗NA
        2.3 将基本安全库存乘以主要尺寸的安全库存的销售重量归一化
            x * 基本安全库存 /(max - min)
        2.4 设置期初库存和需求下界 同时为0 的安全库存 为 0

    3.计算基本的目标库存  # Calculating basic target inventory
        it0 = cal_basic_it(po, i0, s)
        3.1 计算上周可以销售的库存
            合并销售数据表 与 期初库存表，并清洗数据
            将期初库存 与 销售量 相加 （得到的是一周前的期初库存），
            如果值s<0, 设置为0; 如果期初库存 > 销量, 则设置s为期初库存
        3.2 合并可销售的库存 与 目标库存  #???

    4.计算目标库存的上下界 # Calculating target inventory lower- and upper- bounds
        it_bd = cal_it_bd(d, qss) 
        根据 1 预测需求 和 2 安全库存 计算目标库存的上下界
        4.1 NA 置零
        4.2 目标库存的上界 = 预期需求的上届 + 安全库存
            目标库存的下界 = 预测需求的下界 + 安全库存

    5. 计算提取skc和组织 计算目标库存的目标skc/org  # Extracting target skc/orgs of calculating target inventory
        po_ti = extr_po_ti(po, i0, it_bd)

    6. 计算门店的目标库存 # Calculating target inventory of stores
        it = exec_targ_inv_opt(po_ti, ms, it0, it_bd, cdl, cid)
        1. 获取org_id 唯一的值，计算得到org的个数
        2. 多线程 计算 目标库存优化
            2.1 pool.map(partial(exec_unit, po, ms, it0, it_bd, cdl, cid),
                      po['mng_reg_id'].drop_duplicates())
                po : 产品和门店的计算目标库存
                ms : 主码
                it0 ：基本的目标库存
                it_bd ：目标库存的上下界
                cdl ：单位需求损失成本
                cid ：单位库存差价成本

            2.2 exec_unit(po, ms, it0, it_bd, cdl, cid, mng_reg_id_sel)
                每30个一组 循环调用 it_opt_solver(po_grp, ms, it0, it_bd, cdl, cid)
            2.3 计算目标库存优化 （pyscipopt）
                it_opt_solver(po_grp, ms, it0, it_bd, cdl, cid)

                变量：
                    创建决策变量
                    创建辅助变量 key : prod_id, color_id, size, org_id   dict
                        dlpl = {} 目标库存与上界的差异
                        dlpu = {} 目标库存与下界的差异
                        ida = {}  目标库存与基本库存的绝对差异
                目标：
                    1) 目标库存低于下界的成本
                        x * 单位损失成本下界
                    2) 目标库存高于上界的成本
                        x * 单位损失成本上界
                    3) 目标库存与 基本值得绝对差值
                        x * 单位库存差异的 最大值
                辅助等式约束：
                    1) 目标库存 低于 下界 #？？？？？
                    2) 目标库存 高于 上界
                    3) 目标库存 与 基本值的 绝对值

                约束：
                    如果skc的目标库存 是正数，那么主码的目标库存 也必须是正数。
                    （设置M 是极大值）
        3.合并表

    7. 调整目标库存 # Adjusting target inventory
        it = adj_targ_inv(po, it)
        1. 合并目标库存表 与 产品组织表，并置NA为0


















六、初始化数据
七、执行skc/org的补货
八、执行补货
九、更新
十、执行skc/org调拨
十一、执行调拨
十二、 更新
十三、 统计

