一. 加载数据 #load_data.py
    1.  pi : Product information
        获取的是昨天的数据
        sku级别的去重复、滤除所有包含NaN 、 转换数据类型、排序sort_values、设置index

    2.  si : Store basic information
        选取正常营业的门店
        mng_reg_id = 大区id (dq_id) + 地区id (zone_id) 组合
        org级别的去重复、滤除所有包含NaN 、 转换数据类型、排序sort_values、设置index

    3.  wi : Warehouse basic information	仓库基本信息
             index : org_id     
             columns : prov_id, city_id
        如果为空，则设置为浙江 宁波 #WHERE store_id = '420867' ？？？？

    # 4.  ms : Main sizes
    4.  i0 : Initial inventory data      期初存货
             index : prod_id, color_id, size, org_id
             columns : i0, r

             i0 : 期初库存，前一天的库存量
             r  ：在途库存，期待到达的日期在9天前 到7天前，#####？？？？？？？
                 sum（期待到达的数量 - 实际到达的数量）
            'prod_id', 'color_id', 'size', 'org_id'级别去重复、滤除所有包含NaN 、 转换数据类型、排序sort_values、设置index

    5.  s  : Sales data 
            index : prod_id, color_id, size, org_id
            columns : date_sell, s

            s: 计算了一个月内的销量qty之和 （between 1 and 30）

    6.  mv : Moving data  
         index : prod_id, color_id, size
         columns : org_send_id, org_rec_id, date_send, date_rec, quant_send (发送数量),
                   quant_rec (接受数量)
        6.1 从库存移动事实表中，选取发送日期 三个月内的数据 #三个月内？？
        6.2 删除 quant_send < 0 的数据
        6.2 删除NA, 转化数据格式，排序，设置index

    7.  mr : Moving restriction states 
            index : prod_id, color_id, org_id
            columns : not_in, not_out 
           （not_in, not_out 门店因为装修等原因不能进出）
            选取当前日期 正常营业的门店的移动现实状态（具体到某个门店的skc）

    8.  mp :  Store moving period   ##？？？
              index : org_id	
              columns : is_rep, is_trans 
              选取当前日期的正常营业的门店的数据 
              is_rep, is_trans : (0, 1)变量, Y 为1，否则为0 

######################################################################
二. 预处理数据 #prep_data.py
    pi, si, po, i0, s, qsf, io = prp.prep_data(pi, si, wi, ms, i0, s, mv, mr, mp, pl.date_dec, pl.qsf_base,pl.prt_per_in)
    
    return pi_p, si_p, po, i0_f, s_f, qsf, io

    date_dec : Decision making date (date)
    qsf_base : Basic continue-size length in a fullsize group (int)
    prt_per_in : Moving in protecting period (int)

    1. 处理期初库存(i0) #i0_p = proc_neg_inv(i0)
        1）如果每个org的skc期初库存之和为0， 设置sku的期初库存 置为0  ###为什么要有置零操作？？？
        2）设置 所有期初库存i0 <0 置为0
        3) 设置 所有在途库存r <0 置为0
        4) 进行 第 6 步

    2. 统计每周总销售额 #s_week = agg_week_sales(s, date_dec)
        s_week : pd.DataFrame
             Weekly sales
             index : prod_id, color_id, size, org_id, week_no
             columns : s
        1) 按7天 销售数据分组， -1 代表上周，-2 代表上上周
        2）分组求和，计算每周的销量
        3）将周销量s <0, 置零
        
    3. 处理产品信息 #pi_p = proc_prod_info(pi, i0_p, mr)
        1）计算sku的最大期初库存i0_max，max大于0的才认为有效 ###代码
        2）计算每个skc的移动限制，选择同时能够移入移出的sku
            not_in * not_out >0  => not_in >1 and not_out >1  ####?????
        3）选择满足1）和2）的商品

    4. 处理门店信息 #si_p = proc_org_info(si, i0_p, s_week, mr, mp)
        1）计算每个门店的累加库存i0_sum （i0_sum = 期初库存i0 + 在途库存 r），选出 i0_sum > 0的门店
        2）计算每个门店的上一周(week_no == -1)的销量总和, 选出 销量 > 0 的门店
        3）选择 满足1) 或 2) 两者之一 (与或条件)的门店
        4）计算每个门店的移动限制，选择同时能够移入移出的门店
        5）选择满足3）不满足4）的组织  ## ~~~~？？？

    5. 商品和门店信息 #po = cross_prod_org(pi_p, si_p, wi, i0_p, s_week, mr)
        po : pd.DataFrame
            Crossed products and organizations
            index : prod_id, color_id, size, org_id
            columns : size_order, year, season_id, class_0, prov_id, city_id,
                   dist_id, mng_reg_id, is_store, is_rep, is_trans

        1) 设置门店信息(is_store=1) 与 仓库信息（is_store=0）标记
        2) 合并数据
            将门店信息 与 RDC信息合并；再与产品信息合并
            选取 前两周的销售数据，
        3）选择skc和组织 满足 期初库存 或 前两周最大销量(期初库存i0+ 在途库存r+销量s）> 0 的 skc和组织对
            is_store != 1
            ###代码？？？

    6. 填充 5) 得到的表中的无效的数据 (pi_p, si_p, wi, i0_p, s_week, mr)
        1）将5的数据表 去重后 join 初始库存表
        2）填充NaN 为 0

    7. 填充销售数据 (po, i0)
        1）产品组织表去重
        2) 生成周表。week_no = [-4, -3, -2, -1]
        3) 合并 生成完整的每周销售数据

    8. 计算最小的连码数 (ms, qsf_base)
        1) 设置基本的连码数
        2）对于大于基本连码数的 设置 = 基本连码数（qsf_base）

    9. 标记一种内是否有移入移出 (po, mv, date_dec, prt_per_in)
        1) 七天的保护周期
        2）计算当前时间的一周内 累积的发出量 之和
        3）计算当前时间的一周内 累计的接收量 之和
        4）计算当前时间的一周内 发出与接收之和
        5）去重、NA置0， 如果有库存 >0 has_in=1, else has_in= 0

######################################################################
三. 获取权重 extr_we.py  
    sales_we, sr_we, ib_we = etw.extr_we(po, si, wi, ms, i0, s)

    1. 计算销量权重 #sales_we = cal_sales_we(po, i0, s)
        (计算权重 是为了让销量好的 移出的成本更大)
        1.1 计算在区域内的每个skc的权重  #sp_skc = cal_sales_prop_skc(po, i0, s)
            1.1.1 计算每个门店的skc 销量之和
            1.1.2 计算每个门店的skc 期初库存之和
            1.1.3 将po 从大类筛选、删除size、去重，与 1) 和 2) 合并，去除无效值
            1.1.4 计算每个skc在 大类和区域的 权重
  
        1.2 计算在skc中每个门店的权重 #sp_store = cal_sales_prop_store(po, s)
            1.2.1 
            1.2.2
            1.2.3 

        1.3 计算每个size的权重 #sp_size = cal_sales_prop_size(po, s)
            1.3.1
            1.3.2
            1.3.3

        1.4 计算销售权重之和 #sales_we = cal_sum_sales_we(po, sp_skc, sp_store, sp_size)
            1.4.1 Sum sales weights of skc, size, and store based on store
   
    2. 计算组织之间的发送和接收权重 #sr_we = cal_sr_we(si, wi, sales_we)
        2.1 产生一对移动组织 _gen_mov_cp(si, wi)
        2.2 计算发送和接收的门店的权重 _extr_sr_we(mv_cp, sales_we)
        2.3 归一化 _normalize(sr_we)
            最大最小归一化 max - min + 1.0 E -10

    3. 计算库存平衡权重 #ib_we = cal_inv_dev_we(ms, i0, s)
        3.1 计算初始库存与 每个skc的销量之和
        3.2 合并数据并计算各规模初始库存和销售额之和的相关系数。
        3.3 计算库存平衡权重

######################################################################
四、计算成本参数 cal_cost_params.py
    cmq, cmp, cid, cdl, cbs = \
    ccp.cal_cost_params(pi, si, wi, po, io, sales_we, sr_we, ib_we,
                        pl.cmq_base_ws, pl.cmq_base_ss, pl.cmp_base_ws,
                        pl.cmp_base_ss, pl.cid_base, pl.cdl_base, pl.cbs_base)
    
    1. 计算移动数量的单位成本 #cmq = cal_mov_quant_cost(po, io, sales_we, cmq_base_ws, cmq_base_ss)
        cal_mov_quant_cost(po, io, sales_we, cmq_base_ws, cmq_base_ss)
        1.1 根据销售权重-> 计算 补货、进入、进出的cmp
        1.2 将无效的值分别填充为最小值，最大值

    2. 计算移动包裹的单位成本 #cmp = cal_mov_pkg_cost(si, wi, sr_we, cmp_base_ws, cmp_base_ss)
        cal_mov_pkg_cost(si, wi, sr_we, cmp_base_ws, cmp_base_ss)
        2.1 根据组织间的发送和接收权重-> 计算
        2.2 产生成对的移动组织、 补货组织
        2.3 合并表,用最大值填充无效值
        2.4 计算移动包裹的单位成本（包括仓库到门店、门店之间）

    3. 计算库存差异的单位成本 #cid = cal_inv_diff_cost(po, sales_we, cid_base)
    （库存优化中，计算目标库存与库存的差异）
        3.1 根据销售权重之和-> 计算
        3.2 将无效的值分别填充为最小值，最大值
        3.3 用权重之和 乘以 单位库存差异成本（cid_base）得到最小值 cid_a 及最大值 cid_d

    4. 计算需求损失的单位成本 #cdl = cal_dem_loss_cost(po, sales_we, cdl_base)
        4.1 根据销售权重之和 -> 计算
        4.2 将无效的值分别填充为最小值，最大值
        4.3 用权重之和 乘以单位库存差异成本 得到一个上限和下限 （下限*2）cdl_lb, cdl_ub

    5. 计算断码的单位成本 #cbs = cal_brokensize_cost(pi, ib_we, cbs_base)
        5.1 根据 库存平衡权重-> 计算
        5.2 产品信息表 与 库存平衡权重表 合并，NA用最大值清洗
        5.3 用权重乘以单位断码率的成本

######################################################################
五、计算目标库存 #cal_targ_inv.py
    #d, qss, it = cti.cal_targ_inv(po, ms, i0, s, sales_we, cdl, cid, pl.w, pl.qss_base)

    1. 计算预测需求 # d = cal_dem(po, ms, s, w)
        1.1 计算下周的基本需求 #d_base = cal_basic_dem(s, w)
            1.1.1 设置周销量的权重  w = {'w_1': 0.7, 'w_2': 0.3}，对于NA设置为0
            1.1.2 将前两周的销量 按照权重 相加，返回下周的基本需求

        1.2 计算每个size在区域的销售比例 #sp_size = cal_sales_prop_size(po, s)
            1.2.1 计算每个skc/size的 销量之和
            1.2.2 将mng_reg_id 合并到 销量之和的表，并将NA置0，并按照 sku + ,mng_reg_id 求和
                  将 <0 的销量置0
            1.2.3 计算每个每个size在skc和区域的销售比例
                  计算skc 在 mng_reg_id 的销售之和，将销售除以总和

        1.3 计算需求的上下界 #d = cal_dem_bd(ms, d_base, sp_size)
            cal_dem_bd(ms, d_base, sp_size)
            1.3.1 标准化销售比例 
                  norm_min = sp / max  norm_max = sp /min  #?????
            1.3.2 合并 基本需求表与销售比例表
            1.3.3 计算主码的需求上下界 ：上界: 基本需求的平均值 * 标准化后的比例上界 
                                       下界：基本需求的平均值 * 标准化后的比例下界
                   填充非主码都为基本需求
                  

    2. 计算安全库存 # qss = cal_safety_stock(po, ms, i0, d, sales_we, qss_base)
        qss = cal_safety_stock(po, ms, i0, d, sales_we, qss_base)
        2.1 计算每个skc的 初始库存 及 预测需求的下界
        2.2 合并： 销售权重表、 主码表、 初始库存表、 预测需求的下界 ,并清洗NA
        2.3 将基本安全库存乘以主要尺寸的安全库存的销售重量归一化
            x * 基本安全库存 /(max - min)
        2.4 设置期初库存和需求下界 同时为0 的安全库存 为 0

    3.计算基本的目标库存  # it0 = cal_basic_it(po, i0, s)
        it0 = cal_basic_it(po, i0, s)
        3.1 计算上周可以销售的库存
            合并销售数据表 与 期初库存表，并清洗数据
            将期初库存 与 销售量 相加 （得到的是一周前的期初库存），
            如果值s<0, 设置为0; 如果期初库存 > 销量, 则设置s为期初库存
        3.2 合并可销售的库存 与 目标库存  #???

    4.计算目标库存的上下界 # it_bd = cal_it_bd(d, qss)
        it_bd = cal_it_bd(d, qss) 
        根据 1 预测需求 和 2 安全库存 计算目标库存的上下界
        4.1 NA 置零
        4.2 目标库存的上界 = 预期需求的上届 + 安全库存
            目标库存的下界 = 预测需求的下界 + 安全库存

    5. 计算提取skc和组织 计算目标库存的目标skc/org  #po_ti = extr_po_ti(po, i0, it_bd)

        po_ti = extr_po_ti(po, i0, it_bd)

    6. 计算门店的目标库存 # it = exec_targ_inv_opt(po_ti, ms, it0, it_bd, cdl, cid)
        6.1. 获取org_id 唯一的值，计算得到org的个数
        6.2. 多线程 计算 目标库存优化
            6.2.1 pool.map(partial(exec_unit, po, ms, it0, it_bd, cdl, cid),
                      po['mng_reg_id'].drop_duplicates())
                po : 产品和门店的计算目标库存
                ms : 主码
                it0 ：基本的目标库存
                it_bd ：目标库存的上下界
                cdl ：单位需求损失成本
                cid ：单位库存差价成本

            6.2.2 exec_unit(po, ms, it0, it_bd, cdl, cid, mng_reg_id_sel)
                每30个一组 循环调用 it_opt_solver(po_grp, ms, it0, it_bd, cdl, cid)
            6.2.3 计算目标库存优化 （pyscipopt）
                it_opt_solver(po_grp, ms, it0, it_bd, cdl, cid)

                变量：
                    创建决策变量
                    创建辅助变量 key : prod_id, color_id, size, org_id   dict
                        dlpl = {} 目标库存与下界的差异
                        dlpu = {} 目标库存与上界的差异
                        ida = {}  目标库存与基本库存的绝对差异
                目标 (极小值)：
                    1) 目标库存低于下界的成本
                        x * 单位损失成本下界 (惩罚系数) (cdl_ub) 
                    2) 目标库存高于上界的成本
                        x * 单位损失成本上界 (惩罚系数) (cdl_ub)
                    3) 目标库存与 基本值得绝对差值
                        x * 单位库存差异的 最大值
                辅助等式约束：
                    1) 目标库存 低于 下界 
                    dll = 目标库存下界 - 目标库存
                    如果dll < 0 ,则 dlpl为0 ，不需要惩罚，如果dll > 0 ,则dlpl = dll, 需要惩罚
                    #dlpl = max(dll, 0 )
                    2) 目标库存 高于 上界 #dlpu = max(dlu, 0)
                    3) 目标库存 与 基本值的 绝对值

                约束：
                    如果skc的目标库存 是正数，那么主码的目标库存 也必须是正数。
                    （设置M 是极大值）
        6.3.合并表

    7. 调整目标库存 # it = adj_targ_inv(po, it)
        it = adj_targ_inv(po, it)
        1. 合并目标库存表 与 产品组织表，并置NA为0

######################################################################
六、初始化数据 #postp_data.py
    # q, i0_aft_mov, io_aft_mov = psp.init_data(i0, io)
    1. 组织之间每个sks的移动数量 q (空表)
    2. 更新 初始库存（复制）i0_aft_mov 和 移动/移出 （hash_in = 0）

######################################################################
七、提取补货的目标skc/org #extr_dec_targ.py
    # po_rep = edt.extr_po_rep(po, i0_aft_mov, mr, it)
    1. 合并数据
    2. 对于每个skc，组织 必须是门店，目标库存 高于 期初库存之和， 能够允许调入
    3. 对于每个skc, 组织 必须是仓库，且 期初库存 > 0
    4. 求和  移入/移出的标记

######################################################################
八、执行补货 #mov_opt.py
    #q_rep, cmp = mvp.exec_rep(po_rep, i0_aft_mov, it, cmq, cmp, cid)
                      exec_rep(po, i0, it, cmq, cmp, cid)
    cmp : 移动包裹的单位成本

    1. 获取目标组织 org_rec_id 的数量、目标skc的数量
    2. 每100个一组，执行 补货优化,得到仓库到门店的每个sks的数量 ：
       补货优化 rep_opt_solver.py
       ros.rep_opt_solver(po_grp, i0, it, cmq, cmp, cid)
       rep_opt_solver(po, i0, it, cmq, cmp, cid)

       辅助变量：
            q : 组织之间 每个sks的移动量 key : prod_id, color_id, size, org_send_id, org_rec_id
            i : 移动之后最终的库存  key : prod_id, color_id, size, org_id
            qis : 移入数量之和   key : prod_id, color_id, size, org_id
            qos : 移出数量之和   key : prod_id, color_id, size, org_id
            idl ： 最终库存低于 目标库存的数量  key : prod_id, color_id, size, org_id
            qsp ： 每个包裹的移动 数量之和
            qpb : 每个移动包括的数量是否 > 0 （type = bool）
        
        目标函数：（最小化）
            1）移动数量的成本
                发送方到接收方的单位数量成本cmq * 组织间的移动数量
            2）移动包裹的成本
                发送方到接收方的单位包裹成本cmp * [0, 1] （有数量肯定有包裹，没有数量肯定没包裹，包裹只算一次）
            3）最终的库存低于目标的成本
                库存差异的单位成本 * 库存差异的数量idl
        
        辅助等式约束：

        约束：
            1）对于每个sks，移出量之和qos 不能大于 初始库存i0
            2）对于每个sks，移入量之和qis 不能导致 最终的库存 大于 目标库存

    3. 更新 移动包裹的单位成本
        3.1 对求得仓库到门店的每个sks数量 dict 求和 > 0， 则：
            将移动数量累加求和，如果大于0，则将包裹的cmp置0 （因为是分段算的，第一段已经计算后，第二段应该将单位成本置0）


######################################################################
九、更新 #postp_data.py
    # psp.postp_data(q, q_rep, i0_aft_mov, io_aft_mov)
    1. 更新移动数量
        q_n = update_q(q, q_a)
        q : 组织之间的每个sks的移动数量
        q_a : 插入的组织之间的每个sks的移动数量

        1.1 两个表 外连接 合并，清洗NA为0
        1.2 将数量与 插入的数量相加，返回更新后的移动数量

    2. 计算 移出和移入的数量之和
        qis, qos = cal_qios(q_a)
        cal_qios(q)
        2.1 q 以接收组织id 为重置索引和分组的对象，累计求和，返回移入的数量 qis
        2.2 q 以发送组织id 为重置索引和分组的对象，累计求和，返回移出的数量 qos

    3. 更新移出组织的期初库存
        i0_n = update_inv(i0, qis, qos)
        i0 = i0 - 移出的数量
        r (在途库存)  = 在途库存 + 移入的量
 
    4. 更新io标记 # Updating io markers
　　    io_n = update_io(io, qis, qos)
        如果移入数量 > 0, has_in =1
        如果移出数量 > 0, has_out =2

######################################################################
十、执行skc/org调拨 # extr_dec_targ.py
    edt.extr_po_trans(po, i0_aft_mov, io_aft_mov, mr, it)
    extr_po_trans(po, i0, io, mr, it)
    po : 产品组织表 
    i0 ：期初库存表 
    io : 移入移出标记表 
    mr : 移动限制状态表 
    it : 目标库存表

    1. 合并数据：筛选是调拨的门店， 合并表
        i0_sum = 期初存货i0 + 在途库存r #？？？？？
    2. 标记在调拨中的每个skc的调入调出
        2.1 能够移入的门店 (to_in = 1)，必须满足：
            目标库存 it > 0
            目标库存 it > i0_sum
            不能被移出 has_out = 0
            没有被限制 移入 not_in = 0
        2.2 能够移出的门店 (to_out = 1)，必须满足：
            期初存货 i0 > 0 
            不能被移入 has_in = 0
            没有被限制 移出 not_out = 0
    3. 对移入、移出求和
        to_out_skcr = to_out 求和
        to_mov_skcs = to_in + to_out 再求和
    4. 挑选 to_out_skcr > 0 且 to_mov_skcs 的数据


######################################################################
十一、执行调拨优化 # mov_opt.py
    q_trans, cmp = mvp.exec_trans(po_trans, ms, i0_aft_mov, it, qsf, cmq, cmp, cid,
                              cbs)
    exec_trans(po, ms, i0, it, qsf, cmq, cmp, cid, css)
    1. 获取目标组织 org_rec_id 的数量、目标skc的数量
    2. 多线程 计算 调拨优化 
    exec_trans_unit(po, ms, i0, it, qsf, cmq, cmp, cid, css, mng_reg_id_sel)
        2.1 便于并行，拆分每个地区的单位包裹成本
            管理区域的目标门店数目
            区域的目标skc数目
        2.2 每30个一组，执行 调拨优化：# trans_opt_solver.py
        q_grp = tos.trans_opt_solver(po_grp, ms, i0, it, qsf, cmq, cmp_reg,
                                         cid, css)
        trans_opt_solver(po, ms, i0, it, qsf, cmq, cmp, cid, cbs)

        决策变量 q (dict)：
            门店之间每个sks的移动数量 q

        辅助变量 （dict）：
            i = 每个sks 调拨后的最终库存
            ib = 是否最终库存 >0 (bool) 
            qis = 每个sks的移入数量之和
            qos = 每个sks的移出数量之和
            qib = 是否qis > 0 (bool)
            qob = 是否qos > 0 (bool)
            idg = 最终库存 > 目标库存的差异量
            idl = 最终库存 < 目标库存的差异量

            is_ = 每个skc 调拨后的最终库存
            isb = 是否每个skc最终库存 >0 (bool) 
            bsb = 是否每个skc的所有主码是断码 (bool)
            ibs = 每个断码的skc的最终库存
            qsp = 每个包裹的移动数量
            qpb = 是否每个包裹的移动数量 > 0 (bool)

        目标：
            1）移动数量的成本  ###?????
                2 * 移入单位成本cmq_trans_in * 移出单位成本cmq_trans_out / (移入单位成本 * 移出单位成本)

            2）移动包裹的成本
                移动包裹的单位成本 * qpb (0,1变量)

            3）目标库存与最终库存的差异的成本
                单位库存差异成本最小值 cid_a * 最终库存 与目标库存的差异 idg
                单位库存差异成本的最大值 cid_b * 目标库存 - 最终库存的差异 idl
                #???????
            4）主码断码的成本
                断码的单位成本cbs * 每个断码的skc的最终库存 ibs
        
        辅助等式约束：
            1) qib = sign (qis)
            2) qob = sign (qos)
            3) ib = sign (i)
            4) isb = sign(is_)
            5) idg = max (id1, 0)
               idl = max (id2, 0)
            6) 标出每个skc的全码org
                qfsb = sign(qfs)  ####？？ fullsize 是指的三连码
                标出在移动之后 所有的主码 变成断码 bsb
            7)  if bsb = 0, ibs = 0, else if bsb = 1, ibs = is_
            8)  qpb = sign(qsp)

        约束：
            1）移出量总和不能超过期初库存
                每个skc的移出量 qos < =  max (i0, 0)
            2）移入量 不能导致 最终的库存 高于 目标库存
                每个sks的移入之和 qis < = max(0, 目标库存 it - (期初库存 i0+ 在途库存 r))
            3）移入 和 移出 不能同时发生 (如果不这样算，多算了一边变量)
                qib + qob <= 1

######################################################################
十二、 更新
    再次执行 第九步，更新所有的数据
    q, i0_aft_mov, io_aft_mov = psp.postp_data(q, q_trans, i0_aft_mov, io_aft_mov)

######################################################################
十三、 统计
    输出信息：版本、更新、决策时间、运行时间
    补货：
        移动的数量之和
        包裹的数量之和
        skc的数量之和
        调出的RDC的数目之和
        补货的门店的数目之和
    调拨：
        移动的数量之和
        包裹的数量之和
        skc的数量之和
        调出的RDC的数目之和
        补货的门店的数目之和




