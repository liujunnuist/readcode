  -----1产品信息表 done
  SELECT t1.product_code AS prod_id,
                                t1.color_code as color_id,
                                t1.size_code AS size,
                                t1.size_order,
                                t1.sku_year AS year,
                                t1.sku_quarter AS season_id,
                                t1.big_class AS class_0
                           FROM erke_shidian_edw_ai_dev.dim_sku AS t1
            
                           JOIN (select product_year, product_quarter 
                                 from erke_shidian_edw_ai_dev.config_target_product 
                                 where day_date = '2019-3-12') AS t2
                               ON t1.sku_year = t2.product_year and t1.sku_quarter = t2.product_quarter
                               
                               
                
-----2组织信息表 done
                          SELECT stockorg_code AS org_id, 
                           province AS prov_id, 
                           city AS city_id,
                                  reserved1 as dist_id,
                                  org_code as mng_reg_id,
                                  (case org_flag when 1 then 1 else 0 end) as is_store
                           FROM erke_shidian_edw_ai_dev.dim_stockorg
                           
                           WHERE (stockorg_code = '0001') OR (status = 'Y' AND org_flag = 1)
-----3.期初库存
					
                            with
                                -- 日末库存 
                                init_inv_data AS
                                 (SELECT product_code AS prod_id,
                                         color_code as color_id,
                                         size_code AS size,
                                         org_code as org_id,
                                         stock_qty AS i0
                                  FROM erke_shidian_edw_ai_dev.mid_day_end_stock
                                  WHERE stock_date = '{0}'),
    
                                -- 在途库存
                               init_inv_arr_data AS
                                 (SELECT product_code AS prod_id,
                                         color_code as color_id,
                                         size_code AS size,
                                         org_code as org_id,
                                         road_stock_qty AS r
                                  FROM erke_shidian_edw_ai_dev.mid_day_end_available_stock
                                  WHERE stock_date = '{0}'
                                  ),
    
                                prod_info_targ AS
                                (SELECT t1.product_code AS prod_id,
                                     t1.color_code as color_id,
                                     t1.size_code AS size
                                 FROM erke_shidian_edw_ai_dev.dim_sku AS t1
                                 JOIN (select product_year, product_quarter 
                                       from erke_shidian_edw_ai_dev.config_target_product 
                                       where day_date = '{0}') AS t2
                                       ON t1.sku_year = cast(t2.product_year as int) and t1.sku_quarter = t2.product_quarter
                                ),
    
                                 org_info_targ AS
                                 (SELECT distinct stockorg_code AS org_id
                                  FROM abu_opff_edw_ai_dev.dim_storeorg
                                  WHERE (stockorg_code = 'code0812') OR (status = 'Y' AND org_flag = '1'))
    
                            SELECT t3.prod_id, t3.color_id, t3.size, t3.org_id, t3.i0, t3.r
                            FROM (SELECT COALESCE(t1.prod_id, t2.prod_id) AS prod_id,
                                       COALESCE(t1.color_id, t2.color_id) AS color_id,
                                       COALESCE(t1.size, t2.size) AS size,
                                       COALESCE(t1.org_id, t2.org_id) AS org_id,
                                       COALESCE(t1.i0, 0) AS i0,
                                       COALESCE(t2.r, 0) AS r
                                FROM init_inv_data AS t1
                                LEFT JOIN init_inv_arr_data AS t2
                                ON t1.prod_id = t2.prod_id AND
                                   t1.color_id = t2.color_id AND
                                   t1.size = t2.size AND
                                   t1.org_id = t2.org_id) AS t3
                            JOIN prod_info_targ AS t4
                            ON t3.prod_id = t4.prod_id AND t3.color_id = t4.color_id AND t3.size = t4.size
                            JOIN org_info_targ AS t5
                            ON t3.org_id = t5.org_id
                           
                           
                           
                           
                           
                           
                           
                           
                           
                           
                           
                           
----- 4.销售表 done
							
                           WITH 
                                prod_info_targ AS
                                (SELECT t1.product_code AS prod_id,
                                     t1.color_code as color_id,
                                     t1.size_code AS size
                                 FROM erke_shidian_edw_ai_dev.dim_sku AS t1
                                 JOIN (select product_year, product_quarter 
                                       from erke_shidian_edw_ai_dev.config_target_product 
                                       where day_date = '2019-02-28') AS t2
                                       ON t1.sku_year = cast(t2.product_year as int) and t1.sku_quarter = t2.product_quarter
                                ),
    
                               org_info_targ AS
                                 (SELECT distinct stockorg_code AS org_id
                                  FROM erke_shidian_edw_ai_dev.dim_stockorg
                                  WHERE status = 'Y' AND org_flag = '1')
    
                              SELECT t1.product_code AS prod_id,
                                     t1.color_code AS color_id,
                                     t1.size_code AS size,
                                     t1.org_code AS org_id,
                                     t1.sale_date AS date_sell,
                                     SUM(COALESCE(t1.qty, 0)) AS s
                              FROM erke_shidian_edw_ai_dev.fct_sales AS t1
                             
                              GROUP BY t1.product_code, t1.color_code, t1.size_code, t1.org_code, t1.sale_date
                                  
                                   JOIN prod_info_targ AS t2
                              ON t1.product_code = t2.prod_id AND
                                 t1.color_code = t2.color_id AND
                                 t1.size_code = t2.size
                              JOIN org_info_targ AS t3
                              ON t1.org_code = t3.org_id
                              WHERE t1.sale_date BETWEEN '{0}' AND '{1}'
                                  
                                  
                                  
---------------5.移动数据表 done
							SELECT product_code AS prod_id,
                                 color_code as color_id,
                                 size_code AS size,
                                 send_org_code AS org_send_id,
                                 receive_org_code AS org_rec_id,
                                 send_date AS date_send,
                                 receive_date AS date_rec,
                                 COALESCE(send_qty, 0) AS qty_send,
                                 COALESCE(receive_qty, 0) AS qty_rec
                          FROM erke_shidian_edw_ai_dev.fct_stock_move
                          WHERE send_date BETWEEN '2018-01-01' and '2019-03-20' 
                          
                          
                                  
                                  
                                  
                                  
                                  
                                  
                                  
                                  
