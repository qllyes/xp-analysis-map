SELECT  lev3_org_name                                                                   AS `取数维度（战区/集团）`
       ,dongxiao_war_num                                                                AS `动销战区数`
       ,dongxiao_war_detail                                                             AS `动销战区`
       ,stock_up_status_name_9000                                                       AS `9000备货状态`
       ,stock_up_status_name                                                            AS `战区最优备货状态` -- 原命名：备货状态 
       ,goods_code                                                                      AS `商品编码`
       ,goods_name                                                                      AS `商品名称`
       ,spec                                                                            AS `规格`
       ,factory_name                                                                    AS `生产厂家`
       ,meas_unit                                                                       AS `单位`
       ,cal_operate_cate_name                                                           AS `经营类别`
       ,cate_king_label                                                                 AS `是否品类王`
       ,fire_goods_label                                                                AS `火炬选品`
       ,aprl_no                                                                         AS `批准文号`
       ,bar_code                                                                        AS `国际条码`
       ,expiry_date_name                                                                AS `效期`
       ,medi_period                                                                     AS `服用天数`
       ,avg_sale_price / medi_period                                                    AS `日服用成交价（顾客）` -- 原命名：日服用量成本 
       ,avg_shop_dijia / medi_period                                                    AS `日服用底价` -- 原命名：日服用量成本 
       ,mc_content                                                                      AS `主要成份含量`
       ,mc_loading_qty                                                                  AS `主要成份装量`
       ,regoin_current_move_avg_price_nums_weighting                                    AS `仓库移动平均价` -- 标准单位零售定价【BD】 
       ,regoin_last_purc_price                                                          AS `仓库最后进价` -- 新老品标准单位【零售价】对比 
       ,current_move_avg_price_9000                                                     AS `9000的移动平均价`
       ,last_purc_price_9000                                                            AS `9000的最后进价`
       ,latest_base_price_90days                                                        AS `【使用最新】近90天购进批次的最新底价-不含销售返利`
       ,min_base_price_90days                                                           AS `【使用最低】近90天购进批次的最低底价`
       ,avg_base_price_90days                                                           AS `【使用平均】近90天购进批次的平均底价-含购进、配送、购进营销收入` -- 底价 (返利后) 
       ,avg_buyin_price                                                                 AS `进价` -- 原命名：平均进价 
       ,avg_sale_price                                                                  AS `实际成交价` -- 原命名：平均售价 
       ,avg_sale_price - avg_shop_dijia                                                 AS `单盒实际综合毛利额`
       ,(avg_sale_price - avg_shop_dijia) / avg_sale_price                              AS `实际成交综合毛利率`
       ,comm_lev_name                                                                   AS `实际提成级别`
       ,avg_sale_price / avg_sugg_retail_price                                          AS `折扣率`
       ,avg_sugg_retail_price                                                           AS `建议零售价`
       ,avg_sugg_retail_price - avg_shop_dijia                                          AS `建议零售价-综合毛利额`
       ,medical_insurance_type                                                          AS `国家医保目录`
       ,CONCAT ( jiangsu_wsuzhou,'/浙江:','江苏:',zhejiang,'/天津:',tianjin,'/上海:',shanghai ) AS `省医保目录`
       ,region_price                                                                    AS `省医保支付价`
       ,month_avg_qty90                                                                 AS `近90天月均销售数量`
       ,month_avg_amount90                                                              AS `近90天月均销售金额`
       ,month_avg_qt_profit                                                             AS `近90天月均前台含税毛利额`
       ,avg_zh_profit_90days_within                                                     AS `【最终毛利额】近90天月均综合毛利额-考虑下游支出`
       ,company_brand_level                                                             AS `厂牌等级`
       ,yf_jsfs                                                                         AS `结算方式`
       ,zhangqi_days                                                                    AS `账期天数`
       ,refund_jj                                                                       AS `退货条件`
       ,refund_rate                                                                     AS `可退比例`
       ,ht_yf                                                                           AS `供应商全称`
       ,chnl_attr_name                                                                  AS `渠道属性`
       ,purc_mode_name                                                                  AS `采购模式`
       ,purc_exec_dept_name                                                             AS `采购执行部门`
       ,jingying_companys_num                                                           AS `铺货公司数`
       ,war_puhuo_shop_all_nums                                                         AS `铺货门店数`
       ,super_flag_shop_dongxiao_shop_nums                                              AS `超级旗舰店动销门店数`
       ,flag_shop_dongxiao_shop_nums                                                    AS `旗舰店动销门店数`
       ,big_shop_dongxiao_shop_nums                                                     AS `大店动销门店数`
       ,medium_shop_dongxiao_shop_nums                                                  AS `中店动销门店数`
       ,small_shop_dongxiao_shop_nums                                                   AS `小店动销门店数`
       ,develop_shop_dongxiao_shop_nums                                                 AS `成长店动销门店数`
       ,super_flag_shop_total_shop_nums                                                 AS `超级旗舰店总门店数`
       ,flag_shop_total_shop_nums                                                       AS `旗舰店总门店数`
       ,big_shop_total_shop_nums                                                        AS `大店总门店数`
       ,medium_shop_total_shop_nums                                                     AS `中店总门店数`
       ,small_shop_total_shop_nums                                                      AS `小店总门店数`
       ,develop_shop_total_shop_nums                                                    AS `成长店总门店数`
       ,super_flag_shop_month_avg_qty                                                   AS `超级旗舰店月店均销量`
       ,flag_shop_month_avg_qty                                                         AS `旗舰店月店均销量`
       ,big_shop_month_avg_qty                                                          AS `大店月店均销量`
       ,medium_shop_month_avg_qty                                                       AS `中店月店均销量`
       ,small_shop_month_avg_qty                                                        AS `小店月店均销量`
       ,develop_shop_month_avg_qty                                                      AS `成长店月店均销量`
       ,stock_stock_qty                                                                 AS `仓库库存数量`
       ,shop_stock_qtys                                                                 AS `门店库存数量`
       ,goods_common_name                                                               AS `通用名`
       ,war_goods_common_name_skus                                                      AS `同通用名下sku数`
       ,war_goods_common_name_month_qty                                                 AS `通用名月均销量`
       ,war_goods_common_name_month_amount                                              AS `通用名月均销售额`
       ,war_goods_common_name_month_qt_profit                                           AS `通用名月均前台毛利额`
       ,common_zh_profit                                                                AS `通用名综合毛利额-含购进、配送、购进营销收入`
       ,common_zh_profit_rate                                                           AS `通用名综合毛利率-含购进、配送、购进营销收入`
       ,strategy_classify_name                                                          AS `三级策略分类`
       ,mc_name                                                                         AS `主要成份`
       ,jianzhuang                                                                      AS `件装`
       ,media_type_name                                                                 AS `剂型`
       ,goods_class_lev1_name                                                           AS `一级大类`
       ,strategy_zh_profit_rate                                                         AS `三级策略分类综合毛利率-含购进、配送、购进营销收入`
       ,mc_zh_profit_rate                                                               AS `主要成份综合毛利率-含购进、配送、购进营销收入`
       ,last30days_duanhuo_rate                                                         AS `近30天断货率`
       ,month_avg_cost                                                                  AS `近90天月均成本额`
       ,else_shop_dongxiao_shop_nums                                                    AS `其他店型店动销门店数`
       ,else_shop_month_avg_qty                                                         AS `其他店型店月店均销量`
       ,month_avg_good_instorage_num                                                    AS `月入库量`
       ,( super_flag_shop_dongxiao_shop_nums + flag_shop_dongxiao_shop_nums + big_shop_dongxiao_shop_nums + medium_shop_dongxiao_shop_nums + small_shop_dongxiao_shop_nums + develop_shop_dongxiao_shop_nums + else_shop_dongxiao_shop_nums ) AS `动销门店数`
       ,avg_base_price_with_downstream_expense                                          AS `近90天购进批次的平均底价-考虑下游支出` -- 新老品标准单位【底价】对比 
       ,sales_rebate_per_box                                                            AS `销售返` -- 标准单位成交价【BA】 
       ,mkt_income_per_box                                                              AS `营销收入金额` -- 新老品标准单位【成交价】对比 
       ,single_mention_income_per_box                                                   AS `单提收入金额` -- 标准单位综合毛利额 
       ,last_dijia_shoplevel_90                                                         AS `近90天门店最新一批的底价` -- 仓库移动平均价 
       ,lowest_dijia_shoplevel_90                                                       AS `近90天门店级最低底价` -- 仓库最后进价 
       ,avg_base_price_without_downstream_expense                                       AS `近90天购进批次的平均底价-不考虑下游支出` -- 新品：底价/对标品：最低底价 
       ,avg_zh_profit_90days                                                            AS `近90天月均综合毛利额-含购进、配送、购进营销收入` -- 会前会决议决议铺货通道 
       ,avg_zh_profit_90days_without                                                    AS `近90天月均综合毛利额-不考虑下游支出` -- 会前会决议铺货通道新品费 
       ,dt                                                                              AS `取数日期`
FROM new_product_review_all_allindex_v2_dfp
WHERE dt = (
SELECT  MAX(dt)
FROM new_product_review_all_allindex_v2_dfp) AND dongxiao_war_num > 0