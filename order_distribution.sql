
/* ==================================================================================================
说明：四仓订单配货&发货时长
作者：王政鸣
更新时间：2017-10-09
sql脚本类型：impala
 */

-- 1. 四仓订单配货&发货时长简报所需数据 ==========================================================================
WITH 
t1 AS
(SELECT order_id
        ,order_sn
        ,depod_id AS depot_id
        ,unix_timestamp((CASE WHEN pay_id = 41 THEN pay_time ELSE result_pay_time END), 'yyyy-MM-dd HH:mm:ss') AS pay_time     -- 支付时间
        ,unix_timestamp(shipping_time) AS shipping_time
        ,(CASE WHEN is_shiped = 1 THEN 'shiped' ELSE 'not_shiped' END) AS is_shiped
FROM zydb.dw_order_sub_order_fact
WHERE pay_status IN (1, 3)
    AND order_status = 1
    AND depod_id IN (4, 5, 6, 7, 14)
),
t2 AS
(SELECT *
FROM t1
WHERE pay_time >= unix_timestamp('2016-01-01', 'yyyy-MM-dd')
    AND pay_time <= unix_timestamp(FROM_UNIXTIME(unix_timestamp(), 'yyyy-MM-dd'))
),

-- 定时任务创建时间（定时任务的创建时间为配货完成时间）
-- 发现部分订单有多个gmt_created
-- CN仓
t3 AS
(SELECT order_id
        ,MAX(gmt_created) AS assign_time_cn
FROM jolly.who_wms_outing_stock_detail
GROUP BY order_id
),
-- SA仓
t4 AS
(SELECT order_id
        ,MAX(gmt_created) AS assign_time_sa
FROM jolly_wms.who_wms_outing_stock_detail
GROUP BY order_id
),

-- 计算duration
t5 AS
(SELECT t2.*
        ,t3.assign_time_cn
        ,t4.assign_time_sa
        ,FROM_UNIXTIME(t2.pay_time, 'yyyy-MM-dd') AS data_date
        ,(COALESCE(t3.assign_time_cn, t4.assign_time_sa) - t2.pay_time)/3600 AS assign_duration
        ,((CASE WHEN t2.shipping_time = 0 THEN NULL ELSE t2.shipping_time END) - t2.pay_time)/3600 AS ship_duration
FROM t2 
LEFT JOIN t3 ON t2.order_id = t3.order_id
LEFT JOIN t4 ON t2.order_id = t4.order_id
),

-- 对duration进行分组
t6 AS
(SELECT t5.*
        ,(CASE WHEN assign_duration IS NULL THEN 'No'
                     WHEN assign_duration <= 24 THEN '<=24h'
                     WHEN assign_duration <= 48 THEN '24-48h'
                     WHEN assign_duration <= 72 THEN '48-72h'
                     WHEN assign_duration <= 96 THEN '72-96h'
                     WHEN assign_duration <= 120 THEN '96-120h'
                     WHEN assign_duration <= 144 THEN '120-144h'
                     ELSE '144+h'
          END) AS assign_duration_class
        ,(CASE WHEN ship_duration IS NULL THEN 'No'
                     WHEN ship_duration <= 24 THEN '<=24h'
                     WHEN ship_duration <= 48 THEN '24-48h'
                     WHEN ship_duration <= 72 THEN '48-72h'
                     WHEN ship_duration <= 96 THEN '72-96h'
                     WHEN ship_duration <= 120 THEN '96-120h'
                     WHEN ship_duration <= 144 THEN '120-144h'
                     ELSE '144+h'
          END) AS ship_duration_class
FROM t5
),

-- 结果表
t100 AS 
(SELECT data_date
        ,depot_id
        ,assign_duration_class
        ,ship_duration_class
        ,COUNT(order_id) AS order_num
        ,SUM(assign_duration) AS assign_duration
        ,SUM(ship_duration) AS ship_duration
FROM t6
GROUP BY data_date
        ,depot_id
        ,assign_duration_class
        ,ship_duration_class
)

SELECT * 
FROM t100
ORDER BY data_date
        ,depot_id
        ,assign_duration_class
        ,ship_duration_class;

--  =============================1. 四仓订单配货&发货时长简报所需数据（底）=============================================

-- 确定订单是否完成配货及配货时间
-- 看看分组情况
t6 AS
(SELECT t5.order_id
        ,t5.depot_id
        ,FROM_UNIXTIME(t5.pay_time, 'yyyy-MM-dd') AS pay_date
        ,FROM_UNIXTIME(t5.pay_time) AS pay_time
        ,(CASE WHEN t5.assign_time IS NULL AND t5.min_modify_time IS NULL THEN 'a1b1'
                   WHEN t5.assign_time IS NULL AND t5.min_modify_time = 0 THEN 'a1b2'
                   WHEN t5.assign_time IS NULL AND t5.min_modify_time > 0 THEN 'a1b3'
                   WHEN t5.assign_time IS NOT NULL AND t5.min_modify_time IS  NULL THEN 'a2b1'
                   WHEN t5.assign_time IS NOT NULL AND t5.min_modify_time = 0 THEN 'a2b2'
                   WHEN t5.assign_time IS NOT NULL AND t5.min_modify_time > 0 THEN 'a2b3'
                   ELSE "其他"
          END) AS order_type
FROM t5
),
SELECT pay_date
        ,order_type
        ,COUNT(order_id)
FROM t6
GROUP BY pay_date
        ,order_type

-- 再来一个，区分采购和调拨
t AS
(
SELECT t5.order_id
        ,t5.order_sn
        ,t5.order_status
        ,t5.depot_id
        ,FROM_UNIXTIME(t5.pay_time, 'yyyy-MM-dd') AS pay_date
        ,FROM_UNIXTIME(t5.pay_time) AS pay_time
        ,FROM_UNIXTIME(t5.pur_assign_time) AS pur_assign_time
        ,FROM_UNIXTIME(t5.allo_assign_time) AS allo_assign_time
        ,FROM_UNIXTIME(t5.assign_time2) AS assign_time2
        ,(CASE WHEN t5.pur_assign_time IS NOT NULL AND t5.allo_assign_time IS NULL THEN 'pur'
                   WHEN t5.pur_assign_time IS NULL AND t5.allo_assign_time IS NOT NULL THEN 'allo'
                   WHEN t5.pur_assign_time IS NOT NULL AND t5.allo_assign_time IS NOT NULL THEN 'pur AND allo'
                   ELSE 'NOT pur AND allo' 
          END) AS order_type
        ,(CASE WHEN t5.pur_assign_time IS NOT NULL AND t5.allo_assign_time IS NULL THEN t5.pur_assign_time
                   WHEN t5.pur_assign_time IS NULL AND t5.allo_assign_time IS NOT NULL THEN t5.allo_assign_time
                   WHEN t5.pur_assign_time IS NOT NULL AND t5.allo_assign_time IS NOT NULL THEN greatest(t5.pur_assign_time, t5.allo_assign_time)
                   ELSE t5.assign_time2
          END) AS final_assign_time
        ,(CASE WHEN t5.pur_assign_time IS NOT NULL AND t5.allo_assign_time IS NULL THEN (t5.pur_assign_time - t5.pay_time)/3600
                   WHEN t5.pur_assign_time IS NULL AND t5.allo_assign_time IS NOT NULL THEN (t5.allo_assign_time - t5.pay_time)/3600
                   WHEN t5.pur_assign_time IS NOT NULL AND t5.allo_assign_time IS NOT NULL THEN (greatest(t5.pur_assign_time, t5.allo_assign_time)- t5.pay_time)/3600
                   ELSE (t5.assign_time2 - t5.pay_time)/3600
          END) AS pay_assign_duration
        /*,FROM_UNIXTIME(coalesce(t5.assign_time1, t5.assign_time2)) AS assign_time3
        ,(coalesce(t5.assign_time1, t5.assign_time2) - t5.pay_time)/3600 AS pay_assign_duration*/
FROM t5
)

-- 比较一下只需采购、只需调拨、需采购和调拨、无需采购和调拨等的配货时长差异
SELECT order_type
        ,depot_id
        ,COUNT(order_id) AS order_num
        ,SUM(pay_assign_duration) AS total_duration
        ,AVG(pay_assign_duration) AS avg_duration
FROM t
GROUP BY order_type
        ,depot_id;


/* ==============================================================================
说明：订单需采购商品数（未命中）
作者：王政鸣
更新时间：2017-9-15
sql脚本类型：impala
 */

WITH 
-- 1.查询订单
t1 AS
(
SELECT order_id
        ,order_sn
        ,depod_id AS depot_id
        ,goods_number AS goods_num
        ,unix_timestamp((CASE WHEN pay_id = 41 THEN pay_time ELSE result_pay_time END), 'yyyy-MM-dd HH:mm:ss') AS pay_time     -- 支付时间
FROM zydb.dw_order_sub_order_fact
WHERE pay_status IN (1, 3)
    AND depod_id IN (4, 5, 6)
    AND order_status = 1
),
t2 AS
(
SELECT t1.order_id
        ,t1.order_sn
        ,t1.depot_id
        ,t1.goods_num
        ,t1.pay_time
        ,SUM(t2.original_goods_number) AS original_goods_num
FROM t1
LEFT JOIN jolly.who_order_goods t2 ON t1.order_id = t2.order_id
WHERE t1.pay_time >= unix_timestamp('2017-06-14', 'yyyy-MM-dd')
    AND t1.pay_time <= unix_timestamp('2017-07-19', 'yyyy-MM-dd')
GROUP BY t1.order_id
        ,t1.order_sn
        ,t1.depot_id
        ,t1.goods_num
        ,t1.pay_time
),
-- 2.锁定表取采购和调拨的商品数量
t3 AS
(
SELECT order_id
        ,SUM(CASE WHEN source_type = 1 THEN org_num ELSE NULL END) AS pur_num
        ,SUM(CASE WHEN source_type = 2 THEN org_num ELSE NULL END) AS tra_num
FROM jolly.who_wms_goods_need_lock_detail t3
GROUP BY order_id
),
-- 明细表
t AS
(SELECT t2.*
        ,FROM_UNIXTIME(t2.pay_time, 'yyyy-MM-dd') AS pay_date
        ,t3.pur_num
        ,t3.tra_num
FROM t2
LEFT JOIN t3 ON t2.order_id = t3.order_id
)
-- 结果表
SELECT pay_date
        ,depot_id
        ,goods_num
        ,original_goods_num
        ,pur_num
        ,tra_num
        ,COUNT(order_id) AS order_num
FROM t
GROUP BY pay_date
        ,depot_id
        ,goods_num
        ,original_goods_num
        ,pur_num
        ,tra_num
order by pay_date;


-- 查询一级类目的关联销售情况
WITH 
-- 固定日期范围的有效订单
t1 AS
(SELECT order_id
FROM jolly.who_order_info p1
WHERE p1.add_time >= unix_timestamp('2017-07-12', 'yyyy-MM-dd')
    AND p1.add_time <= unix_timestamp('2017-07-19', 'yyyy-MM-dd')
    AND order_status IN (1,3)
    AND pay_status = 1
),
-- 各商品的一级类目
t2 AS
(SELECT p1.order_id
        ,p1.goods_id
        ,p2.cat_level1_name AS cat_name
FROM jolly.who_order_goods p1
LEFT JOIN zydb.dim_jc_goods p2 ON p1.goods_id = p2.goods_id
WHERE p1.order_id IN (SELECT * FROM t1)
),
-- 汇总，得到各订单购买的一级类目
t3 AS
(SELECT order_id 
        ,cat_name
FROM t2
GROUP BY order_id
        ,cat_name
)
SELECT *
FROM t3;

-- 一级类目的关联销售规则提升度低，基本没有可应用的规则
-- 下沉到二级类目来瞧一瞧：
WITH 
-- 固定日期范围的有效订单
t1 AS
(SELECT order_id
FROM jolly.who_order_info p1
WHERE p1.add_time >= unix_timestamp('2017-07-12', 'yyyy-MM-dd')
    AND p1.add_time <= unix_timestamp('2017-07-19', 'yyyy-MM-dd')
    AND order_status IN (1,3)
    AND pay_status = 1
),
-- 各商品的二级类目
t2 AS
(SELECT p1.order_id
        ,p1.goods_id
        ,p2.cat_level1_name AS cat1_name
        ,p2.cat_level2_name AS cat2_name
        ,concat_ws(' - ', p2.cat_level1_name, p2.cat_level2_name) AS cat12_name
        ,goods_number
        ,(goods_number * goods_price) AS goods_amount
FROM jolly.who_order_goods p1
LEFT JOIN zydb.dim_jc_goods p2 ON p1.goods_id = p2.goods_id
WHERE p1.order_id IN (SELECT * FROM t1)
),
-- 汇总，得到各订单购买的一级类目
t3 AS
(SELECT order_id 
        ,cat1_name
        ,cat2_name
        ,cat12_name
        ,SUM(goods_number) AS goods_number
        ,SUM(goods_amount) AS goods_amount
FROM t2
GROUP BY order_id
        ,cat1_name
        ,cat2_name
        ,cat12_name
)
SELECT *
FROM t3;

-- 商品级别
WITH 
-- 固定日期范围的有效订单
t1 AS
(SELECT order_id
FROM jolly.who_order_info p1
WHERE p1.add_time >= unix_timestamp('2017-07-12', 'yyyy-MM-dd')
    AND p1.add_time <= unix_timestamp('2017-07-19', 'yyyy-MM-dd')
    AND order_status IN (1,3)
    AND pay_status = 1
),
-- 各商品的一级类目
t2 AS
(SELECT p1.order_id
        ,concat_ws(' - ', p2.cat_level2_name, 
                                  cast(p1.goods_id AS string)) AS cat2_goods_id
        ,p2.cat_level1_name AS cat1_name
        ,p2.cat_level2_name AS cat2_name
        ,concat_ws(' - ', p2.cat_level1_name, p2.cat_level2_name) AS cat12_name
FROM jolly.who_order_goods p1
LEFT JOIN zydb.dim_jc_goods p2 ON p1.goods_id = p2.goods_id
WHERE p1.order_id IN (SELECT * FROM t1)
),
-- 汇总，得到各订单购买的一级类目
t3 AS
(SELECT order_id 
        ,cat1_name
        ,cat2_name
        ,cat12_name
        ,cat2_goods_id
FROM t2
GROUP BY order_id
        ,cat1_name
        ,cat2_name
        ,cat12_name
        ,cat2_goods_id
)
SELECT *
FROM t3;


-- 命中/未命中商品/商家聚类分析
-- 目的：探索未命中商品和商家的特点

-- 1.商品
WITH 
-- 训练数据的订单发生日期（下单日期）
t1 AS
(SELECT order_id
FROM jolly.who_order_info p1
WHERE p1.add_time >= unix_timestamp('2017-07-12', 'yyyy-MM-dd')
    AND p1.add_time <= unix_timestamp('2017-07-19', 'yyyy-MM-dd')
    AND order_status IN (1,3)
    AND pay_status = 1
),
-- 采购数量（剔除调拨）
-- 发现表中有重复的order_goods_rec_id，因此求和num
t2 AS
(SELECT order_goods_rec_id
        ,SUM(org_num) AS pur_num
FROM jolly.who_wms_goods_need_lock_detail
WHERE source_type = 1
GROUP BY order_goods_rec_id
),
-- 训练数据的订单发生日期的上一周订单
t3 AS
(SELECT order_id
FROM jolly.who_order_info p1
WHERE p1.add_time >= unix_timestamp('2017-07-05', 'yyyy-MM-dd')
    AND p1.add_time <= unix_timestamp('2017-07-12', 'yyyy-MM-dd')
    AND order_status IN (1,3)
    AND pay_status = 1
),
-- 训练数据的订单发生日期的上一周商品销售明细
t4 AS
(SELECT sku_id
          ,SUM(original_goods_number) AS org_num_7
FROM jolly.who_order_goods p1
WHERE p1.order_id IN (SELECT * FROM t3)
GROUP BY sku_id
),
-- 商品下单数量、采购数量
t5 AS
(SELECT p1.order_id
        ,p1.rec_id
        ,p1.goods_id
        ,p1.sku_id
        ,p2.shop_price
        ,p2.provide_code
        ,p2.is_new
        ,p2.is_stock
        ,p2.level
        ,DATEDIFF('2017-07-18 23:59:59', p2.first_on_sale_time) AS on_sale_day
        ,p2.cat_level1_name
        ,t4.org_num_7
        ,p1.original_goods_number AS org_num
        ,t2.pur_num
FROM jolly.who_order_goods p1
LEFT JOIN t2 ON p1.rec_id = t2.order_goods_rec_id
LEFT JOIN t4 ON p1.sku_id = t4.sku_id
LEFT JOIN zydb.dim_jc_goods p2 ON p1.goods_id = p2.goods_id
WHERE p1.order_id IN (SELECT * FROM t1)
)
-- 结果表
SELECT *
FROM t5;







-- 所有仓库订单配货和发货时间结构 ====================================
-- 发运是以每天发运订单来计算的，而不是以每天的支付订单
-- 1.配货 =====================================================
WITH 
t1 AS
(SELECT order_id
        ,order_sn
        ,depod_id AS depot_id
        ,unix_timestamp((CASE WHEN pay_id = 41 THEN pay_time ELSE result_pay_time END), 'yyyy-MM-dd HH:mm:ss') AS pay_time     -- 支付时间
        ,unix_timestamp(shipping_time) AS shipping_time
        ,is_shiped
FROM zydb.dw_order_sub_order_fact
WHERE pay_status IN (1, 3)
    AND order_status = 1
    AND depod_id IN (4, 5, 6, 7)
),
t2 AS
(SELECT *
FROM t1
WHERE pay_time >= unix_timestamp('2017-04-01', 'yyyy-MM-dd')
    AND pay_time <= unix_timestamp(FROM_UNIXTIME(unix_timestamp(), 'yyyy-MM-dd'))
),

-- 定时任务创建时间（定时任务的创建时间为配货完成时间）
-- 发现部分订单有多个gmt_created
-- CN仓
t3 AS
(SELECT order_id
        ,MAX(gmt_created) AS assign_time_cn
FROM jolly.who_wms_outing_stock_detail
GROUP BY order_id
),
-- SA仓
t4 AS
(SELECT order_id
        ,MAX(gmt_created) AS assign_time_sa
FROM jolly_wms.who_wms_outing_stock_detail
GROUP BY order_id
),

-- 计算duration
t5 AS
(SELECT t2.*
        ,t3.assign_time_cn
        ,t4.assign_time_sa
        ,FROM_UNIXTIME(t2.pay_time, 'yyyy-MM-dd') AS data_date
        ,(COALESCE(t3.assign_time_cn, t4.assign_time_sa) - t2.pay_time)/3600 AS duration
FROM t2 
LEFT JOIN t3 ON t2.order_id = t3.order_id
LEFT JOIN t4 ON t2.order_id = t4.order_id
),

-- 对duration进行分组
t6 AS
(SELECT t5.*
        ,(CASE WHEN duration IS NULL THEN 'No'
                     WHEN duration <= 24 THEN '<=24h'
                     WHEN duration <= 48 THEN '24-48h'
                     WHEN duration <= 72 THEN '48-72h'
                     WHEN duration <= 96 THEN '72-96h'
                     WHEN duration <= 120 THEN '96-120h'
                     WHEN duration <= 144 THEN '120-144h'
                     ELSE '144+h'
          END) AS duration_class
FROM t5
),
-- 配货结果表
t100 AS 
(SELECT data_date
        ,depot_id
        ,duration_class
        ,'assign-pay' AS type
        ,COUNT(order_id) AS order_num
        ,SUM(duration) AS duration
FROM t6
GROUP BY data_date
        ,depot_id
        ,duration_class
),

-- 2.发货 =====================================================

t202 AS
(SELECT t1.*
        ,FROM_UNIXTIME(t1.shipping_time, 'yyyy-MM-dd') AS data_date
        ,(t1.shipping_time - t1.pay_time)/3600 AS duration
FROM t1
WHERE is_shiped = 1
    AND shipping_time >= unix_timestamp('2017-04-01', 'yyyy-MM-dd')
    AND shipping_time <= unix_timestamp(FROM_UNIXTIME(unix_timestamp(), 'yyyy-MM-dd'))
),
t203 AS
(SELECT t202.*
        ,(CASE WHEN duration IS NULL THEN 'No'
                     WHEN duration <= 24 THEN '<=24h'
                     WHEN duration <= 48 THEN '24-48h'
                     WHEN duration <= 72 THEN '48-72h'
                     WHEN duration <= 96 THEN '72-96h'
                     WHEN duration <= 120 THEN '96-120h'
                     WHEN duration <= 144 THEN '120-144h'
                     ELSE '144+h'
          END) AS duration_class
FROM t202
),

-- 发货结果表
t200 AS 
(SELECT data_date
        ,depot_id
        ,duration_class
        ,'ship-pay' AS type
        ,COUNT(order_id) AS order_num
        ,SUM(duration) AS duration
FROM t203
GROUP BY data_date
        ,depot_id
        ,duration_class
),

-- 配货 + 发货 结果表
t000 AS
(SELECT * 
FROM t100
UNION ALL 
SELECT * 
FROM t200
)

SELECT *
FROM t000
ORDER BY type
        ,data_date
        ,depot_id
        ,duration_class;




-- 所有仓库订单配货和发货时间结构 ====================================
-- 以每天支付订单，统计其配货完成和发运时间
WITH 
-- 支付时间、人工标非时间、打包时间、发运时间
t1 AS
(SELECT p1.order_id
        ,p1.order_sn
        ,p1.depod_id AS depot_id
        ,(CASE WHEN p1.pay_id = 41 THEN 'COD' ELSE 'NCOD' END) AS pay_name
        ,unix_timestamp((CASE WHEN p1.pay_id = 41 THEN p1.pay_time ELSE p1.result_pay_time END), 'yyyy-MM-dd HH:mm:ss') AS pay_time     -- 支付时间
        ,unix_timestamp(p1.no_problems_order_uptime) AS no_problems_order_uptime
        ,unix_timestamp(p1.order_pack_time) AS pack_time
        ,unix_timestamp(p1.shipping_time) AS shipping_time
        ,(CASE WHEN p1.is_shiped = 1 THEN 'shiped' ELSE 'not_shiped' END) AS is_shiped
        ,p2.rec_id
FROM zydb.dw_order_sub_order_fact p1
LEFT JOIN jolly.who_cod_order_check_feedback p2 
            ON p1.order_id = p2.order_id
WHERE p1.pay_status IN (1, 3)
    AND p1.order_status = 1
    AND p1.depod_id IN (4, 5, 6, 7)
),
t2 AS
(SELECT *
        ,(no_problems_order_uptime - pay_time)/3600 AS check_pay_duration
        ,(CASE WHEN rec_id IS NULL THEN 'Dont_Need_Check' ELSE 'Need_Check' END) AS is_need_check
FROM t1
WHERE pay_time >= unix_timestamp('2017-04-01', 'yyyy-MM-dd')
    AND pay_time <= unix_timestamp(FROM_UNIXTIME(unix_timestamp(), 'yyyy-MM-dd'))
),

-- 定时任务创建时间（定时任务的创建时间为配货完成时间，即可拣货时间）
-- 发现部分订单有多个gmt_created
-- CN仓
t3 AS
(SELECT order_id
        ,MAX(gmt_created) AS assign_time_cn
FROM jolly.who_wms_outing_stock_detail
GROUP BY order_id
),
-- SA仓
t4 AS
(SELECT order_id
        ,MAX(gmt_created) AS assign_time_sa
FROM jolly_wms.who_wms_outing_stock_detail
GROUP BY order_id
),

-- 拣货完成时间
-- CN仓
t7 AS
(SELECT p2.order_id
        ,P2.order_sn
        ,MAX(p1.gmt_created) AS pick_begin_time_cn  -- 拣货单生成时间
        ,MAX(p1.finish_time) AS pick_finish_time_cn --拣货完成时间
FROM jolly.who_wms_picking_info P1
INNER JOIN jolly.who_wms_picking_goods_detail p2 
               ON p1.picking_id=p2.picking_id
GROUP BY p2.order_id
        ,P2.order_sn
),
-- SA仓
t8 AS
(SELECT p2.order_id
        ,P2.order_sn
        ,MAX(p1.gmt_created) AS pick_begin_time_sa  -- 拣货单生成时间
        ,MAX(p1.finish_time) AS pick_finish_time_sa --拣货完成时间
FROM jolly_wms.who_wms_picking_info P1
INNER JOIN jolly_wms.who_wms_picking_goods_detail p2 
               ON p1.picking_id=p2.picking_id
GROUP BY p2.order_id
        ,P2.order_sn
),

-- 计算duration
t5 AS
(SELECT t2.*
        ,t3.assign_time_cn
        ,t4.assign_time_sa
        ,t7.pick_begin_time_cn
        ,t7.pick_finish_time_cn
        ,t8.pick_begin_time_sa
        ,t8.pick_finish_time_sa
        ,FROM_UNIXTIME(t2.pay_time, 'yyyy-MM-dd') AS data_date
        ,(COALESCE(t3.assign_time_cn, t4.assign_time_sa) - t2.pay_time)/3600 AS assign_pay_duration
        ,(COALESCE(t7.pick_begin_time_cn, t8.pick_begin_time_sa) - COALESCE(t3.assign_time_cn, t4.assign_time_sa))/3600 AS pick_begin_assign_duration
        ,(COALESCE(t7.pick_finish_time_cn, t8.pick_finish_time_sa) - COALESCE(t3.assign_time_cn, t4.assign_time_sa))/3600 AS pick_finish_assign_duration
        ,(t2.pack_time - COALESCE(t7.pick_finish_time_cn, t8.pick_finish_time_sa))/3600 AS pack_pick_finish_duration
        ,(t2.shipping_time - t2.pack_time)/3600 AS ship_pack_duration
        ,((CASE WHEN t2.shipping_time = 0 THEN NULL ELSE t2.shipping_time END) - t2.pay_time)/3600 AS ship_pay_duration
FROM t2 
LEFT JOIN t3 ON t2.order_id = t3.order_id
LEFT JOIN t4 ON t2.order_id = t4.order_id
LEFT JOIN t7 ON t2.order_id = t7.order_id
LEFT JOIN t8 ON t2.order_id = t8.order_id
),

-- 对duration进行分组
t6 AS
(SELECT t5.*
        ,(CASE WHEN assign_duration IS NULL THEN 'No'
                     WHEN assign_duration <= 24 THEN '<=24h'
                     WHEN assign_duration <= 48 THEN '24-48h'
                     WHEN assign_duration <= 72 THEN '48-72h'
                     WHEN assign_duration <= 96 THEN '72-96h'
                     WHEN assign_duration <= 120 THEN '96-120h'
                     WHEN assign_duration <= 144 THEN '120-144h'
                     ELSE '144+h'
          END) AS assign_duration_class
        ,(CASE WHEN ship_duration IS NULL THEN 'No'
                     WHEN ship_duration <= 24 THEN '<=24h'
                     WHEN ship_duration <= 48 THEN '24-48h'
                     WHEN ship_duration <= 72 THEN '48-72h'
                     WHEN ship_duration <= 96 THEN '72-96h'
                     WHEN ship_duration <= 120 THEN '96-120h'
                     WHEN ship_duration <= 144 THEN '120-144h'
                     ELSE '144+h'
          END) AS ship_duration_class
FROM t5
),
-- 结果表
t100 AS 
(SELECT data_date
        ,depot_id
        ,assign_duration_class
        ,ship_duration_class
        ,COUNT(order_id) AS order_num
        ,SUM(assign_duration) AS assign_duration
        ,SUM(ship_duration) AS ship_duration
FROM t6
GROUP BY data_date
        ,depot_id
        ,assign_duration_class
        ,ship_duration_class
)

SELECT * 
FROM t100
ORDER BY data_date
        ,depot_id
        ,assign_duration_class
        ,ship_duration_class;

-- 各仓每天订单数和作业时长
SELECT data_date
    ,depot_id
    ,is_need_check
    ,pay_name
    ,COUNT(order_id) AS order_num
    ,SUM(check_pay_duration) AS check_pay_duration
    ,SUM(assign_pay_duration) AS assign_pay_duration
    ,SUM(pick_begin_assign_duration) AS pick_begin_assign_duration
    ,SUM(pick_finish_assign_duration) AS pick_finish_assign_duration
    ,SUM(pack_pick_finish_duration) AS pack_pick_finish_duration
    ,SUM(ship_pack_duration) AS ship_pack_duration
    ,SUM(ship_pay_duration) AS ship_pay_duration
FROM t5
WHERE is_shiped = 'shiped'
     AND pay_time > 0
     AND no_problems_order_uptime > 0
     AND (assign_time_sa > 0 OR assign_time_cn > 0)
     AND (pick_begin_time_sa > 0 OR pick_begin_time_cn > 0)
     AND (pick_finish_time_sa > 0 OR pick_finish_time_cn > 0)
     AND pack_time > 0
     AND shipping_time > 0
     AND depot_id = 7
GROUP BY data_date
    ,depot_id
    ,is_need_check
    ,pay_name
ORDER BY data_date
    ,depot_id
    ,is_need_check
    ,pay_name;



UNIX_TIMESTAMP(TRUNC(NOW(), 'DD'))
SELECT p4.region_name
        ,p1.depot_id
        ,COUNT(DISTINCT order_id) AS order_num
        ,




FROM zydb.dw_order_node_time p1
LEFT JOIN jolly.who_order_user_info p3
            ON p1.order_id = p3.order_id
LEFT JOIN (SELECT region_id, region_name
                    FROM jolly.who_region
                    WHERE region_type = 0
                    AND region_status = 1) p4
            ON p3.country = p4.region_id
WHERE p1.pay_time >= '2017-07-01'
     AND p1.pay_time < '2017-08-01'
     AND p1.is_shiped = 1
     AND p1.order_status = 1
     AND p1.pay_status IN (1, 3)
     AND p1.depot_id IN (4, 5)






--- 7月大陆仓作业时长
select depot_id
    ,avg(receipt_quality_duration + quality_onshelf_duration + picking_duration + package_duration + shipping_duration) as LT2
from zydb.rpt_depot_daily_report
where depot_id in (4, 5)
--  and data_date >= '2017-07-01'
--  and data_date <= '2017-07-31'
group by depot_id;


-- 对duration进行分组（按天）
t6 AS
(SELECT t5.*
        ,(CASE WHEN ship_duration IS NULL THEN 'No'
                     WHEN ship_duration <= 24 THEN '1天'
                     WHEN ship_duration <= 48 THEN '2天'
                     WHEN ship_duration <= 72 THEN '3天'
                     WHEN ship_duration <= 96 THEN '4天'
                     WHEN ship_duration <= 120 THEN '5天'
                     WHEN ship_duration <= 144 THEN '6天'
                     WHEN ship_duration <= 168 THEN '7天'
                     WHEN ship_duration <= 192 THEN '8天'
                     WHEN ship_duration <= 216 THEN '9天'
                     WHEN ship_duration <= 240 THEN '10天'
                     WHEN ship_duration <= 264 THEN '11天'
                     WHEN ship_duration <= 288 THEN '12天'
                     WHEN ship_duration <= 312 THEN '13天'
                     WHEN ship_duration <= 336 THEN '14天'
                     WHEN ship_duration <= 360 THEN '15天'
                     ELSE '15+天'
          END) AS ship_duration_class
FROM t5
),