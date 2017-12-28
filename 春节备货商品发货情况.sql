/*
内容：春节备货商品的发货情况
说明：2017年春节备货的采购需求，从12月19日开始生成，采购类型pur_type=11(节日采购)
作者：Neo王政鸣
 */

--   impala ==================================

WITH 
-- 01.备货需求
t1 AS
(SELECT p1.rec_id
        ,p2.pur_order_goods_rec_id          -- 同一个采购需求，也可能对应多个这个rec_id，这意味着多个采购单
        ,p1.goods_id
        ,p1.sku_id
        ,p1.depot_id
        ,p1.supp_name
        ,p3.cat_level1_name
        ,p1.supp_num
        ,p1.oos_num
        ,p1.send_num
        ,FROM_UNIXTIME(p1.gmt_created) AS create_time   -- 需求生成时间
        ,FROM_UNIXTIME(p1.check_time) AS check_time     -- 需求审核时间
FROM jolly_spm.jolly_spm_pur_goods_demand p1
LEFT JOIN jolly_spm.jolly_spm_pur_goods_demand_relation p2
             ON p1.rec_id = p2.demand_rec_id
LEFT JOIN zydb.dim_jc_goods p3
             ON p1.goods_id = p3.goods_id
WHERE p1.pur_type = 11          -- 11表示节日备货
     AND p1.check_time >= UNIX_TIMESTAMP('2017-12-19', 'yyyy-MM-dd')       -- 春节备货的需求在12月19日开始生成需求
     --AND p1.check_time < UNIX_TIMESTAMP('2017-12-28', 'yyyy-MM-dd')       -- 春节备货的需求在12月19日开始生成需求
),
t2 AS
(SELECT t1.*
        ,p5.pur_order_sn
        ,FROM_UNIXTIME(p5.gmt_created) AS send_time        -- 供应商发货时间
        ,p8.tracking_no       -- 同一个采购需求，可能对应了多个tracking_no，这是因为有多个采购单
        ,FROM_UNIXTIME(p9.gmt_created) AS receipt_time      -- 到货签收时间
FROM t1
LEFT JOIN jolly_spm.jolly_spm_pur_order_goods p4        -- 采购单详情表
             ON t1.pur_order_goods_rec_id = p4.rec_id 
LEFT JOIN jolly_spm.jolly_spm_pur_order_info p5        -- 采购单表
             ON p4.pur_order_id = p5.pur_order_id 
LEFT JOIN jolly.who_wms_delivered_order_info p7        -- 发货单表
             ON p5.pur_order_sn = p7.delivered_order_sn
LEFT JOIN jolly.who_wms_delivered_order_tracking_info p8        -- 采购单物流信息表
             ON p7.delivered_order_id = p8.delivered_order_id
LEFT JOIN jolly.who_wms_delivered_receipt_info p9       -- 到货签收单
             ON TRIM(p8.tracking_no) = TRIM(p9.tracking_no)
),
-- 剔除重复的记录，看各环节的时间即可
t3 AS
(SELECT t2.rec_id
        ,t2.goods_id
        ,t2.sku_id
        ,t2.depot_id
        ,t2.supp_name
        ,t2.cat_level1_name
        ,t2.supp_num
        ,t2.oos_num
        ,t2.send_num
        ,t2.create_time
        ,t2.check_time
        ,MIN(send_time) AS min_send_time
        ,MIN(receipt_time) AS min_receipt_time
FROM t2
GROUP BY t2.rec_id
        ,t2.goods_id
        ,t2.sku_id
        ,t2.depot_id
        ,t2.supp_name
        ,t2.cat_level1_name
        ,t2.supp_num
        ,t2.oos_num
        ,t2.send_num
        ,t2.create_time
        ,t2.check_time
),
t401 AS 
-- 按照供应商，先不取sku_count
(SELECT supp_name
        ,COUNT(DISTINCT goods_id) AS goods_count
        ,SUM(supp_num) AS demand_num
        ,SUM(send_num) AS send_num
        ,SUM(CASE WHEN min_receipt_time IS NULL THEN 0 ELSE send_num END) AS receipt_num
        ,SUM(supp_num - send_num) AS didnt_send_num
        ,SUM(oos_num) AS oos_num
FROM t3
GROUP BY supp_name
),
t402 AS 
-- 按照供应商，取sku_count
(SELECT supp_name
        ,COUNT(DISTINCT sku_id) AS sku_count
FROM t3
GROUP BY supp_name
)
-- JOIN t401和t402
SELECT t401.supp_name
        ,t401.goods_count
        ,t402.sku_count
        ,t401.demand_num
        ,t401.send_num
        ,t401.receipt_num
        ,t401.didnt_send_num
        ,t401.oos_num
FROM t401, t402
WHERE t401.supp_name = t402.supp_name
;

t501 AS 
-- 按照一级类目汇总，先不取sku_count
(SELECT cat_level1_name
        ,COUNT(DISTINCT goods_id) AS goods_count
        ,SUM(supp_num) AS demand_num
        ,SUM(send_num) AS send_num
        ,SUM(CASE WHEN min_receipt_time IS NULL THEN 0 ELSE send_num END) AS receipt_num
        ,SUM(supp_num - send_num) AS didnt_send_num
        ,SUM(oos_num) AS oos_num
FROM t3
GROUP BY cat_level1_name
),
t502 AS 
-- 按照一级类目汇总，取sku_count
(SELECT cat_level1_name
        ,COUNT(DISTINCT sku_id) AS sku_count
FROM t3
GROUP BY cat_level1_name
)
-- JOIN t501和t502
SELECT t501.cat_level1_name
        ,t501.goods_count
        ,t502.sku_count
        ,t501.demand_num
        ,t501.send_num
        ,t501.receipt_num
        ,t501.didnt_send_num
        ,t501.oos_num
FROM t501, t502
WHERE t501.cat_level1_name = t502.cat_level1_name
;
