/*
-- 主题：采购需求发货-到货-配货节奏分析
-- 内容：
-- 1. 采购需求各环节时间，数据用于分析发货到货对系统配货的影响，订单采购需求 --> 推送 --> 缺货 --> 发货 --> 到货 --> 系统配货
-- 2. 统计分析
-- 作者：Neo王政鸣
-- 时间：2018-1-11
 */

-- 1.明细数据 ===================================================================

-- 1.1 impala，没有跨越的揽件时间 ================================================
CREATE TABLE zybiro.neo_pur_lock_detail
AS
WITH
-- 1.支付订单（子单）
-- 子单中每个sku的原始商品数量和最终数量，支付时间
-- 注意：
-- (1).goods_number <> 0, oos_num = 0表示供应商标记缺货了，但是最终商品配货成功，标记缺货没有影响到最终配货，这种情况不会统计缺货率。
-- (2).goods_number = 0 or goods_number < original_goods_number, oos_num = 0表示供应商标记缺货了且影响到了商品配货，这种情况会统计缺货率
t101 AS
(SELECT p1.order_id
        ,p1.order_sn
        ,p1.depod_id AS depot_id
        ,p1.order_status
        ,p1.is_shiped
        ,(CASE WHEN p1.pay_id=41 THEN p1.pay_time ELSE p1.result_pay_time END) AS order_pay_time      -- 支付时间
        ,TO_DATE(CASE WHEN p1.pay_id=41 THEN p1.pay_time ELSE p1.result_pay_time END) AS order_pay_date      -- 支付日期
        ,p2.goods_id
        ,p2.sku_id
        ,p2.original_goods_number
        ,p2.goods_number
FROM zydb.dw_order_sub_order_fact AS p1
LEFT JOIN zydb.dw_order_goods_fact AS p2
       ON p1.order_id = p2.order_id
WHERE p1.pay_status IN (1, 3)
),
-- 筛选支付时间在黑五前一个月和黑五后一个月
t102 AS
(SELECT t101.*
FROM t101
WHERE (t101.order_pay_date >= '2017-09-12' AND t101.order_pay_date < '2017-10-12')     -- 黑五之前
   OR (t101.order_pay_date >= '2017-12-12' AND t101.order_pay_date < '2018-01-12')     -- 黑五之后
),

-- 2.订单商品锁定明细
-- 采购需求的原始商品需求数量
t2 AS
(SELECT p1.rec_id
        ,p1.order_id
        ,p1.source_rec_id
        ,p1.order_goods_rec_id
        ,p1.sku_id
        ,p1.org_num
        ,p1.status      -- 状态：1有效；2无效
        ,FROM_UNIXTIME(p1.last_modified_time) AS sku_lock_time      -- sku配货时间
FROM jolly_oms.who_wms_goods_need_lock_detail AS p1
WHERE p1.source_type = 1        -- source_type：需求类型； = 1表示采购需求，= 2表示调拨需求
),

-- 3.采购需求
-- 需求推送时间、标记缺货时间、缺货数量、发货数量
t3 AS
(SELECT p1.rec_id
        ,p2.pur_order_goods_rec_id          -- 同一个采购需求，也可能对应多个这个rec_id，这意味着多个采购单
        ,p1.goods_id
        ,p1.sku_id
        --,p1.depot_id
        ,p1.supp_name
        ,p3.cat_level1_name
        ,p1.oos_num
        ,p1.send_num
        ,FROM_UNIXTIME(p1.gmt_created) AS demand_create_time   -- 需求生成时间
        ,FROM_UNIXTIME(p1.check_time) AS demand_push_time     -- 需求审核时间，也就是推送时间
        ,FROM_UNIXTIME(p1.oos_time) AS demand_oos_time      -- 供应商标记缺货时间
FROM jolly_spm.jolly_spm_pur_goods_demand p1
LEFT JOIN jolly_spm.jolly_spm_pur_goods_demand_relation p2
       ON p1.rec_id = p2.demand_rec_id
LEFT JOIN zydb.dim_jc_goods p3
       ON p1.goods_id = p3.goods_id
WHERE p1.pur_type IN (1, 2, 5, 7)          -- 表示按需采购
  AND p1.review_status = 1       -- 审核状态：0 未审核；1 已审核；2不用审核
),

-- 4.入库各环节时间
t4 AS
(SELECT p1.delivered_order_id
        ,p1.delivered_order_sn
        ,p1.tracking_no
        ,p2.tracking_id AS shipping_id
        ,p3.value_name AS shipping_name
        ,MAX(p1.end_receipt_time) AS receipt_time
        ,MAX(p1.finish_check_time) AS finish_check_time
        ,MAX(p1.on_shelf_finish_time) AS finish_onshelf_time
FROM zydb.dw_delivered_receipt_onself AS p1
LEFT JOIN jolly_spm.jolly_spm_pur_order_tracking_info AS p2
       ON p1.tracking_no = p2.tracking_no
LEFT JOIN (SELECT *
           FROM jolly.who_pur_set_value
           WHERE type_id=16) AS p3
       ON p2.tracking_id = p3.value_id
GROUP BY p1.delivered_order_id
        ,p1.delivered_order_sn
        ,p1.tracking_no
        ,p2.tracking_id
        ,p3.value_name
),

-- 5. JOIN各表，获取各环节时间
t5 AS
(SELECT t102.order_id
        ,t102.order_sn
        ,t102.depot_id
        ,t102.order_status
        ,t102.is_shiped
        ,t102.order_pay_time
        ,t102.order_pay_date
        ,t102.goods_id
        ,t102.sku_id
        ,NVL(t102.original_goods_number, 0) AS original_goods_number
        ,NVL(t102.goods_number, 0) AS goods_number
        ,NVL(t2.org_num, 0) AS org_num
        ,t3.supp_name
        ,t3.cat_level1_name
        ,NVL(t3.oos_num, 0) AS oos_num
        ,NVL(t3.send_num, 0) AS send_num
        ,t3.demand_create_time
        ,t3.demand_push_time
        ,t3.demand_oos_time
        ,p5.pur_order_id
        ,p5.pur_order_sn
        ,FROM_UNIXTIME(p5.gmt_created) AS pur_send_time
        --,GET_JSON_OBJECT(p6.tracking_detail, '$.datas[3].col_008') AS pur_grab_time         -- 物流商至供应商处揽件时间
        ,t4.tracking_no
        ,t4.shipping_id
        ,t4.shipping_name
        --,SUBSTR(GET_JSON_OBJECT(p6.tracking_detail,'$.datas[:1].col_008'), -18, 16) AS wuliu_daohuo_time
        ,t4.receipt_time
        ,t4.finish_check_time
        ,t4.finish_onshelf_time
        ,t2.sku_lock_time
        ,p7.outing_stock_time       -- 配货完成，可拣货时间
FROM t102
LEFT JOIN t2
       ON t102.order_id = t2.order_id
      AND t102.sku_id = t2.sku_id
LEFT JOIN t3
       ON t2.source_rec_id = t3.rec_id
LEFT JOIN jolly_spm.jolly_spm_pur_order_goods AS p4       -- 采购单商品明细表
       ON t3.pur_order_goods_rec_id = p4.rec_id
LEFT JOIN jolly_spm.jolly_spm_pur_order_info AS p5         -- 采购单表，发货时间
       ON p4.pur_order_id = p5.pur_order_id
LEFT JOIN t4
       ON p5.pur_order_sn = t4.delivered_order_sn
--LEFT JOIN jolly_tms_center.tms_domestic_order_shipping_tracking_detail p6
       --ON t4.tracking_no = p6.tracking_no
LEFT JOIN zydb.dw_order_node_time p7
       ON t102.order_id = p7.order_id
),

-- 通过group by，去除多个采购单、多个物流单导致的多条记录，取较大的时间
t6 AS
(SELECT t5.order_id
        ,t5.order_sn
        ,t5.depot_id
        ,t5.order_status
        ,t5.is_shiped
        ,t5.order_pay_time
        ,t5.order_pay_date
        ,t5.goods_id
        ,t5.sku_id
        ,t5.original_goods_number
        ,t5.goods_number
        ,t5.org_num
        ,t5.supp_name
        ,t5.oos_num
        ,t5.send_num
        ,t5.demand_create_time
        ,t5.demand_push_time
        ,t5.demand_oos_time
        ,t5.shipping_id
        ,t5.shipping_name
        ,MAX(t5.pur_send_time) AS pur_send_time
        --,MAX(t5.pur_grab_time) AS pur_grab_time
        ,MAX(t5.receipt_time) AS receipt_time
        ,MAX(t5.finish_check_time) AS finish_check_time
        ,MAX(t5.finish_onshelf_time) AS finish_onshelf_time
        ,t5.sku_lock_time
        ,t5.outing_stock_time
FROM t5
GROUP BY t5.order_id
        ,t5.order_sn
        ,t5.depot_id
        ,t5.order_status
        ,t5.is_shiped
        ,t5.order_pay_time
        ,t5.order_pay_date
        ,t5.goods_id
        ,t5.sku_id
        ,t5.original_goods_number
        ,t5.goods_number
        ,t5.org_num
        ,t5.supp_name
        ,t5.oos_num
        ,t5.send_num
        ,t5.demand_create_time
        ,t5.demand_push_time
        ,t5.demand_oos_time
        ,t5.shipping_id
        ,t5.shipping_name
        ,t5.sku_lock_time
        ,t5.outing_stock_time
)

SELECT *
FROM t6
;





-- 1.2 hive 包含揽件时间 ========================================================
CREATE TABLE zybiro.neo_pur_lock_detail
AS
WITH
-- 1.支付订单（子单）
-- 子单中每个sku的原始商品数量和最终数量，支付时间
-- 注意：
-- (1).goods_number <> 0, oos_num = 0表示供应商标记缺货了，但是最终商品配货成功，标记缺货没有影响到最终配货，这种情况不会统计缺货率。
-- (2).goods_number = 0 or goods_number < original_goods_number, oos_num = 0表示供应商标记缺货了且影响到了商品配货，这种情况会统计缺货率
t1 AS
(SELECT p1.order_id
        ,p1.order_sn
        ,p1.depod_id AS depot_id
        ,(CASE WHEN p1.pay_id=41 THEN p1.pay_time ELSE p1.result_pay_time END) AS order_pay_time      -- 支付时间
        ,TO_DATE(CASE WHEN p1.pay_id=41 THEN p1.pay_time ELSE p1.result_pay_time END) AS order_pay_date      -- 支付日期
        ,p2.goods_id
        ,p2.sku_id
        ,p2.original_goods_number
        ,p2.goods_number
FROM zydb.dw_order_sub_order_fact AS p1
LEFT JOIN zydb.dw_order_goods_fact AS p2
       ON p1.order_id = p2.order_id
WHERE p1.order_status = 1
     --AND p1.order_id = 40683881
  AND p1.is_shiped = 1
  AND p1.is_problems_order = 2       -- 默认值为0,1是问题单,2非问题单
  AND p1.pay_status IN (1, 3)
  AND p1.add_time >= '2017-10-01'
  AND p1.add_time <= '2018-01-01'
),
-- 2.订单商品锁定明细
-- 采购需求的原始商品需求数量
t2 AS
(SELECT p1.rec_id
        ,p1.order_id
        ,p1.source_rec_id
        ,p1.order_goods_rec_id
        ,p1.sku_id
        ,p1.org_num
        ,p1.status      -- 状态：1有效；2无效
        ,FROM_UNIXTIME(p1.last_modified_time) AS sku_lock_time      -- sku配货时间
FROM jolly_oms.who_wms_goods_need_lock_detail AS p1
WHERE p1.source_type = 1        -- source_type：需求类型； = 1表示采购需求，= 2表示调拨需求
),

-- 3.采购需求
-- 需求推送时间、标记缺货时间、缺货数量、发货数量
t3 AS
(SELECT p1.rec_id
        ,p2.pur_order_goods_rec_id          -- 同一个采购需求，也可能对应多个这个rec_id，这意味着多个采购单
        ,p1.goods_id
        ,p1.sku_id
        --,p1.depot_id
        ,p1.supp_name
        ,p3.cat_level1_name
        ,p1.oos_num
        ,p1.send_num
        ,FROM_UNIXTIME(p1.gmt_created) AS demand_create_time   -- 需求生成时间
        ,FROM_UNIXTIME(p1.check_time) AS demand_push_time     -- 需求审核时间，也就是推送时间
        ,FROM_UNIXTIME(p1.oos_time) AS demand_oos_time      -- 供应商标记缺货时间
FROM jolly_spm.jolly_spm_pur_goods_demand p1
LEFT JOIN jolly_spm.jolly_spm_pur_goods_demand_relation p2
       ON p1.rec_id = p2.demand_rec_id
LEFT JOIN zydb.dim_jc_goods p3
       ON p1.goods_id = p3.goods_id
WHERE p1.pur_type IN (1, 2, 5, 7)          -- 表示按需采购
     AND p1.review_status = 1       -- 审核状态：0 未审核；1 已审核；2不用审核
),

-- 4.入库各环节时间
t4 AS
(SELECT p1.delivered_order_id
        ,p1.delivered_order_sn
        ,p1.tracking_no
        ,p2.tracking_id AS shipping_id
        ,p3.value_name AS shipping_name
        ,MAX(p1.end_receipt_time) AS receipt_time
        ,MAX(p1.finish_check_time) AS finish_check_time
        ,MAX(p1.on_shelf_finish_time) AS finish_onshelf_time
FROM zydb.dw_delivered_receipt_onself AS p1
LEFT JOIN jolly_spm.jolly_spm_pur_order_tracking_info AS p2
       ON p1.tracking_no = p2.tracking_no
LEFT JOIN (SELECT *
           FROM jolly.who_pur_set_value
           WHERE type_id=16) AS p3
       ON p2.tracking_id = p3.value_id
GROUP BY p1.delivered_order_id
        ,p1.delivered_order_sn
        ,p1.tracking_no
        ,p2.tracking_id
        ,p3.value_name
),

-- 5. JOIN各表，获取各环节时间
t5 AS
(SELECT t1.order_id
        ,t1.order_sn
        ,t1.depot_id
        ,t1.order_pay_time
        ,t1.order_pay_date
        ,t1.goods_id
        ,t1.sku_id
        ,t1.original_goods_number
        ,t1.goods_number
        ,t2.org_num
        ,t3.supp_name
        ,t3.cat_level1_name
        ,t3.oos_num
        ,t3.send_num
        ,t3.demand_create_time
        ,t3.demand_push_time
        ,t3.demand_oos_time
        ,p5.pur_order_id
        ,p5.pur_order_sn
        ,FROM_UNIXTIME(p5.gmt_created) AS pur_send_time
        ,GET_JSON_OBJECT(p6.tracking_detail, '$.datas[3].col_008') AS pur_grab_time         -- 物流商至供应商处揽件时间
        ,t4.tracking_no
        ,t4.shipping_id
        ,t4.shipping_name
        --,SUBSTR(GET_JSON_OBJECT(p6.tracking_detail,'$.datas[:1].col_008'), -18, 16) AS wuliu_daohuo_time
        ,t4.receipt_time
        ,t4.finish_check_time
        ,t4.finish_onshelf_time
        ,t2.sku_lock_time
        ,p7.outing_stock_time       -- 配货完成，可拣货时间
FROM t1
LEFT JOIN t2
       ON t1.order_id = t2.order_id
      AND t1.sku_id = t2.sku_id
LEFT JOIN t3
       ON t2.source_rec_id = t3.rec_id
LEFT JOIN jolly_spm.jolly_spm_pur_order_goods AS p4       -- 采购单商品明细表
       ON t3.pur_order_goods_rec_id = p4.rec_id
LEFT JOIN jolly_spm.jolly_spm_pur_order_info AS p5         -- 采购单表，发货时间
       ON p4.pur_order_id = p5.pur_order_id
LEFT JOIN t4
       ON p5.pur_order_sn = t4.delivered_order_sn
LEFT JOIN jolly_tms_center.tms_domestic_order_shipping_tracking_detail p6
       ON t4.tracking_no = p6.tracking_no
LEFT JOIN zydb.dw_order_node_time p7
       ON t1.order_id = p7.order_id
),

-- 通过group by，去除多个采购单、多个物流单导致的多条记录，取较大的时间
t6 AS
(SELECT t5.order_id
        ,t5.order_sn
        ,t5.depot_id
        ,t5.order_pay_time
        ,t5.order_pay_date
        ,t5.goods_id
        ,t5.sku_id
        ,t5.original_goods_number
        ,t5.goods_number
        ,t5.org_num
        ,t5.supp_name
        ,t5.oos_num
        ,t5.send_num
        ,t5.demand_create_time
        ,t5.demand_push_time
        ,t5.demand_oos_time
        ,t5.shipping_id
        ,t5.shipping_name
        ,MAX(t5.pur_send_time) AS pur_send_time
        ,MAX(t5.pur_grab_time) AS pur_grab_time
        ,MAX(t5.receipt_time) AS receipt_time
        ,MAX(t5.finish_check_time) AS finish_check_time
        ,MAX(t5.finish_onshelf_time) AS finish_onshelf_time
        ,t5.sku_lock_time
        ,t5.outing_stock_time
FROM t5
GROUP BY t5.order_id
        ,t5.order_sn
        ,t5.depot_id
        ,t5.order_pay_time
        ,t5.order_pay_date
        ,t5.goods_id
        ,t5.sku_id
        ,t5.original_goods_number
        ,t5.goods_number
        ,t5.org_num
        ,t5.supp_name
        ,t5.oos_num
        ,t5.send_num
        ,t5.demand_create_time
        ,t5.demand_push_time
        ,t5.demand_oos_time
        ,t5.shipping_id
        ,t5.shipping_name
        ,t5.sku_lock_time
        ,t5.outing_stock_time
)

-- 看几条记录
SELECT *
FROM t6
LIMIT 20;




/*
-- 订单
40683881
40574881
-- 发货单, jolly_spm.jolly_spm_pur_order_info
pur_order_id  pur_order_sn
3459918 GZ2FHD201712262050104014
 */

-- 2.统计分析 =======================================================================================
-- zybiro.neo_pur_lock_detail
SELECT *
FROM zybiro.neo_pur_lock_detail
WHERE order_id = 27077099
;

-- 2.0 这段时间的订单和商品汇总信息 ==============================================
-- 多少天？多少个订单？多少个商品？订单命中率？商品命中率？
-- 需采购商品数？推送商品数？推送比例？平均推送时长？推送时长分布(以小时计)？
-- 缺货商品数？缺货率？发货商品数？发货率？平均发货时长？发货时长分布？
-- 平均在途时长？在途时长分布？平均配货时长？配货时长分布？



-- 2.1 每天汇总 =================================================================
-- 订单数、商品件数
-- 命中商品件数、商品命中率（命中商品：无需采购，org_num IS NULL）
-- 需采购商品数、需采购商品占比（未命中商品：org_num >= 1）
-- 采购需求推送数量(需求数量>=1且有推送时间：org_num >= 1 AND demand_push_time IS NOT NULL)
WITH
t1 AS
(SELECT p1.order_pay_date
        ,COUNT(DISTINCT p1.order_id) AS order_num
        ,SUM(p1.original_goods_number) AS org_goods_num
        ,SUM(p1.goods_number) AS ship_goods_num
        ,SUM(p1.original_goods_number - p1.org_num) AS aim_goods_num
        ,SUM(p1.original_goods_number - p1.org_num) / SUM(p1.original_goods_number) AS aim_goods_rate
        ,SUM(p1.original_goods_number) - SUM(p1.original_goods_number - p1.org_num) AS pur_goods_num
        ,1 - SUM(p1.original_goods_number - p1.org_num) / SUM(p1.original_goods_number) AS pur_goods_rate
        ,SUM(CASE WHEN p1.demand_push_time IS NULL THEN 0 ELSE p1.org_num END) AS push_goods_num
        ,SUM(p1.oos_num) AS oos_goods_num
        ,SUM(p1.send_num) AS send_goods_num
FROM zybiro.neo_pur_lock_detail AS p1
WHERE p1.depot_id IN (4, 5, 14)
GROUP BY p1.order_pay_date
ORDER BY p1.order_pay_date
),

-- 下单数
-- 整单命中订单数、整单命中占比（整单所有商品都不需要采购，org_num = 0）
-- 完成配货订单数、占比
-- 发货订单数
-- 汇总到订单级别
CREATE TABLE zybiro.neo_pur_lock_orders
AS
SELECT (CASE WHEN p1.order_pay_date < '2017-10-13' THEN 'before' ELSE 'after' END) AS black_friday      --黑五前后
        ,p1.order_pay_date
        ,p1.order_id
        ,p1.order_sn
        ,p1.order_status
        ,p1.is_shiped
        ,p1.order_pay_time
        ,(CASE WHEN SUM(p1.org_num) = 0 THEN 'yes' ELSE 'no' END) AS is_aim_order
        ,SUM(p1.original_goods_number) AS org_goods_num
        ,SUM(p1.goods_number) AS ship_goods_num
        ,MAX(p1.demand_create_time) AS demand_create_time
        ,SUM(p1.org_num) AS demand_org_num
        ,MAX(p1.demand_push_time) AS demand_push_time
        ,MAX(p1.pur_send_time) AS demand_send_time
        ,MIN(p1.receipt_time) AS receipt_time
        ,p1.outing_stock_time
FROM zybiro.neo_pur_lock_detail AS p1
WHERE p1.depot_id IN (4, 5, 14)
GROUP BY (CASE WHEN p1.order_pay_date < '2017-10-13' THEN 'before' ELSE 'after' END)
        ,p1.order_pay_date
        ,p1.order_id
        ,p1.order_sn
        ,p1.order_status
        ,p1.is_shiped
        ,p1.order_pay_time
        ,p1.outing_stock_time
;

-- 汇总黑五前后
SELECT t2.black_friday
        ,COUNT(t2.order_id) AS pay_order_num
        ,SUM(t2.org_goods_num) AS pay_goods_num
        ,SUM(CASE WHEN t2.is_aim_order = 'yes' THEN 1 ELSE 0 END) AS aim_order_num
        ,SUM(t2.demand_org_num) AS demand_goods_num
        ,SUM(CASE WHEN t2.outing_stock_time IS NOT NULL AND t2.outing_stock_time > t2.order_pay_time
                  THEN 1
                  ELSE 0
             END) AS peihuo_order_num
        ,SUM(CASE WHEN t2.outing_stock_time IS NOT NULL AND t2.outing_stock_time > t2.order_pay_time
                  THEN (UNIX_TIMESTAMP(t2.outing_stock_time) - UNIX_TIMESTAMP(t2.order_pay_time)) / 3600
                  ELSE 0
             END) AS peihuo_duration
        ,SUM(CASE WHEN is_shiped = 1 THEN 1 ELSE 0 END) AS ship_order_num
        ,SUM(t2.ship_goods_num) AS ship_goods_num
FROM zybiro.neo_pur_lock_orders AS t2
WHERE t2.order_pay_date < '2018-01-09'
GROUP BY t2.black_friday
;


-- 汇总到每一天
SELECT t2.black_friday
        ,t2.order_pay_date
        ,COUNT(t2.order_id) AS pay_order_num
        ,SUM(t2.org_goods_num) AS pay_goods_num
        ,SUM(CASE WHEN t2.is_aim_order = 'yes' THEN 1 ELSE 0 END) AS aim_order_num
        ,SUM(CASE WHEN t2.outing_stock_time IS NOT NULL AND t2.outing_stock_time > t2.order_pay_time
                  THEN 1
                  ELSE 0
             END) AS peihuo_order_num
        ,SUM(CASE WHEN t2.outing_stock_time IS NOT NULL AND t2.outing_stock_time > t2.order_pay_time
                  THEN (UNIX_TIMESTAMP(t2.outing_stock_time) - UNIX_TIMESTAMP(t2.order_pay_time)) / 3600
                  ELSE 0
             END) AS peihuo_duration
        ,SUM(CASE WHEN is_shiped = 1 THEN 1 ELSE 0 END) AS ship_order_num
FROM zybiro.neo_pur_lock_orders AS t2
GROUP BY t2.black_friday
        ,t2.order_pay_date
;
/*
黑五前后 非命中订单   命中订单
before  51.33440842 14.70760713
after   58.1125181  9.70300022
*/

-- 每天完成配货的订单数
SELECT t2.black_friday
        ,t2.order_pay_date
        ,COUNT(t2.order_id) AS pay_order_num
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 0 THEN 1 ELSE 0 END) AS t0_peihuo_order_num
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 0
                  THEN (UNIX_TIMESTAMP(t2.outing_stock_time) - UNIX_TIMESTAMP(t2.order_pay_time)) / 3600
                  ELSE 0
             END) AS t0_peihuo_duration
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 1 THEN 1 ELSE 0 END) AS t1_peihuo_order_num
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 1
                  THEN (UNIX_TIMESTAMP(t2.outing_stock_time) - UNIX_TIMESTAMP(t2.order_pay_time)) / 3600
                  ELSE 0
             END) AS t1_peihuo_duration
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 2 THEN 1 ELSE 0 END) AS t2_peihuo_order_num
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 2
                  THEN (UNIX_TIMESTAMP(t2.outing_stock_time) - UNIX_TIMESTAMP(t2.order_pay_time)) / 3600
                  ELSE 0
             END) AS t2_peihuo_duration
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 3 THEN 1 ELSE 0 END) AS t3_peihuo_order_num
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 3
                  THEN (UNIX_TIMESTAMP(t2.outing_stock_time) - UNIX_TIMESTAMP(t2.order_pay_time)) / 3600
                  ELSE 0
             END) AS t3_peihuo_duration
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 4 THEN 1 ELSE 0 END) AS t4_peihuo_order_num
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 4
                  THEN (UNIX_TIMESTAMP(t2.outing_stock_time) - UNIX_TIMESTAMP(t2.order_pay_time)) / 3600
                  ELSE 0
             END) AS t4_peihuo_duration
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 5 THEN 1 ELSE 0 END) AS t5_peihuo_order_num
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 5
                  THEN (UNIX_TIMESTAMP(t2.outing_stock_time) - UNIX_TIMESTAMP(t2.order_pay_time)) / 3600
                  ELSE 0
             END) AS t5_peihuo_duration
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 6 THEN 1 ELSE 0 END) AS t6_peihuo_order_num
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 6
                  THEN (UNIX_TIMESTAMP(t2.outing_stock_time) - UNIX_TIMESTAMP(t2.order_pay_time)) / 3600
                  ELSE 0
             END) AS t6_peihuo_duration
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) >= 7 THEN 1 ELSE 0 END) AS t7_peihuo_order_num
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) >= 7
                  THEN (UNIX_TIMESTAMP(t2.outing_stock_time) - UNIX_TIMESTAMP(t2.order_pay_time)) / 3600
                  ELSE 0
             END) AS t7_peihuo_duration
FROM zybiro.neo_pur_lock_orders AS t2
GROUP BY t2.black_friday
        ,t2.order_pay_date
ORDER BY t2.order_pay_date
;




-- 汇总各个节点的时效
SELECT p1.order_pay_date
        -- create_2_push_duration
        ,SUM(p1.org_num) AS demand_goods_num
        ,SUM(CASE WHEN p1.demand_push_time IS NULL OR p1.demand_push_time < p1.demand_create_time
                  THEN 0
                  ELSE p1.org_num
             END) AS push_goods_num
        ,SUM(CASE WHEN p1.demand_push_time IS NULL OR p1.demand_push_time < p1.demand_create_time
                  THEN 0
                  ELSE (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(P1.demand_create_time)) * p1.org_num /3600
             END) AS push_goods_duration
        -- push_2_send_duration
        ,SUM(p1.org_num - p1.oos_num) AS need_send_num
        ,SUM(p1.oos_num) AS oos_num
        ,SUM(CASE WHEN p1.pur_send_time IS NULL OR p1.pur_send_time < p1.demand_push_time
                  THEN 0
                  ELSE p1.send_num
             END) AS real_send_num
        ,SUM(CASE WHEN p1.pur_send_time IS NULL OR p1.pur_send_time < p1.demand_push_time
                  THEN 0
                  ELSE (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time)) * p1.send_num / 3600
             END) AS send_goods_duration
        -- send_2_receipt_duration
        ,SUM(CASE WHEN p1.receipt_time IS NULL OR p1.receipt_time < p1.pur_send_time
                  THEN 0
                  ELSE p1.send_num
             END) AS receipt_num
        ,SUM(CASE WHEN p1.receipt_time IS NULL OR p1.receipt_time < p1.pur_send_time
                  THEN 0
                  ELSE (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time)) * p1.send_num / 3600
             END) AS receipt_goods_duration
FROM zybiro.neo_pur_lock_detail AS p1
WHERE p1.depot_id IN (4, 5, 14)
GROUP BY p1.order_pay_date
ORDER BY p1.order_pay_date
;


-- 采购需求各节点时效和量
SELECT p1.order_pay_date
        -- create_2_push:小时：0/2/4/6/8/10+
        ,SUM(p1.org_num) AS demand_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 > 0
                   AND (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 < 2
                  THEN p1.org_num
                  ELSE 0
             END) AS t0_push_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 > 0
                   AND (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 < 2
                  THEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600
                  ELSE 0
             END) AS t0_push_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 >=2
                   AND (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 < 4
                  THEN p1.org_num
                  ELSE 0
             END) AS t2_push_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 >=2
                   AND (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 < 4
                  THEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600
                  ELSE 0
             END) AS t2_push_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 >=4
                   AND (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 < 6
                  THEN p1.org_num
                  ELSE 0
             END) AS t4_push_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 >=4
                   AND (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 < 6
                  THEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600
                  ELSE 0
             END) AS t4_push_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 >=6
                   AND (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 < 8
                  THEN p1.org_num
                  ELSE 0
             END) AS t6_push_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 >=6
                   AND (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 < 8
                  THEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600
                  ELSE 0
             END) AS t6_push_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 >=8
                   AND (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 < 10
                  THEN p1.org_num
                  ELSE 0
             END) AS t8_push_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 >=8
                   AND (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 < 10
                  THEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600
                  ELSE 0
             END) AS t8_push_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 >=10
                  THEN p1.org_num
                  ELSE 0
             END) AS t10_push_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 >=10
                  THEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600
                  ELSE 0
             END) AS t10_push_goods_duration
        -- push_2_send：小时：0/12/24/36/48/60/72+
        ,SUM(p1.org_num - p1.oos_num) AS need_send_num
        ,SUM(p1.oos_num) AS oos_num
        ,SUM(CASE WHEN p1.pur_send_time IS NULL OR p1.pur_send_time < p1.demand_push_time
                  THEN 0
                  ELSE p1.send_num
             END) AS real_send_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 > 0
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 12
                  THEN p1.send_num
                  ELSE 0
             END) AS t0_send_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 > 0
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 12
                  THEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600
                  ELSE 0
             END) AS t0_send_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=12
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 24
                  THEN p1.send_num
                  ELSE 0
             END) AS t12_send_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=12
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 24
                  THEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600
                  ELSE 0
             END) AS t12_send_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=24
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 36
                  THEN p1.send_num
                  ELSE 0
             END) AS t24_send_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=24
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 36
                  THEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600
                  ELSE 0
             END) AS t24_send_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=36
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 48
                  THEN p1.send_num
                  ELSE 0
             END) AS t36_send_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=36
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 48
                  THEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600
                  ELSE 0
             END) AS t36_send_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=48
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 60
                  THEN p1.send_num
                  ELSE 0
             END) AS t48_send_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=48
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 60
                  THEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600
                  ELSE 0
             END) AS t48_send_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=60
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 72
                  THEN p1.send_num
                  ELSE 0
             END) AS t60_send_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=60
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 72
                  THEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600
                  ELSE 0
             END) AS t60_send_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=72
                  THEN p1.send_num
                  ELSE 0
             END) AS t72_send_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=72
                  THEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600
                  ELSE 0
             END) AS t72_send_goods_duration
        -- send_2_receipt：小时：0/12/24/36/48/60/72+
        ,SUM(CASE WHEN p1.receipt_time IS NULL OR p1.receipt_time < p1.pur_send_time
                  THEN 0
                  ELSE p1.send_num
             END) AS receipt_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 > 0
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 12
                  THEN p1.send_num
                  ELSE 0
             END) AS t0_receipt_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 > 0
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 12
                  THEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600
                  ELSE 0
             END) AS t0_receipt_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=12
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 24
                  THEN p1.send_num
                  ELSE 0
             END) AS t12_receipt_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=12
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 24
                  THEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600
                  ELSE 0
             END) AS t12_receipt_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=24
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 36
                  THEN p1.send_num
                  ELSE 0
             END) AS t24_receipt_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=24
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 36
                  THEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600
                  ELSE 0
             END) AS t24_receipt_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=36
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 48
                  THEN p1.send_num
                  ELSE 0
             END) AS t36_receipt_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=36
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 48
                  THEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600
                  ELSE 0
             END) AS t36_receipt_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=48
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 60
                  THEN p1.send_num
                  ELSE 0
             END) AS t48_receipt_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=48
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 60
                  THEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600
                  ELSE 0
             END) AS t48_receipt_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=60
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 72
                  THEN p1.send_num
                  ELSE 0
             END) AS t60_receipt_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=60
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 72
                  THEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600
                  ELSE 0
             END) AS t60_receipt_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=72
                  THEN p1.send_num
                  ELSE 0
             END) AS t72_receipt_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=72
                  THEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600
                  ELSE 0
             END) AS t72_receipt_goods_duration
FROM zybiro.neo_pur_lock_detail AS p1
WHERE p1.depot_id IN (4, 5, 14)
GROUP BY p1.order_pay_date
ORDER BY p1.order_pay_date
;



-- 2.2 推送时长(小时)
-- 黑五期间修改了推送时间间隔
-- 修改前：推送时长一般在20分钟左右；
-- 修改后：平均推送时长为5.5小时；
-- 那么有疑问：修改前后的订单配货时长是否有较大差异？

SELECT p1.order_pay_date
        ,SUM(p1.org_num) AS demand_goods_num
        ,SUM(CASE WHEN p1.demand_push_time IS NULL OR p1.demand_push_time < p1.demand_create_time
                  THEN 0
                  ELSE p1.org_num
             END) AS push_goods_num
        ,SUM(CASE WHEN p1.demand_push_time IS NULL OR p1.demand_push_time < p1.demand_create_time
                  THEN 0
                  ELSE (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(P1.demand_create_time)) * p1.org_num /3600
             END) AS push_goods_duration
FROM zybiro.neo_pur_lock_detail AS p1
WHERE p1.depot_id IN (4, 5, 14)
GROUP BY p1.order_pay_date
ORDER BY p1.order_pay_date
;

-- 继续看一下推送时长的分布
-- 数据量太大，300多万，抽样5万条看一下
SELECT p1.order_pay_date
        ,p1.order_id
        ,p1.demand_create_time
        ,p1.demand_push_time
        ,(UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(P1.demand_create_time)) / 60 AS push_duration
FROM zybiro.neo_pur_lock_detail AS p1
WHERE p1.depot_id IN (4, 5, 14)
  AND p1.order_pay_date < '2018-01-10'
  AND p1.demand_push_time IS NOT NULL
  AND p1.demand_push_time >= p1.demand_create_time
GROUP BY p1.order_pay_date
        ,p1.order_id
        ,p1.demand_create_time
        ,p1.demand_push_time
ORDER BY RAND()         -- 结合order by rand()和limit n，可以实现抽样
LIMIT 50000
;

-- 2.3 采购需求发货时长分布
-- 没有发货时间：未发货
-- 有发货时间：统计send_num
-- 应发数量：org_num - oos_num

---- 2.3.1 发货商品数量和平均时长
SELECT p1.order_pay_date
        ,SUM(p1.org_num - p1.oos_num) AS need_send_num
        ,SUM(CASE WHEN p1.pur_send_time IS NULL OR p1.pur_send_time < p1.demand_push_time
                  THEN 0
                  ELSE p1.send_num
             END) AS real_send_num
        ,SUM(CASE WHEN p1.pur_send_time IS NULL OR p1.pur_send_time < p1.demand_push_time
                  THEN 0
                  ELSE (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time)) * p1.send_num / 3600
             END) AS send_goods_duration
FROM zybiro.neo_pur_lock_detail AS p1
WHERE p1.depot_id IN (4, 5, 14)
GROUP BY p1.order_pay_date
ORDER BY p1.order_pay_date
;

---- 2.3.2 发货时长分布


-- 2.4 采购需求到货时长
-- 无到货签收时间：未到货
-- 有到货签收时间：默认到货签收数量 = send_num，忽略质检上架过程中发现的异常

---- 2.4.1 到货签收商品数量和平均时长
SELECT p1.order_pay_date
        ,SUM(CASE WHEN p1.receipt_time IS NULL OR p1.receipt_time < p1.pur_send_time
                  THEN 0
                  ELSE p1.send_num
             END) AS receipt_num
        ,SUM(CASE WHEN p1.receipt_time IS NULL OR p1.receipt_time < p1.pur_send_time
                  THEN 0
                  ELSE (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time)) * p1.send_num / 3600
             END) AS receipt_goods_duration
FROM zybiro.neo_pur_lock_detail AS p1
WHERE p1.depot_id IN (4, 5, 14)
GROUP BY p1.order_pay_date
ORDER BY p1.order_pay_date
;


