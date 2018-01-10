/*
-- 内容：采购需求各环节时间，数据用于分析发货到货对系统配货的影响，订单采购需求 --> 推送 --> 缺货 --> 发货 --> 到货 --> 系统配货
-- 作者：Neo王政鸣
-- 时间：2018-1-4
 */

-- impala，没有跨越的揽件时间 ==================================================================
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
  AND p1.add_time <= '2017-10-02'
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
--LEFT JOIN jolly_tms_center.tms_domestic_order_shipping_tracking_detail p6
       --ON t4.tracking_no = p6.tracking_no
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





-- hive ========================================================================
CREATE TABLE zybiro.neo_pur_demand_peihuo_detail
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

