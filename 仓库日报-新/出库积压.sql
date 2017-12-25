/*
-- 内容：发货积压
-- 作者：Neo王政鸣
-- 类型：hive & impala
-- 时间：20171122
 */

-- hive hue 带日期参数 ========================================
WITH 
t1 aS
(SELECT p1.depot_id
        ,p1.depot_name
        ,p1.order_sn
        ,p1.goods_number
        ,p1.pay_time
        ,p1.no_problems_order_uptime AS no_problems_time
        ,p1.outing_stock_time AS into_wms_time
        ,p1.picking_time AS picking_begin_tiem
        ,p1.picking_finish_time
        ,p1.order_pack_time AS packing_finish_time
        ,p1.shipping_time
FROM zydb.dw_order_node_time p1
WHERE  p1.outing_stock_time>=FROM_UNIXTIME(UNIX_TIMESTAMP('${data_date}','yyyyMMdd'))
AND p1.outing_stock_time<FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(to_date(FROM_UNIXTIME(UNIX_TIMESTAMP('${data_date}','yyyyMMdd'))),' 18:00:00'),'yyyy-MM-dd HH:mm:ss'))
AND p1.is_problems_order != 1
AND p1.order_status=1
AND (p1.shipping_time>=DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP('${data_date}','yyyyMMdd')),1) or p1.shipping_time IS NULL)
)
-- 查询明细
SELECT * FROM t1;

-- 查询各仓出库积压订单数和商品总数
SELECT depot_id
        ,COUNT(order_sn) AS order_num
        ,SUM(goods_number) AS goods_num
FROM t1
GROUP BY depot_id
ORDER BY depot_id
;


-- impala 客户端 带日期参数 ===================================
WITH 
t1 aS
(SELECT p1.depot_id
        ,p1.depot_name
        ,p1.order_sn
        ,p1.goods_number
        ,p1.pay_time
        ,p1.no_problems_order_uptime AS no_problems_time
        ,p1.outing_stock_time AS into_wms_time
        ,p1.picking_time AS picking_begin_tiem
        ,p1.picking_finish_time
        ,p1.order_pack_time AS packing_finish_time
        ,p1.shipping_time
FROM zydb.dw_order_node_time p1
WHERE  p1.outing_stock_time>=FROM_UNIXTIME(UNIX_TIMESTAMP('$[&data_date]','yyyyMMdd'))
AND p1.outing_stock_time<FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(to_date(FROM_UNIXTIME(UNIX_TIMESTAMP('$[&data_date]','yyyyMMdd'))),' 18:00:00'),'yyyy-MM-dd HH:mm:ss'))
AND p1.is_problems_order != 1
AND p1.order_status=1
AND (p1.shipping_time>=DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP('$[&data_date]','yyyyMMdd')),1) or p1.shipping_time IS NULL)
)
-- 查询明细
SELECT * FROM t1;

-- 查询各仓出库积压订单数和商品总数
SELECT depot_id
        ,COUNT(order_sn) AS order_num
        ,SUM(goods_number) AS goods_num
FROM t1
GROUP BY depot_id
ORDER BY depot_id
;



-- hive 推送脚本 ========================================
WITH 
t1 AS
(SELECT p1.depot_id
        ,p1.depot_name
        ,p1.order_sn
        ,p1.goods_number
        ,p1.pay_time
        ,p1.no_problems_order_uptime AS no_problems_time
        ,p1.outing_stock_time AS into_wms_time
        ,p1.picking_time AS picking_begin_tiem
        ,p1.picking_finish_time
        ,p1.order_pack_time AS packing_finish_time
        ,p1.shipping_time
FROM zydb.dw_order_node_time p1
WHERE  p1.outing_stock_time >= FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE(), 1), 'yyyy-MM-dd'))
AND p1.outing_stock_time<FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(DATE_SUB(CURRENT_DATE(), 1),' 18:00:00'),'yyyy-MM-dd HH:mm:ss'))
AND p1.is_problems_order != 1
AND p1.order_status=1
AND (p1.shipping_time >= FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_DATE(), 'yyyy-MM-dd'))
           OR p1.shipping_time IS NULL)
)
-- 查询明细
SELECT * FROM t1;

-- 查询各仓出库积压订单数和商品总数
SELECT depot_id
        ,COUNT(order_sn) AS order_num
        ,SUM(goods_number) AS goods_num
FROM t1
GROUP BY depot_id
ORDER BY depot_id
;


