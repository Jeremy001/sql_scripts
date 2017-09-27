
-- 对比每日发货订单数和出库商品件数

WITH 
-- 每天发出订单数
t1 AS
(SELECT TRUNC(shipping_time, 'DD') AS data_date
        ,depot_id
        ,COUNT(DISTINCT order_id) AS total_delivery_num
        ,SUM(goods_number) AS goods_num
FROM zydb.dw_order_node_time
WHERE pay_status = 1
     AND depot_id = 7
     AND shipping_time >= '2017-06-01'
     AND is_shiped = 1
     AND order_status = 1
GROUP BY TRUNC(shipping_time, 'DD')
        ,depot_id
),
-- 每天出库商品件数
t2 AS
(SELECT FROM_UNIXTIME(change_time, 'yyyy-MM-dd') AS data_date
        ,7 AS depot_id
        ,SUM(change_num) AS sa_delivout_goods_num
 FROM jolly_wms.who_wms_goods_stock_detail_log
 WHERE change_type IN (5,6,13,17)
      AND change_time >= UNIX_TIMESTAMP('2017-06-01')
 GROUP BY FROM_UNIXTIME(change_time, 'yyyy-MM-dd')
        ,depot_id
)
-- JOIN得到结果
SELECT t1.*
        , t2.sa_delivout_goods_num
FROM t1
LEFT JOIN t2 
             ON t1.data_date = t2.data_date AND t1.depot_id = t2.depot_id
ORDER BY t1.data_date;




-- ========================================================================
-- 国内仓出库商品数 VS 订单商品数
WITH 
-- 每天发出订单数
t1 AS
(SELECT TRUNC(shipping_time, 'DD') AS data_date
        ,depot_id
        ,COUNT(DISTINCT order_id) AS total_delivery_num
        ,SUM(goods_number) AS goods_num
FROM zydb.dw_order_node_time
WHERE pay_status = 1
     AND depot_id IN (4, 5, 6)
     AND shipping_time >= '2017-06-01'
     AND is_shiped = 1
     AND order_status = 1
GROUP BY TRUNC(shipping_time, 'DD')
        ,depot_id
),
-- 每天出库商品件数
t2 AS
(SELECT FROM_UNIXTIME(change_time, 'yyyy-MM-dd') AS data_date
        ,depot_id
        ,SUM(change_num) AS delivout_goods_num
 FROM jolly.who_wms_goods_stock_detail_log
 WHERE change_type IN (5,6,13,17)
      AND change_time >= UNIX_TIMESTAMP('2017-06-01')
      and depot_id IN (4, 5, 6)
 GROUP BY FROM_UNIXTIME(change_time, 'yyyy-MM-dd')
        ,depot_id
)
-- JOIN得到结果
SELECT t1.*
        , t2.delivout_goods_num
FROM t1
LEFT JOIN t2 
             ON t1.data_date = t2.data_date AND t1.depot_id = t2.depot_id
ORDER BY t1.data_date;