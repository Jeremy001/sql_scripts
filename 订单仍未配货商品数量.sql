/*
主题：订单的配货完成情况，总商品x1件，配货完成x2件
作者：Cherish & Neo
时间：20171204
 */


WITH 
-- 订单
t1 AS 
(SELECT a.order_id
        ,a.order_sn
        ,a.depot_id
        ,a.is_shiped
        ,a.order_status
        ,a.goods_num
        ,a.pay_status
        ,ROUND(CAST(a.pay_money AS Float) + CAST(a.surplus AS Float) + CAST(a.order_amount AS Float) ,2) AS order_total_amount
        ,FROM_UNIXTIME(IF(a.prepare_pay_time = 0, a.pay_time, a.prepare_pay_time)) AS pay_time
        ,FROM_UNIXTIME(a.no_problems_order_uptime) AS no_problems_order_uptime
        ,(CASE WHEN a.pay_id =41 THEN 'cod' ELSE 'not_cod' END) AS is_cod
        ,(CASE WHEN p3.source_order_id IS NOT NULL THEN 1 ELSE 0 END) AS is_split
FROM default.who_order_info a 
LEFT JOIN default.who_order_user_info p3 
             ON p3.source_order_id= a.order_id
WHERE 1=1
AND a.pay_status in (1, 3)
AND IF(prepare_pay_time = 0,a.pay_time,prepare_pay_time) >= UNIX_TIMESTAMP(DATE_SUB(NOW(), 20)) 
),
-- 配货，订单未配齐数量
t2 AS
(SELECT depot_id
        ,order_id
        ,SUM(num) AS still_need_num
FROM jolly_oms.who_wms_goods_need_lock_detail
GROUP BY depot_id
        ,order_id
),
-- 结果明细表
t0 AS
(SELECT t1.*
        ,t2.still_need_num
FROM t1
LEFT JOIN t2
             ON t1.order_id = t2.order_id
WHERE t1.is_split = 0
     AND t1.depot_id = 6
     AND t2.still_need_num >= 1
)

-- 根据订单内商品数分组，查询缺一件的订单数
SELECT goods_num
        ,COUNT(order_id) AS order_num
        ,SUM(CASE WHEN still_need_num = 1 THEN 1 ELSE 0 END) AS need_1_order_num
FROM t0
GROUP BY goods_num
ORDER BY goods_num
;

-- 每天订单总数及缺X件商品的订单数
SELECT TO_DATE(pay_time) AS data_date
        ,still_need_num
        ,COUNT(order_id) AS order_num
FROM t0
GROUP BY TO_DATE(pay_time)
        ,still_need_num
ORDER BY TO_DATE(pay_time)
        ,still_need_num
;


-- 24号及之前的订单明细
SELECT * 
FROM t0
WHERE pay_time <= '2017-11-24'
;














