/*
内容：超10天未发货
时间：20171130
作者：Cherish & Neo
 */


-- 订单级别，未发货订单和相关信息 ===============================================================
-- 对于未配货的sku，展示未配齐sku、数量、供应商名称
WITH 
-- who_order_info 和 who_order_user_info， 取子单的信息
t01 AS
(SELECT p1.order_id
        ,p1.order_sn
        ,p1.is_shiped
        ,p1.depot_id
        ,p1.site_id
        ,p1.order_status
        ,p1.goods_num
        ,(p1.pay_money+p1.surplus+p1.order_amount) AS order_amount
        ,p1.pay_status
        ,p1.is_problems_order
        --,p2.country_name
        ,FROM_UNIXTIME(IF(p1.prepare_pay_time = 0, p1.pay_time, p1.prepare_pay_time)) AS pay_time
        ,FROM_UNIXTIME(p1.no_problems_order_uptime) AS no_problems_order_uptime
        ,FROM_UNIXTIME(p1.shipping_time) AS shipping_time
        ,(CASE WHEN p1.pay_id = 41 THEN 'cod' ELSE 'not_cod' END) AS is_cod
        ,(CASE WHEN p2.source_order_id IS NOT NULL THEN 1 ELSE 0 END) AS is_split
FROM default.who_order_info AS p1
LEFT JOIN default.who_order_user_info AS p2
             ON p1.order_id = p2.source_order_id
WHERE p1.depot_id IN (4, 5, 6, 7, 8, 14, 15)
GROUP BY p1.order_id
        ,p1.order_sn
        ,p1.is_shiped
        ,p1.depot_id
        ,p1.site_id
        ,p1.order_status
        ,p1.goods_num
        ,(p1.pay_money+p1.surplus+p1.order_amount)
        ,p1.pay_status
        ,p1.is_problems_order
        --,p2.country_name
        ,FROM_UNIXTIME(IF(p1.prepare_pay_time = 0, p1.pay_time, p1.prepare_pay_time))
        ,FROM_UNIXTIME(p1.no_problems_order_uptime)
        ,FROM_UNIXTIME(p1.shipping_time)
        ,(CASE WHEN p1.pay_id = 41 THEN 'cod' ELSE 'not_cod' END)
        ,(CASE WHEN p2.source_order_id IS NOT NULL THEN 1 ELSE 0 END)
),
-- 订单未配齐总数量
t02 AS
(SELECT order_id
        ,SUM(num) AS total_still_need_num
-- FROM jolly_oms.who_wms_goods_need_lock_detail
FROM default.who_wms_goods_need_lock_detail
GROUP BY order_id
),
-- 未配齐sku和数量
t03 AS
(SELECT p1.order_id
        ,p1.sku_id
        ,p1.num AS sku_still_need_num
FROM default.who_wms_goods_need_lock_detail AS p1
WHERE p1.num >= 1
),
-- JOIN得到供应商名称
t04 AS
(SELECT TO_DATE(t01.pay_time) AS pay_date
        ,(CASE WHEN t01.site_id = 0 THEN 'Jollychic'
                      WHEN t01.site_id = 1 THEN 'NIMINI'
                      WHEN t01.site_id = 2 THEN 'MarkaVIP'
                      ELSE 'Others' END) AS site_id
        ,t01.order_id
        ,t01.order_sn
        ,t01.depot_id
        ,t01.is_cod
        ,(CASE WHEN t01.is_problems_order = 1 THEN '是'
                      WHEN t01.is_problems_order = 2 THEN '否'
                      ELSE '其他' END) AS is_problems_order
        ,(CASE WHEN t01.is_shiped = 0 THEN '未配货' 
                      WHEN t01.is_shiped = 1 THEN '已发货'
                      WHEN t01.is_shiped = 2 THEN '部分发货'
                      WHEN t01.is_shiped = 3 THEN '待发货'
                      WHEN t01.is_shiped = 4 THEN '部分匹配'
                      WHEN t01.is_shiped = 5 THEN '完全匹配'
                      WHEN t01.is_shiped = 6 THEN '拣货完成' 
                      WHEN t01.is_shiped = 7 THEN '待拣货'
                      WHEN t01.is_shiped = 8 THEN '拣货中'
                      ELSE NULL END) AS is_shiped
        ,t01.goods_num
        ,t01.order_amount
        --,t01.country_name
        ,t01.pay_time
        ,t01.shipping_time
        ,t02.total_still_need_num
        ,p5.sku_id
        ,p5.goods_number
        ,t03.sku_still_need_num
        ,p4.supp_name
FROM t01
LEFT JOIN t02
             ON t01.order_id = t02.order_id
LEFT JOIN jolly.who_order_goods p5
             ON t01.order_id = p5.order_id 
LEFT JOIN t03
             ON p5.order_id = t03.order_id AND p5.sku_id = t03.sku_id
LEFT JOIN jolly.who_sku_relation p3
             ON t03.sku_id = p3.rec_id
LEFT JOIN zydb.dim_jc_goods p4
             ON p3.goods_id = p4.goods_id
WHERE t01.is_split = 0
     AND t01.order_status = 1
     AND t01.is_shiped <> 1
     AND t01.pay_time > DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),50)
     AND t01.pay_time <= DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),4)
)


-- 最终结果
SELECT *
FROM t04
WHERE site_id = 'MarkaVIP'
ORDER BY pay_time
;


-- 各仓未发货订单数
SELECT depot_id
        ,COUNT(DISTINCT order_id) AS order_num
        ,SUM(goods_num) AS goods_num
FROM t04
GROUP BY depot_id
ORDER BY depot_id
;

