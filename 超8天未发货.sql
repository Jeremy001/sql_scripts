/*
内容：超10天未发货
时间：20171130
作者：Cherish & Neo
 */

WITH t1 AS
(SELECT to_date(a.pay_time) AS pay_date
        ,a.site_id
        ,a.order_id
        ,a.order_sn
        ,a.order_status
        ,a.depot_id
        ,a.depot_name
        ,a.is_cod
        ,(CASE WHEN a.is_problems_order = 1 THEN '是'
                      WHEN a.is_problems_order = 2 THEN '否'
                      ELSE '其他' END) AS 是否问题单
        ,(CASE WHEN a.is_shiped = 0 THEN '未发货' 
                      WHEN a.is_shiped = 1 THEN '已发货'
                      WHEN a.is_shiped = 2 THEN '部分发货'
                      WHEN a.is_shiped = 3 THEN '待发货'
                      WHEN a.is_shiped = 4 THEN '部分匹配'
                      WHEN a.is_shiped = 5 THEN '完全匹配'
                      WHEN a.is_shiped = 6 THEN '拣货完成' 
                      WHEN a.is_shiped = 7 THEN '待拣货'
                      WHEN a.is_shiped = 8 THEN '拣货中'
                      ELSE NULL END) AS is_shiped
        ,a.original_goods_number
        ,a.goods_number
        ,a.country_name
        ,a.pay_time
        --,a.order_check_time
        --,a.lock_check_time
        ,a.lock_last_modified_time AS 配货完成时间
        ,a.no_problems_order_uptime AS 订单标非时间
        ,a.outing_stock_time AS 可拣货时间
        ,a.picking_finish_time AS 拣货完成时间
        ,a.order_pack_time AS 打包完成时间
        ,a.shipping_time AS 发货时间
FROM  zydb.dw_order_node_time a
WHERE 1=1
     AND a.is_shiped <> 1
     AND a.order_status = 1
     AND a.pay_time > date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),30)
     AND a.pay_time <= date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),7)
     AND a.depot_id IN (4,5,6,7,14)
),
-- 配货，订单未配齐数量
t2 AS
(SELECT depot_id
        ,order_id
        ,SUM(num) AS still_need_num
FROM jolly_oms.who_wms_goods_need_lock_detail
GROUP BY depot_id
        ,order_id
)

-- 最终结果
SELECT t1.*
        ,t2.still_need_num
FROM t1
LEFT JOIN t2 
             ON t1.order_id = t2.order_id
;

