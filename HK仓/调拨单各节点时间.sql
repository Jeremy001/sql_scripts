/*
HK仓调拨
1.生成调拨需求时间
2.生成调拨单时间， 2-1 = 调拨需求响应时长
3.调拨单发出时间， 3-2 = 调出仓调拨作业时长
4.调拨单签收时间， 4-3 = 调拨单在途时长
5.调拨单上架开始时间， 无法取到签收时间，则5-3 = 调拨单在途时长
6.调拨单上架结束时间， 6-5 = 上架时长，
 */


/*
order_id  订单id
order_sn  订单号
goods_number  订单最后同步时候的商品数量
original_goods_number 订单原始商品销量
depot_id  仓库
is_shiped 订单数据最后同步时候状态:顺序应该是   0（完全没开始配货）---4（部分匹配）----5（完全匹配）-----7（待拣货）---8（拣货中）---6（拣货完成）---3（待发货）--2（部分发货）---1（已发货）
pay_time  订单付款时间
order_check_time  订单系统审核分配仓库时间
lock_check_time 订单配货开始、锁定库存、开始采购、调拨
allocate_demand_start 订单调拨需求开始时间（如有，订单中最大一个调拨开始时间）
allocate_order_start  订单需求所在调拨单开始生成时间（如有，订单中最大一个调拨单生成时间）
allocate_order_out  订单需求所在调拨单开始出库时间（如有，订单中最大一个调拨单出库时间）
allocate_order_start_onself 订单需求所在调拨单开始入库时间（如有，订单中最大一个调拨单入库时间）
allocate_order_finish_onself  订单需求所在调拨单入库完成时间（如有，订单中最大一个调拨单入库完成时间）
lock_last_modified_time 订单配货完成时间
no_problems_order_uptime  订单客服审单放行时间
outing_stock_time 订单定时任务触发时间=订单流入WMS的可拣货开始时间
picking_time  订单的拣货单生成时间（对应最后一个拣货单生成时间）
order_pack_time 订单的打包时间
shipping_time 订单的发运时间
oos_num 订单中对应的缺货商品数
type  订单缺货商品对应的缺货类型，此处如果是多个商品缺货可能会重复出现订单条数
create_time 订单登记缺货的时间
*/



WITH 
-- 调拨单的收货数量和上架开始/结束时间
t00 AS
(SELECT p2.pur_order_sn
        ,SUM(p2.deliver_num) AS deliver_num     -- 收货数量
        ,MAX(p2.gmt_created) AS gmt_created      -- 质检结束时间，也即上架开始时间
        ,MAX(p3.finish_time) AS finish_time     -- 上架结束时间
FROM jolly.who_wms_pur_deliver_goods p2 
INNER JOIN jolly.who_wms_pur_deliver_info  p3 
                 ON p2.deliver_id = p3.deliver_id AND p2.type = 2
GROUP BY p2.pur_order_sn
), 
t01 AS
(SELECT p1.order_id
        ,MAX(demand_gmt_created) AS demand_gmt_created
        ,MAX(allocate_gmt_created) AS allocate_gmt_created
        ,MAX(out_time) AS out_time
        ,FROM_UNIXTIME(MAX(t00.gmt_created)) AS deli_gmt_created
        ,FROM_UNIXTIME(MAX(t00.finish_time))  AS finish_time
        ,SUM(p1.allocate_num) AS allocate_num 
FROM zydb.dw_allocate_out_node p1
LEFT JOIN t00
             ON p1.allocate_order_sn=t00.pur_order_sn 
WHERE 1=1
     AND p1.to_depot_id = 6     -- 调往HK
--AND p1.demand_gmt_created  >= date_sub(FROM_UNIXTIME(unix_timestamp(),'yyyy-MM-dd'),7)
--AND p1.demand_gmt_created < date_sub(FROM_UNIXTIME(unix_timestamp(),'yyyy-MM-dd'),0)
GROUP BY p1.order_id
),
t02 AS
(SELECT p.order_id
        ,p.order_sn
        ,p.goods_number
        ,p.original_goods_number
        ,p.depot_id
        ,p.is_shiped
        ,p.pay_time
        ,p.order_check_time
        ,p.lock_check_time
        ,a.demand_gmt_created AS allocate_demand_start
        ,a.allocate_gmt_created AS allocate_order_start
        ,a.out_time AS allocate_order_out
        ,a.deli_gmt_created AS allocate_order_start_onself
        ,a.finish_time AS allocate_order_finish_onself
        ,p.lock_last_modified_time
        ,p.no_problems_order_uptime
        ,p.outing_stock_time
        ,p.picking_time
        ,p.order_pack_time
        ,p.shipping_time
        ,p4.oos_num
        ,p4.type
        ,FROM_UNIXTIME(p4.create_time) AS create_time
FROM zydb.dw_order_node_time p
LEFT JOIN t01
             ON p.order_id = t01.order_id 
LEFT JOIN jolly.who_wms_order_oos_log p4 
             ON p.order_id = p4.order_id
WHERE 1=1
     AND p.pay_time >= '2017-07-01 00:00:00'  
     AND p.pay_time < '2017-08-01 00:00:00'
--  >= date_sub(FROM_UNIXTIME(unix_timestamp(),'yyyy-MM-dd'),7)
--  < date_sub(FROM_UNIXTIME(unix_timestamp(),'yyyy-MM-dd'),0)
     AND p.depot_id = 6 --只取6
),

t2 AS
(SELECT order_sn
        ,to_date(pay_time) AS pay_date
        ,allocate_demand_start
        ,allocate_order_start
        ,allocate_order_out
        ,allocate_order_start_onself
        ,allocate_order_finish_onself
        ,(unix_timestamp(allocate_order_start) - unix_timestamp(allocate_demand_start)) / 3600 AS 调拨需求响应时长
        ,(unix_timestamp(allocate_order_out) - unix_timestamp(allocate_order_start)) / 3600 AS 调出仓作业时长
        ,(unix_timestamp(allocate_order_start_onself) - unix_timestamp(allocate_order_out)) / 3600 AS 调拨在途时长
        ,(unix_timestamp(allocate_order_finish_onself) - unix_timestamp(allocate_order_start_onself)) / 3600 AS 调拨单上架时长
FROM t1
WHERE allocate_demand_start IS NOT NULL
AND allocate_order_start IS NOT NULL
AND allocate_order_out IS NOT NULL
AND allocate_order_start_onself IS NOT NULL
AND allocate_order_finish_onself IS NOT NULL
AND allocate_order_start > allocate_demand_start
)
















