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
        ,t01.demand_gmt_created AS allocate_demand_start    -- 调拨需求开始时间
        ,t01.allocate_gmt_created AS allocate_order_start    -- 订单需求所在调拨单开始生成时间
        ,t01.out_time AS allocate_order_out    -- 调拨单出库时间
        ,t01.deli_gmt_created AS allocate_order_start_onself    -- 订单需求所在调拨单开始入库时间
        ,t01.finish_time AS allocate_order_finish_onself    -- 订单需求所在调拨单完成入库时间
        ,p.lock_last_modified_time
        ,p.no_problems_order_uptime
        ,p.outing_stock_time
        ,p.picking_time
        ,p.order_pack_time
        ,p.shipping_time
        ,t01.allocate_num
        ,p4.oos_num
        ,p4.type
        ,FROM_UNIXTIME(p4.create_time) AS create_time
FROM zydb.dw_order_node_time p
LEFT JOIN t01
             ON p.order_id = t01.order_id 
LEFT JOIN jolly.who_wms_order_oos_log p4 
             ON p.order_id = p4.order_id
WHERE 1=1
     AND p.pay_time >= '2017-04-01 00:00:00'  
     AND p.pay_time < '2017-10-01 00:00:00'
--  >= date_sub(FROM_UNIXTIME(unix_timestamp(),'yyyy-MM-dd'),7)
--  < date_sub(FROM_UNIXTIME(unix_timestamp(),'yyyy-MM-dd'),0)
     AND p.depot_id = 6 --只取6
),

t03 AS
(SELECT order_sn
        ,TO_DATE(pay_time) AS pay_date
        ,SUBSTR(CAST(pay_time AS string), 1, 7) AS pay_month
        ,allocate_num
        ,allocate_demand_start
        ,allocate_order_start
        ,allocate_order_out
        ,allocate_order_start_onself
        ,allocate_order_finish_onself
        ,((unix_timestamp(allocate_order_start) - unix_timestamp(allocate_demand_start)) / 3600) AS allocate_response_duration    -- 调拨需求响应时长 = 调拨单生成时间 - 调拨需求生成时间
        ,((unix_timestamp(allocate_order_out) - unix_timestamp(allocate_order_start)) / 3600) AS allocate_work_duration    -- 调出仓作业时长 = 出库时间 - 调拨单生成时间
        ,((unix_timestamp(allocate_order_start_onself) - unix_timestamp(allocate_order_out)) / 3600) AS allocate_onway_duration    -- 调拨在途时长 = 开始入库时间 - 出库时间
        ,((unix_timestamp(allocate_order_finish_onself) - unix_timestamp(allocate_order_start_onself)) / 3600) AS allocate_onshelf_duration    -- 调拨单上架时长 = 入库完成时间 - 开始入库时间
FROM t02
WHERE allocate_demand_start IS NOT NULL
AND allocate_order_start IS NOT NULL
AND allocate_order_out IS NOT NULL
AND allocate_order_start_onself IS NOT NULL
AND allocate_order_finish_onself IS NOT NULL
AND allocate_order_start > allocate_demand_start
)

-- 每天调拨各节点平均时长
SELECT pay_date
        ,AVG(allocate_response_duration) AS 调拨需求平均响应时长
        ,AVG(allocate_work_duration) AS 调出仓平均作业时长
        ,AVG(allocate_onway_duration) AS 调拨单平均在途时长
        ,AVG(allocate_onshelf_duration) AS 调拨单平均上架时长
        ,SUM(allocate_num) AS 调拨数量
        ,COUNT(order_sn) AS 订单数量
FROM t03
GROUP BY pay_date
ORDER BY pay_date;

-- 每月调拨各节点平均时长
SELECT pay_month
        ,AVG(allocate_response_duration) AS 调拨需求平均响应时长
        ,AVG(allocate_work_duration) AS 调出仓平均作业时长
        ,AVG(allocate_onway_duration) AS 调拨单平均在途时长
        ,AVG(allocate_onshelf_duration) AS 调拨单平均上架时长
        ,SUM(allocate_num) AS 调拨数量
        ,COUNT(order_sn) AS 订单数量
FROM t03
GROUP BY pay_month
ORDER BY pay_month;
















