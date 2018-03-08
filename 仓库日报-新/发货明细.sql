/*
-- 内容：发货明细
-- 作者：Neo王政鸣
-- 类型：hive & impala
-- 时间：20171122
日期参数取昨天
 */

-- impala 带日期参数 =====================================
WITH t1 AS
(SELECT depot_id
        ,order_sn
        ,is_shiped
        ,pay_time
        ,outing_stock_time AS can_pick_time
        ,picking_finish_time AS picking_finish_time
        ,order_pack_time AS packing_finish_time
        ,shipping_time
FROM zydb.dw_order_node_time p1
WHERE depot_id IN (4, 5, 6, 14)
AND is_shiped = 1
AND is_problems_order IN (0, 2)
AND order_status = 1
AND pay_status IN (1, 3)
AND shipping_time >= FROM_UNIXTIME(UNIX_TIMESTAMP('$[&data_date]','yyyyMMdd'))
AND shipping_time < DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP('$[&data_date]','yyyyMMdd')),1)
--AND shipping_time >= '2017-10-01'
--AND shipping_time < '2017-10-31'
ORDER BY depot_id
        ,shipping_time
)

SELECT *
FROM t1
ORDER BY shipping_time;





-- impala 不带日期参数 =====================================
SELECT depot_id
        ,order_sn
        ,is_shiped
        ,pay_time
        ,outing_stock_time AS can_pick_time
        ,picking_finish_time AS picking_finish_time
        ,order_pack_time AS packing_finish_time
        ,shipping_time
FROM zydb.dw_order_node_time p1
WHERE depot_id IN (4, 5, 6, 14)
AND is_shiped = 1
AND is_problems_order IN (0, 2)
AND order_status = 1
AND pay_status IN (1, 3)
AND shipping_time >= FROM_UNIXTIME(UNIX_TIMESTAMP('${data_date}','yyyyMMdd'))
AND shipping_time < DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP('${data_date}','yyyyMMdd')),1)
--AND shipping_time >= '2017-10-01'
--AND shipping_time < '2017-10-31'
ORDER BY depot_id
        ,shipping_time;








