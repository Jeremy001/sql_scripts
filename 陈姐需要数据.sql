
-- 1.每天分配订单量和客单件
-- 以支付时间为准
-- data_date1 = 20170901
-- data_date2 = 20171122
-- 去重订单
WITH t101 AS
(SELECT depod_id
        ,order_id
        ,substr(case when pay_id=41 then pay_time  else result_pay_time end, 1, 10) AS data_date
        ,goods_number
        ,original_goods_number
from zydb.dw_order_sub_order_fact 
where (case when pay_id=41 then pay_time  else result_pay_time end) >=from_unixtime(unix_timestamp('${data_date1}','yyyyMMdd'))
and (case when pay_id=41 then pay_time else result_pay_time end) <date_add(from_unixtime(unix_timestamp('${data_date2}','yyyyMMdd')),1)
and depod_id IN (4, 5, 14)
and pay_status in(1,3)
GROUP BY depod_id
        ,order_id
        ,substr(case when pay_id=41 then pay_time  else result_pay_time end, 1, 10)
        ,goods_number
        ,original_goods_number
),

t1 AS
(select depod_id
        ,data_date
        ,count(order_id) AS order_num
        ,SUM(goods_number) AS goods_num
        ,SUM(original_goods_number) AS org_goods_num
from t101
group by depod_id 
        ,data_date
),

t2 AS
-- 2.每天发货订单数
(SELECT depot_id
        ,substr(shipping_time, 1, 10) AS data_date
        ,count(order_sn) as ship_order_num
FROM zydb.dw_order_node_time
WHERE depot_id IN (4, 5, 14)
AND is_shiped = 1
AND is_problems_order IN (0, 2)
AND order_status = 1
AND pay_status IN (1, 3)
AND shipping_time >= from_unixtime(unix_timestamp('${data_date1}','yyyyMMdd'))
AND shipping_time < date_add(from_unixtime(unix_timestamp('${data_date2}','yyyyMMdd')),1)
GROUP BY depot_id
        ,substr(shipping_time, 1, 10)
),

-- 作业时长
/*t3 AS
(SELECT data_date
        ,(receipt_quality_duration+quality_onshelf_duration+picking_duration+shipping_duration) AS warehouse_op_time
from zydb.rpt_supply_chain_final_table
),*/

-- 每天入库商品数
t4 AS 
(SELECT depot_id
        ,FROM_UNIXTIME(p1.change_time, 'yyyy-MM-dd') AS data_date
        ,SUM(p1.change_num) AS in_num
FROM jolly.who_wms_goods_stock_detail_log p1
WHERE p1.change_type IN (1, 2, 3, 4, 9, 11, 14, 15, 16, 18)
     AND p1.change_time >= UNIX_TIMESTAMP('2017-09-01')
     AND p1.change_time < UNIX_TIMESTAMP('2017-11-30')
GROUP BY depot_id
        ,FROM_UNIXTIME(p1.change_time, 'yyyy-MM-dd')
)


SELECT t1.depod_id
        ,t1.data_date
        ,t1.order_num
        ,t1.goods_num
        ,t2.ship_order_num
        ,t4.in_num
FROM t1
LEFT JOIN t2 
             ON t1.depod_id = t2.depot_id AND t1.data_date = t2.data_date
LEFT JOIN t4 
             ON t1.depod_id = t4.depot_id AND t1.data_date = t4.data_date
ORDER BY t1.depod_id
        ,t1.data_date
;






















