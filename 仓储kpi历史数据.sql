/*
-- 内容：仓储&物流kpi历史数据
-- 作者：Neo王政鸣
-- 时间：2018-03-02
*/


-- 仓库作业时长 ===============================================
-- daily
WITH
-- 1.质检时长
t1 AS
(select TO_DATE(t1.on_shelf_start_time) AS data_date
        ,SUM(((unix_timestamp(t1.on_shelf_start_time)-unix_timestamp(t1.start_receipt_time))/3600/24)*t1.num)/sum(t1.num)*24 AS qc_duration
from zydb.dw_delivered_receipt_onself AS t1
where t1.on_shelf_start_time >= '2017-01-01'
and t1.on_shelf_start_time < '2018-03-01'
and ((unix_timestamp(on_shelf_start_time)-unix_timestamp(start_receipt_time))/3600/24)>0
group by TO_DATE(t1.on_shelf_start_time)
),
-- 2.上架时长
t2 AS
(select TO_DATE(t1.on_shelf_finish_time) AS data_date
        ,sum((unix_timestamp(on_shelf_finish_time)-unix_timestamp(on_shelf_start_time))*on_shelf_num)/sum(on_shelf_num)/3600 onshelf_duration
from
(select a.depot_id,
c.on_shelf_num,
c.on_shelf_start_time,
c.on_shelf_finish_time
from zydb.dw_order_node_time a
left join  zydb.dw_demand_pur b on a.order_id=b.order_id
left join  zydb.dw_delivered_receipt_onself c
on b.pur_order_sn=c.delivered_order_sn
and b.sku_id=c.sku_id
where c.on_shelf_finish_time >= '2017-01-01'
and c.on_shelf_finish_time < '2018-03-01'
and unix_timestamp(on_shelf_finish_time)>unix_timestamp(on_shelf_start_time)
and source_type=2
) AS t1
GROUP BY TO_DATE(t1.on_shelf_finish_time)
),
-- 3.拣货时长
-- picking_finish_time从20170316开始
t3 AS
(select TO_DATE(t1.picking_finish_time) AS data_date
        ,sum(unix_timestamp(picking_finish_time) -unix_timestamp(outing_stock_time))/count(*)/3600 pick_duration
from  zydb.dw_order_node_time AS t1
where t1.picking_finish_time >= '2017-01-01'
and t1.picking_finish_time < '2018-03-01'
group by TO_DATE(t1.picking_finish_time)
),
-- 4.打包时长
-- picking_finish_time从20170316开始
t4 AS
(select TO_DATE(t1.order_pack_time) AS data_date
        ,sum(unix_timestamp(order_pack_time) -unix_timestamp(picking_finish_time))/count(*)/3600 pack_duration
from  zydb.dw_order_node_time AS t1
where t1.order_pack_time >= '2017-01-01'
and t1.order_pack_time < '2018-03-01'
group by TO_DATE(t1.order_pack_time)
),
-- 5.发货时长
t5 AS
(select TO_DATE(t1.shipping_time) AS data_date
        ,sum(unix_timestamp(shipping_time) -unix_timestamp(order_pack_time))/count(*)/3600 ship_duration
from  zydb.dw_order_node_time AS t1
where t1.shipping_time >= '2017-01-01'
and t1.shipping_time < '2018-03-01'
group by TO_DATE(t1.shipping_time)
),
-- 6.求和，得到总作业时长
t6 AS
(select t1.*
        ,t2.onshelf_duration
        ,t3.pick_duration
        ,t4.pack_duration
        ,t5.ship_duration
        ,(t1.qc_duration + t2.onshelf_duration + t3.pick_duration + t4.pack_duration + t5.ship_duration) AS wh_total_time
FROM t1
LEFT JOIN t2 ON t1.data_date = t2.data_date
LEFT JOIN t3 ON t1.data_date = t3.data_date
LEFT JOIN t4 ON t1.data_date = t4.data_date
LEFT JOIN t5 ON t1.data_date = t5.data_date
)
-- 7.结果，计算各月的平均作业时长
SELECT SUBSTR(data_date, 1, 7) AS data_month
        ,AVG(wh_total_time) AS wh_avg_time
FROM t6
WHERE wh_total_time IS NOT NULL
GROUP BY SUBSTR(data_date, 1, 7)
ORDER BY data_month
;


-- 调拨时长 =======================================================
WITH
-- 1.20170813前的数据
t1 AS
(select
t.deliver_id,t.deliver_sn,
s.pur_order_id,s.pur_order_sn,
s.pur_order_goods_rec_id ,
t.gmt_created gmt_created,
t.finish_time finish_time,
t.status,t.total_num,t.depot_id,t.from_type
from jolly.who_wms_pur_deliver_info t
inner join jolly.who_wms_pur_deliver_goods s on t.deliver_id=s.deliver_id
where  t.from_type=3
and regexp_replace(substr(from_unixtime(t.finish_time),1,10),'-','') >= '20170101'
AND regexp_replace(substr(from_unixtime(t.finish_time),1,10),'-','') <  '20180301'
and s.is_new=0
and t.is_new=0
),
-- 2.20170813后的数据
t2 AS
(select
t.deliver_id,t.deliver_sn,
s.pur_order_id,s.pur_order_sn,
s.pur_order_goods_rec_id ,
t.gmt_created gmt_created,
t.finish_time finish_time,
t.status,t.total_num,t.depot_id,t.from_type
from jolly.who_wms_pur_deliver_info t
inner join jolly.who_wms_pur_deliver_goods s on t.deliver_id=s.deliver_id
where  t.from_type=3
and regexp_replace(substr(from_unixtime(t.finish_time),1,10),'-','') >= '20170101'
AND regexp_replace(substr(from_unixtime(t.finish_time),1,10),'-','') <  '20180301'
and s.is_new=1
and t.is_new=1
),
-- 3.union t1 & t2
t3 AS
(SELECT t1.* FROM t1
UNION ALL
SELECT t2.* FROM t2
),
-- 4.查询调拨单的发货和签收时间
t4 AS
(select
pur_order_sn,
min(b.gmt_created) allo_start_time,
max(t3.finish_time) arrive_time
from  jolly.who_wms_allocate_order_info  b   --主键调拨单表
inner join jolly.who_wms_delivered_order_info c
on b.allocate_order_sn = c.delivered_order_sn
inner join t3
ON c.delivered_order_id=t3.pur_order_id
WHERE regexp_replace(substr(from_unixtime(t3.finish_time),1,10),'-','') >= '20170101'
  AND regexp_replace(substr(from_unixtime(t3.finish_time),1,10),'-','') <  '20180301'
group by pur_order_sn
),
-- 5.计算每天的调拨时长
t5 AS
(select FROM_UNIXTIME(arrive_time, 'yyyy-MM-dd') AS data_date
,round(avg((arrive_time -allo_start_time)/3600),2) allocate_time -- 平均到货用时
from t4
WHERE arrive_time >= allo_start_time
GROUP BY FROM_UNIXTIME(arrive_time, 'yyyy-MM-dd')
)
-- 6.结果，计算每个月的平均调拨时长
SELECT SUBSTR(data_date, 1, 7) AS data_month
        ,AVG(allocate_time) AS allocate_avg_time
FROM t5
WHERE allocate_time IS NOT NULL
GROUP BY SUBSTR(data_date, 1, 7)
ORDER BY data_month
;


-- 物流时长：从发货到签收 =======================================================
WITH
-- 1.每天物流时长
t1 AS
(SELECT SUBSTR(p2.receipt_time, 1, 10) AS receipt_date
        ,AVG((UNIX_TIMESTAMP(p2.receipt_time) - UNIX_TIMESTAMP(p1.shipping_time))/3600) AS ship_receipt_duration
FROM zydb.dw_order_node_time AS p1
LEFT JOIN zydb.dw_order_shipping_tracking_node AS p2
       ON p1.order_id = p2.order_id
WHERE p2.receipt_time >= '2017-01-01'
  AND p2.receipt_time <  '2018-03-01'
  AND p2.receipt_time IS NOT NULL
  AND p1.shipping_time IS NOT NULL
  AND p2.receipt_time >= p1.shipping_time
GROUP BY SUBSTR(p2.receipt_time, 1, 10)
)
-- 2.结果，每个月平均物流时长
SELECT SUBSTR(t1.receipt_date, 1, 7) AS receipt_month
        ,AVG(ship_receipt_duration) AS wuliu_avg_duration
FROM t1
GROUP BY SUBSTR(t1.receipt_date, 1, 7)
ORDER BY receipt_month
;


-- 发货金额签收占比 =============================================================
-- 签收订单金额：上上月发货且签收的订单金额
-- 发货订单金额：上上月发货的订单金额
WITH
-- 1.订单发货和签收时间
t1 AS
(SELECT p1.order_id
        ,p1.shipping_time
        ,p2.receipt_time
        ,p3.order_amount_no_bonus
FROM zydb.dw_order_node_time AS p1
LEFT JOIN zydb.dw_order_shipping_tracking_node AS p2
       ON p1.order_id = p2.order_id
LEFT JOIN zydb.dw_order_sub_order_fact AS p3
       ON p1.order_id = p3.order_id
WHERE p1.shipping_time >= '2016-11-01'
  AND p1.shipping_time <  '2018-01-01'
  AND p2.receipt_time IS NOT NULL
  AND p1.shipping_time IS NOT NULL
  AND p2.receipt_time >= p1.shipping_time
),
-- 2.逐月统计
t2 AS
(
-- 2017-01
SELECT '2017-01' AS data_month
        ,SUM(t1.order_amount_no_bonus) AS ship_amount
        ,SUM(CASE WHEN t1.receipt_time >= '2016-11-01'
                   AND t1.receipt_time <  '2017-02-01'
                  THEN t1.order_amount_no_bonus
                  ELSE 0
             END) AS receipt_amount
FROM t1
WHERE t1.shipping_time >= '2016-11-01'
  AND t1.shipping_time  < '2016-12-01'
UNION
-- 2017-02
SELECT '2017-02' AS data_month
        ,SUM(t1.order_amount_no_bonus) AS ship_amount
        ,SUM(CASE WHEN t1.receipt_time >= '2016-12-01'
                   AND t1.receipt_time <  '2017-03-01'
                  THEN t1.order_amount_no_bonus
                  ELSE 0
             END) AS receipt_amount
FROM t1
WHERE t1.shipping_time >= '2016-12-01'
  AND t1.shipping_time  < '2017-01-01'
UNION
-- 2017-03
SELECT '2017-03' AS data_month
        ,SUM(t1.order_amount_no_bonus) AS ship_amount
        ,SUM(CASE WHEN t1.receipt_time >= '2017-01-01'
                   AND t1.receipt_time <  '2017-04-01'
                  THEN t1.order_amount_no_bonus
                  ELSE 0
             END) AS receipt_amount
FROM t1
WHERE t1.shipping_time >= '2017-01-01'
  AND t1.shipping_time  < '2017-02-01'
UNION
-- 2017-04
SELECT '2017-04' AS data_month
        ,SUM(t1.order_amount_no_bonus) AS ship_amount
        ,SUM(CASE WHEN t1.receipt_time >= '2017-02-01'
                   AND t1.receipt_time <  '2017-05-01'
                  THEN t1.order_amount_no_bonus
                  ELSE 0
             END) AS receipt_amount
FROM t1
WHERE t1.shipping_time >= '2017-02-01'
  AND t1.shipping_time  < '2017-03-01'
UNION
-- 2017-05
SELECT '2017-05' AS data_month
        ,SUM(t1.order_amount_no_bonus) AS ship_amount
        ,SUM(CASE WHEN t1.receipt_time >= '2017-03-01'
                   AND t1.receipt_time <  '2017-06-01'
                  THEN t1.order_amount_no_bonus
                  ELSE 0
             END) AS receipt_amount
FROM t1
WHERE t1.shipping_time >= '2017-03-01'
  AND t1.shipping_time  < '2017-04-01'
UNION
-- 2017-06
SELECT '2017-06' AS data_month
        ,SUM(t1.order_amount_no_bonus) AS ship_amount
        ,SUM(CASE WHEN t1.receipt_time >= '2017-04-01'
                   AND t1.receipt_time <  '2017-07-01'
                  THEN t1.order_amount_no_bonus
                  ELSE 0
             END) AS receipt_amount
FROM t1
WHERE t1.shipping_time >= '2017-04-01'
  AND t1.shipping_time  < '2017-05-01'
UNION
-- 2017-07
SELECT '2017-07' AS data_month
        ,SUM(t1.order_amount_no_bonus) AS ship_amount
        ,SUM(CASE WHEN t1.receipt_time >= '2017-05-01'
                   AND t1.receipt_time <  '2017-08-01'
                  THEN t1.order_amount_no_bonus
                  ELSE 0
             END) AS receipt_amount
FROM t1
WHERE t1.shipping_time >= '2017-05-01'
  AND t1.shipping_time  < '2017-06-01'
UNION
-- 2017-08
SELECT '2017-08' AS data_month
        ,SUM(t1.order_amount_no_bonus) AS ship_amount
        ,SUM(CASE WHEN t1.receipt_time >= '2017-06-01'
                   AND t1.receipt_time <  '2017-09-01'
                  THEN t1.order_amount_no_bonus
                  ELSE 0
             END) AS receipt_amount
FROM t1
WHERE t1.shipping_time >= '2017-06-01'
  AND t1.shipping_time  < '2017-07-01'
UNION
-- 2017-09
SELECT '2017-09' AS data_month
        ,SUM(t1.order_amount_no_bonus) AS ship_amount
        ,SUM(CASE WHEN t1.receipt_time >= '2017-07-01'
                   AND t1.receipt_time <  '2017-10-01'
                  THEN t1.order_amount_no_bonus
                  ELSE 0
             END) AS receipt_amount
FROM t1
WHERE t1.shipping_time >= '2017-07-01'
  AND t1.shipping_time  < '2017-08-01'
UNION
-- 2017-10
SELECT '2017-10' AS data_month
        ,SUM(t1.order_amount_no_bonus) AS ship_amount
        ,SUM(CASE WHEN t1.receipt_time >= '2017-08-01'
                   AND t1.receipt_time <  '2017-11-01'
                  THEN t1.order_amount_no_bonus
                  ELSE 0
             END) AS receipt_amount
FROM t1
WHERE t1.shipping_time >= '2017-08-01'
  AND t1.shipping_time  < '2017-09-01'
UNION
-- 2017-11
SELECT '2017-11' AS data_month
        ,SUM(t1.order_amount_no_bonus) AS ship_amount
        ,SUM(CASE WHEN t1.receipt_time >= '2017-09-01'
                   AND t1.receipt_time <  '2017-12-01'
                  THEN t1.order_amount_no_bonus
                  ELSE 0
             END) AS receipt_amount
FROM t1
WHERE t1.shipping_time >= '2017-09-01'
  AND t1.shipping_time  < '2017-10-01'
UNION
-- 2017-12
SELECT '2017-12' AS data_month
        ,SUM(t1.order_amount_no_bonus) AS ship_amount
        ,SUM(CASE WHEN t1.receipt_time >= '2017-10-01'
                   AND t1.receipt_time <  '2018-01-01'
                  THEN t1.order_amount_no_bonus
                  ELSE 0
             END) AS receipt_amount
FROM t1
WHERE t1.shipping_time >= '2017-10-01'
  AND t1.shipping_time  < '2017-11-01'
UNION
-- 2018-01
SELECT '2018-01' AS data_month
        ,SUM(t1.order_amount_no_bonus) AS ship_amount
        ,SUM(CASE WHEN t1.receipt_time >= '2017-11-01'
                   AND t1.receipt_time <  '2018-02-01'
                  THEN t1.order_amount_no_bonus
                  ELSE 0
             END) AS receipt_amount
FROM t1
WHERE t1.shipping_time >= '2017-11-01'
  AND t1.shipping_time  < '2017-12-01'
UNION
-- 2018-02
SELECT '2018-02' AS data_month
        ,SUM(t1.order_amount_no_bonus) AS ship_amount
        ,SUM(CASE WHEN t1.receipt_time >= '2017-12-01'
                   AND t1.receipt_time <  '2018-03-01'
                  THEN t1.order_amount_no_bonus
                  ELSE 0
             END) AS receipt_amount
FROM t1
WHERE t1.shipping_time >= '2017-12-01'
  AND t1.shipping_time  < '2018-01-01'
)
SELECT *
FROM t2
ORDER BY data_month
;
