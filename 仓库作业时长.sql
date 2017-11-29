/*
-- 内容：dashboard所需的仓库作业数据：作业量、作业时长
-- 时间：20171127
-- 作者：Neo王政鸣
 */


WITH 
-- 质检时长
t1 AS
(SELECT a.depot_id
        ,SUBSTR(a.on_shelf_start_time, 1, 10) AS data_date
        ,SUM(((UNIX_TIMESTAMP(on_shelf_start_time)-UNIX_TIMESTAMP(start_receipt_time))/3600/24)*num)/SUM(num)*24  AS receipt_quality_duration
FROM zydb.dw_delivered_receipt_onself a
WHERE a.on_shelf_start_time >= FROM_UNIXTIME(UNIX_TIMESTAMP('20160101','yyyyMMdd'))
AND a.on_shelf_start_time < TO_DATE(CURRENT_TIMESTAMP())
AND ((UNIX_TIMESTAMP(on_shelf_start_time) - UNIX_TIMESTAMP(start_receipt_time))/3600/24)>0
GROUP BY a.depot_id
        ,SUBSTR(a.on_shelf_start_time, 1, 10) 
),

--上架时长 
t2 AS
(SELECT depot_id
        ,SUBSTR(a.on_shelf_finish_time, 1, 10) AS data_date
        ,SUM(((UNIX_TIMESTAMP(on_shelf_finish_time)-UNIX_TIMESTAMP(on_shelf_start_time))/3600/24)*on_shelf_num)/SUM(on_shelf_num)*24  AS quality_onshelf_duration
FROM zydb.dw_delivered_receipt_onself a
WHERE a.on_shelf_finish_time >= FROM_UNIXTIME(UNIX_TIMESTAMP('20160101','yyyyMMdd'))
AND a.on_shelf_finish_time < TO_DATE(CURRENT_TIMESTAMP())
GROUP BY depot_id
        ,SUBSTR(a.on_shelf_finish_time, 1, 10)
),

--拣货时长
t3 AS
(SELECT depod_id AS depot_id
        ,SUBSTR(shipping_time, 1, 10) AS data_date
        ,SUM(finish_time-gmt_created)/count(order_id)/3600 picking_duration
FROM 
        (SELECT a.depod_id,a.order_id,a.shipping_time, c.finish_time ,b.gmt_created 
        FROM zydb.dw_order_sub_order_fact a
        LEFT JOIN  
        --可捡货
        (SELECT order_id,MAX(gmt_created) gmt_created 
        FROM jolly.who_wms_outing_stock_detail GROUP BY order_id
        UNION ALL 
        SELECT order_id,MAX(gmt_created) gmt_created 
        FROM jolly_wms.who_wms_outing_stock_detail GROUP BY order_id
        ) b
        ON a.order_id=b.order_id
        LEFT JOIN 
        (
        ---捡货完成
        SELECT order_id,MAX(finish_time) finish_time
        FROM jolly.who_wms_picking_goods_detail a
        LEFT JOIN jolly.who_wms_picking_info b
        ON a.picking_id=b.picking_id
        GROUP BY order_id
        UNION ALL 
        SELECT order_id,MAX(finish_time) finish_time
        FROM
        jolly_wms.who_wms_picking_goods_detail a
        LEFT JOIN  
        jolly_wms.who_wms_picking_info b
        ON a.picking_id=b.picking_id
        GROUP BY order_id
        )c
        ON a.order_id=c.order_id
        WHERE shipping_time >= FROM_UNIXTIME(UNIX_TIMESTAMP('20160101','yyyyMMdd'))
        AND shipping_time < TO_DATE(CURRENT_TIMESTAMP())
        )a
GROUP BY depod_id
        ,SUBSTR(shipping_time, 1, 10)
),

--打包时长
t4 AS
(SELECT a.depod_id AS depot_id
        ,SUBSTR(a.shipping_time, 1, 10) AS data_date
        ,SUM((UNIX_TIMESTAMP(a.order_pack_time)-c.finish_time)*b.real_picking_num)/SUM(b.real_picking_num)/3600 package_duration
FROM zydb.dw_order_sub_order_fact  a
LEFT JOIN 
(
SELECT order_id,picking_id,real_picking_num FROM jolly.who_wms_picking_goods_detail 
UNION ALL
SELECT order_id,picking_id,real_picking_num FROM jolly_wms.who_wms_picking_goods_detail 
) b
ON a.order_id=b.order_id
LEFT JOIN  
(
SELECT picking_id,finish_time FROM jolly.who_wms_picking_info
UNION ALL
SELECT picking_id,finish_time FROM jolly_wms.who_wms_picking_info
)c
ON b.picking_id=c.picking_id
WHERE shipping_time >= FROM_UNIXTIME(UNIX_TIMESTAMP('20160101','yyyyMMdd'))
AND shipping_time< TO_DATE(CURRENT_TIMESTAMP())
GROUP BY a.depod_id
        ,SUBSTR(a.shipping_time, 1, 10)
),

--发货时长
t5 AS
(SELECT depod_id AS depot_id
        ,SUBSTR(shipping_time, 1, 10) AS data_date
        ,avg(UNIX_TIMESTAMP(shipping_time)-UNIX_TIMESTAMP(order_pack_time))/3600 AS shipping_duration
FROM zydb.dw_order_sub_order_fact  
WHERE shipping_time >= FROM_UNIXTIME(UNIX_TIMESTAMP('20160101','yyyyMMdd'))
AND shipping_time < TO_DATE(CURRENT_TIMESTAMP())
GROUP BY depod_id
        ,SUBSTR(shipping_time, 1, 10)
)

-- JOIN 得到各个环节的时效以及计算总时长
SELECT t1.*
        ,t2.quality_onshelf_duration
        ,t3.picking_duration
        ,t4.package_duration
        ,t5.shipping_duration
        ,(t1.receipt_quality_duratiON + t2.quality_onshelf_duratiON + t3.picking_duratiON + t4.package_duratiON + t5.shipping_duration) AS wh_total_duration
FROM t1
LEFT JOIN t2 ON t1.data_date = t2.data_date AND t1.depot_id = t2.depot_id
LEFT JOIN t3 ON t1.data_date = t3.data_date AND t1.depot_id = t3.depot_id
LEFT JOIN t4 ON t1.data_date = t4.data_date AND t1.depot_id = t4.depot_id
LEFT JOIN t5 ON t1.data_date = t5.data_date AND t1.depot_id = t5.depot_id
;








