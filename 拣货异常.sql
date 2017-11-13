
-- 拣货异常记录

SELECT * 
FROM jolly.who_order_goods
WHERE order_id = 31386215
;

SELECT *
FROM jolly.who_order_info
WHERE order_sn = 'JIDA17111007585307397440'
;

SELECT * 
FROM jolly.who_order_info
WHERE order_id = 30608273;


SELECT * 
FROM jolly.who_wms_purchase_returned_exception_detail
LIMIT 10;

SELECT * 
FROM jolly.who_wms_pur_returned_exception_detail
WHERE returned_order_sn = 'JARA17110314575563650240'
-- WHERE returned_order_dn = 'JIDA17111007585307397440'
LIMIT 10
;

SELECT from_unixtime(p1.create_time) AS create_time2
        ,p1.*
FROM jolly.who_wms_order_oos_log p1
WHERE type = 5
-- WHERE order_id = 31386215
ORDER BY create_time DESC
LIMIT 10;


SELECT *
FROM JOLLY.WHO_WMS_RETURNED_ORDER_INFO T
WHERE T.returned_order_sn = 'JIDA17111007585307397440'
;

SELECT *
FROM jolly.who_wms_picking_exception_detail
WHERE order_sn = 'JIDA17111007585307397440'
LIMIT 10;


SELECT from_unixtime(unix_timestamp(), 'yyyy-MM-dd');












