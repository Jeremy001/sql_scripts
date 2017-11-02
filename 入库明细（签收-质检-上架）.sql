select *
FROM jolly.who_wms_delivered_receipt_info
limit 10;


select FROM_unixtime(min(gmt_created))
FROM jolly.who_wms_delivered_receipt_info;


select * 
FROM jolly.who_wms_pur_deliver_receipt
limit 10;


select FROM_unixtime(max(gmt_created))
FROM jolly.who_wms_pur_deliver_receipt;

select * 
FROM zydb.dw_delivered_order_info
limit 10;

select * 
FROM zydb.dw_delivered_receipt_onself
limit 10;


-- 每天入库商品数量
SELECT SUBSTR(p1.on_shelf_finish_time, 1, 10) AS on_shelf_date    -- 上架完成时间，即入库时间
        ,SUM(p1.on_shelf_num) AS on_shelf_num
FROM zydb.dw_delivered_receipt_onself p1
WHERE p1.depot_id = 6
GROUP BY SUBSTR(on_shelf_finish_time, 1, 10)
ORDER BY on_shelf_date;


