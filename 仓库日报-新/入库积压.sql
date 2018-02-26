
/*
-- 新仓库日报-入库积压
-- 包括质检积压和上架积压
-- 日期：2017-11-21
-- 脚本类型：impala/hive，带日期参数和不带日期参数
 */

-- =======================================================
-- 1.质检积压
-- 1.质检积压
-- 1.质检积压
-- 例如21号的质检积压的计算方式是：
-- 20号00:00到24:00仓库签收物流包裹中，在20号24:00仍未质检的商品件数
-- =======================================================

--  1.impala 带日期参数 ===========================
-- 日期参数取昨日
WITH t1 AS
(SELECT p1.depot_id
        ,p1.delivered_order_sn
        ,p1.end_receipt_time AS qc_start_time
        ,SUM(p1.delivered_num) AS real_receive_num
        ,SUM(p1.checked_num) AS qc_num
        ,SUM(p1.delivered_num - p1.checked_num) AS didnt_qc_num
        ,p1.on_shelf_start_time AS qc_finish_time
FROM zydb.dw_delivered_receipt_onself p1
WHERE p1.start_receipt_time >= DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP('$[&data_date]','yyyyMMdd')),1)
     AND p1.end_receipt_time <FROM_UNIXTIME(UNIX_TIMESTAMP('$[&data_date]','yyyyMMdd'))
     AND (p1.on_shelf_start_time>=DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP('$[&data_date]','yyyyMMdd')),1)
                OR p1.on_shelf_start_time IS NULL
                OR p1.on_shelf_start_time='1970-01-01 08:00:00')
     AND exp_num=0
GROUP BY p1.delivered_order_sn
        ,p1.depot_id
        ,p1.end_receipt_time
        ,p1.on_shelf_start_time
ORDER BY p1.depot_id
        ,p1.end_receipt_time
)
-- 查询明细
SELECT * FROM t1;

-- 查询各仓质检积压商品总数
SELECT depot_id
        ,SUM(real_receive_num) AS real_receive_num
        ,SUM(qc_num) AS qc_num
        ,SUM(didnt_qc_num) AS didnt_qc_num
FROM t1
GROUP BY depot_id
ORDER BY depot_id
;


--  2.impala 不带日期参数 ===========================
-- 昨天质检积压商品数
WITH t1 AS
(SELECT p1.delivered_order_sn
        ,p1.depot_id
        ,p1.end_receipt_time AS qc_start_time
        ,SUM(p1.real_num) AS real_receive_num
        ,SUM(p1.inspect_num) AS qc_num
        ,SUM(p1.real_num - p1.inspect_num) AS didnt_qc_num
        ,p1.on_shelf_start_time AS qc_finish_time
FROM zydb.dw_delivered_receipt_onself p1
WHERE start_receipt_time >= DATE_SUB(TO_DATE(CURRENT_TIMESTAMP()), 2)
     AND end_receipt_time < DATE_SUB(TO_DATE(CURRENT_TIMESTAMP()), 1)
     AND (on_shelf_start_time >= TO_DATE(CURRENT_TIMESTAMP())
                OR on_shelf_start_time IS NULL OR on_shelf_start_time='1970-01-01 08:00:00')
     AND exp_num=0
GROUP BY p1.delivered_order_sn
        ,p1.depot_id
        ,p1.end_receipt_time
        ,p1.on_shelf_start_time
ORDER BY p1.depot_id
        ,p1.end_receipt_time
)
-- 查询明细
SELECT * FROM t1;

-- 查询各仓质检积压商品总数
SELECT depot_id
        ,SUM(real_receive_num) AS real_receive_num
        ,SUM(qc_num) AS qc_num
        ,SUM(didnt_qc_num) AS didnt_qc_num
FROM t1
GROUP BY depot_id
ORDER BY depot_id
;

--  3.hive 推送脚本 ===========================
-- 昨天质检积压商品数
WITH t1 AS
(SELECT p1.delivered_order_sn
        ,p1.depot_id
        ,p1.end_receipt_time AS qc_start_time
        ,SUM(p1.real_num) AS real_receive_num
        ,SUM(p1.inspect_num) AS qc_num
        ,SUM(p1.real_num - p1.inspect_num) AS didnt_qc_num
        ,p1.on_shelf_start_time AS qc_finish_time
FROM zydb.dw_delivered_receipt_onself p1
WHERE start_receipt_time >= FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE(), 2), 'yyyy-MM-dd'))
     AND end_receipt_time < FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE(), 1), 'yyyy-MM-dd'))
     AND (on_shelf_start_time >= FROM_UNIXTIME(UNIX_TIMESTAMP(CURRENT_DATE(), 'yyyy-MM-dd'))
                OR on_shelf_start_time IS NULL OR on_shelf_start_time='1970-01-01 08:00:00')
     AND exp_num=0
GROUP BY p1.delivered_order_sn
        ,p1.depot_id
        ,p1.end_receipt_time
        ,p1.on_shelf_start_time
ORDER BY p1.depot_id
        ,p1.end_receipt_time
)
-- 查询明细
SELECT * FROM t1;

-- 查询各仓质检积压商品总数
SELECT depot_id
        ,SUM(real_receive_num) AS real_receive_num
        ,SUM(qc_num) AS qc_num
        ,SUM(didnt_qc_num) AS didnt_qc_num
FROM t1
GROUP BY depot_id
ORDER BY depot_id
;

-- =======================================================


-- =======================================================
-- 2.上架积压
-- 2.上架积压
-- 2.上架积压
-- 上架积压较少，因此数据通常为空；
-- 21号的上架积压的计算方式：
-- 20号00:00到20:00质检完成商品，到21号早上6:00仍未完成上架的商品件数
-- =======================================================

-- hive hue 带日期参数 ===================================
WITH
t1 AS
(SELECT p1.delivered_order_sn
        ,p1.depot_id
        ,p1.on_shelf_start_time AS onshelf_start_time
        ,p1.on_shelf_finish_time AS onshelf_finish_time
        ,SUM(p1.inspect_num) AS qc_num
        ,SUM(p1.on_shelf_num) AS onshelf_num
        ,SUM(p1.inspect_num - p1.on_shelf_num) AS didnt_onshelf_num
FROM zydb.dw_delivered_receipt_onself p1
WHERE p1.on_shelf_start_time >= DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP('${data_date}','yyyyMMdd')),1)
     AND p1.on_shelf_start_time <FROM_UNIXTIME(UNIX_TIMESTAMP('${data_date}','yyyyMMdd'))
     AND (p1.on_shelf_finish_time>=CONCAT(TO_DATE(DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP('${data_date}','yyyyMMdd')),1)),' 06:00:00')
                OR p1.on_shelf_finish_time IS NULL
                OR p1.on_shelf_finish_time='1970-01-01 08:00:00')
     AND p1.exp_num=0
GROUP BY p1.delivered_order_sn
        ,p1.depot_id
        ,p1.on_shelf_start_time
        ,p1.on_shelf_finish_time
)
-- 查询明细
SELECT * FROM t1;

-- 查询各仓上架积压商品总数
SELECT depot_id
        ,SUM(qc_num) AS qc_num
        ,SUM(onshelf_num) AS onshelf_num
        ,SUM(didnt_onshelf_num) AS didnt_onshelf_num
FROM t1
GROUP BY depot_id
ORDER BY depot_id
;



-- hive  推送脚本 ===================================
WITH
t1 AS
(SELECT p1.delivered_order_sn
        ,p1.depot_id
        ,p1.on_shelf_start_time AS onshelf_start_time
        ,p1.on_shelf_finish_time AS onshelf_finish_time
        ,SUM(p1.inspect_num) AS qc_num
        ,SUM(p1.on_shelf_num) AS onshelf_num
        ,SUM(p1.inspect_num - p1.on_shelf_num) AS didnt_onshelf_num
FROM zydb.dw_delivered_receipt_onself p1
WHERE p1.on_shelf_start_time >= FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE(), 2), 'yyyy-MM-dd'))
     AND p1.on_shelf_start_time < FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(DATE_SUB(CURRENT_DATE(), 2), ' 20:00:00'), 'yyyy-MM-dd HH:mm:ss'))
     AND (p1.on_shelf_finish_time>=CONCAT(DATE_SUB(CURRENT_DATE(), 1),' 06:00:00')
                OR p1.on_shelf_finish_time IS NULL
                OR p1.on_shelf_finish_time='1970-01-01 08:00:00')
     AND p1.exp_num=0
GROUP BY p1.delivered_order_sn
        ,p1.depot_id
        ,p1.on_shelf_start_time
        ,p1.on_shelf_finish_time
)
-- 查询明细
SELECT * FROM t1;

-- 查询各仓上架积压商品总数
SELECT depot_id
        ,SUM(qc_num) AS qc_num
        ,SUM(onshelf_num) AS onshelf_num
        ,SUM(didnt_onshelf_num) AS didnt_onshelf_num
FROM t1
GROUP BY depot_id
ORDER BY depot_id
;





