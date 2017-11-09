
/*
-- 新仓库日报-入库积压
-- 包括质检积压和上架积压
-- 日期：2017-11-7
-- 脚本类型：impala/hive，带日期参数和不带日期参数
 */

--  1.质检积压 impala 带日期参数 ===========================
-- 日期参数取昨日
WITH t1 AS
(SELECT p1.delivered_order_sn
        ,p1.depot_id
        ,p1.end_receipt_time AS qc_start_time
        ,SUM(p1.real_num) AS real_num
        ,SUM(p1.inspect_num) AS inspect_num
        ,SUM(p1.real_num - p1.inspect_num) AS didnt_qc_num
        ,p1.on_shelf_start_time AS qc_finish_time
FROM zydb.dw_delivered_receipt_onself p1
WHERE start_receipt_time >= DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP('${data_date}','yyyyMMdd')),1)
     AND end_receipt_time <FROM_UNIXTIME(UNIX_TIMESTAMP('${data_date}','yyyyMMdd'))
     AND (on_shelf_start_time>=date_add(FROM_UNIXTIME(UNIX_TIMESTAMP('${data_date}','yyyyMMdd')),1)
                OR on_shelf_start_time IS NULL 
                OR on_shelf_start_time='1970-01-01 08:00:00') 
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
        ,SUM(real_num) AS real_num
        ,SUM(inspect_num) AS inspect_num
        ,SUM(didnt_qc_num) AS didnt_qc_num
FROM t1
GROUP BY depot_id
ORDER BY depot_id
;


--  2.质检积压 impala 不带日期参数 ===========================
-- 昨天质检积压商品数
WITH t1 AS
(SELECT p1.delivered_order_sn
        ,p1.depot_id
        ,p1.end_receipt_time AS qc_start_time
        ,SUM(p1.real_num) AS real_num
        ,SUM(p1.inspect_num) AS inspect_num
        ,SUM(p1.real_num - p1.inspect_num) AS didnt_qc_num
        ,p1.on_shelf_start_time AS qc_finish_time
FROM zydb.dw_delivered_receipt_onself p1
WHERE start_receipt_time >= DATE_SUB(TO_DATE(CURRENT_TIMESTAMP()), 2)
     AND end_receipt_time < DATE_SUB(TO_DATE(CURRENT_TIMESTAMP()), 1)
     AND (on_shelf_start_time >= TO_DATE(CURRENT_TIMESTAMP())
                OR on_shelf_start_time IS NULL 
                OR on_shelf_start_time='1970-01-01 08:00:00') 
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
        ,SUM(real_num) AS real_num
        ,SUM(inspect_num) AS inspect_num
        ,SUM(didnt_qc_num) AS didnt_qc_num
FROM t1
GROUP BY depot_id
ORDER BY depot_id
;

--  3.质检积压 hive 不带日期参数 ===========================
-- 昨天质检积压商品数
WITH t1 AS
(SELECT p1.delivered_order_sn
        ,p1.depot_id
        ,p1.end_receipt_time AS qc_start_time
        ,SUM(p1.real_num) AS real_num
        ,SUM(p1.inspect_num) AS inspect_num
        ,SUM(p1.real_num - p1.inspect_num) AS didnt_qc_num
        ,p1.on_shelf_start_time AS qc_finish_time
FROM zydb.dw_delivered_receipt_onself p1
WHERE start_receipt_time >= DATE_SUB(CURRENT_DATE(), 2)
     AND end_receipt_time < DATE_SUB(CURRENT_DATE(), 1)
     AND (on_shelf_start_time >= CURRENT_DATE()
                OR on_shelf_start_time IS NULL 
                OR on_shelf_start_time='1970-01-01 08:00:00') 
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
        ,SUM(real_num) AS real_num
        ,SUM(inspect_num) AS inspect_num
        ,SUM(didnt_qc_num) AS didnt_qc_num
FROM t1
GROUP BY depot_id
ORDER BY depot_id
;













