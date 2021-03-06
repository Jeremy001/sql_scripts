-- 作者：王政鸣
-- 更新时间：2018-03-09
-- SQL脚本类型：hive

-- 0.选定zybiro库，zybiro账号只对zybiro库有DML和DDL操作权限
USE zybiro;

-- 1.DROP TABLE
DROP TABLE IF EXISTS zybiro.neo_qc_backlog;

-- 2.CREATE TABLE
CREATE TABLE zybiro.neo_qc_backlog
AS
WITH t1 AS
(SELECT p1.delivered_order_sn
        ,p1.depot_id
        ,MIN(p1.end_receipt_time) AS qc_start_time
        ,SUM(p1.delivered_num) AS real_receive_num
        ,SUM(p1.checked_num) AS qc_num
        ,SUM(p1.delivered_num - p1.checked_num) AS didnt_qc_num
        ,MAX(p1.on_shelf_start_time) AS qc_finish_time
FROM zydb.dw_delivered_receipt_onself AS p1
LEFT JOIN jolly.who_wms_delivered_order_exp_goods AS p2
       ON p1.delivered_order_sn = p2.delivered_order_sn
WHERE p1.start_receipt_time >= FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE(), 2), 'yyyy-MM-dd'))
  AND p1.end_receipt_time < FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE(), 1), 'yyyy-MM-dd'))
  AND (p1.on_shelf_start_time >= FROM_UNIXTIME(UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE(), 0), 'yyyy-MM-dd'))
       OR p1.on_shelf_start_time IS NULL
       OR p1.on_shelf_start_time='1970-01-01 08:00:00')
  AND p1.exp_num=0
  AND p2.delivered_order_sn IS NULL
GROUP BY p1.delivered_order_sn
        ,p1.depot_id
)
-- 查询明细
SELECT *
FROM t1
ORDER BY t1.depot_id
        ,t1.delivered_order_sn
;
