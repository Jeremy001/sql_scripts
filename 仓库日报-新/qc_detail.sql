-- 作者：王政鸣
-- 更新时间：2018-02-07
-- SQL脚本类型：hive

-- 0.选定zybiro库，zybiro账号只对zybiro库有DML和DDL操作权限
use zybiro;

-- 1.DROP TABLE
drop table if exists neo_qc_detail;

-- 2.CREATE TABLE
CREATE TABLE neo_qc_detail
AS
WITH
-- 到货单应到数量、实到数量、签收时间、质检数量、质检时间
t1 AS
(SELECT p1.depot_id
        ,p1.delivered_order_sn
        ,p1.total_num
        ,p1.inspect_num
        ,MIN(p1.start_receipt_time) AS receipt_time
        ,MIN(p1.first_check_time) AS begin_qc_time
        ,MAX(p1.finish_check_time) AS finish_qc_time
FROM zydb.dw_delivered_receipt_onself AS p1
WHERE p1.source_type < 3  -- 剔除调拨单，因为调拨单不质检
GROUP BY p1.depot_id
        ,p1.delivered_order_sn
        ,p1.total_num
        ,p1.inspect_num
),
t2 AS
(SELECT t1.*
FROM t1
WHERE t1.begin_qc_time >= TO_DATE(DATE_SUB(CURRENT_TIMESTAMP(), 1))
  AND t1.begin_qc_time <  TO_DATE(DATE_SUB(CURRENT_TIMESTAMP(), 0))
)
SELECT *
FROM t2
;




