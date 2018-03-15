-- 作者：王政鸣
-- 更新时间：2017-6-26
-- SQL脚本类型：hive

-- 0.选定zybiro库，zybiro账号只对zybiro库有DML和DDL操作权限
USE ZYBIRO;

-- 1.DROP TABLE
DROP TABLE IF EXISTS Neo_Pick_Unusual;

-- 2.CREATE TABLE
CREATE TABLE Neo_Pick_Unusual
AS
SELECT T.DEPOT_ID
        ,T.RETURNED_REC_ID
        ,T.RETURNED_ORDER_ID
        ,T.RETURNED_ORDER_SN
        ,(CASE WHEN T.RETURN_TYPE = 1 THEN '退款（无需退货）'
                      WHEN T.RETURN_TYPE = 2 THEN '退货退款'
                      WHEN T.RETURN_TYPE = 1 THEN '部分退货'
                      WHEN T.RETURN_TYPE = 1 THEN '退货(无需退款)'
                      ELSE NULL END) AS return_type    -- 退货退款类型
        ,FROM_UNIXTIME(T.RETURNED_TIME) AS return_time
        ,T.ADMIN_ID
        ,'拣货异常' AS return_reason
        ,(CASE WHEN T.RETURNED_OP_TYPE = 1 THEN '1整单'
                      WHEN T.RETURNED_OP_TYPE = 2 THEN '2部分'
                      ELSE NULL END) AS return_op_type
        ,T.RETURNED_ORDER_STATUS
        ,T.REMARK
        ,(CASE WHEN T.DUTY_PARTY = 0 THEN '0:其他'
                      WHEN T.DUTY_PARTY = 1 THEN '1:我方责任'
                      WHEN T.DUTY_PARTY = 2 THEN '2:用户责任'
                      ELSE NULL END) AS duty_party
FROM JOLLY.WHO_WMS_RETURNED_ORDER_INFO T
WHERE 1=1
    AND T.RETURNED_TIME >= UNIX_TIMESTAMP(DATE_SUB(CURRENT_TIMESTAMP(), 1), 'yyyy-MM-dd')
    AND T.RETURNED_TIME <= UNIX_TIMESTAMP(DATE_SUB(CURRENT_TIMESTAMP(), 0), 'yyyy-MM-dd')
    AND T.RETURN_REASON = 24
    AND T.DEPOT_ID IN (4,5,6);
