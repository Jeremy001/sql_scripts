/*
说明：仓库--出库各环节积压（推送）：拣货积压、打包积压、发货积压
作者：王政鸣
更新日期：2017-9-14
脚本类型：Hive and Impala
日期参数：data_date，昨天的日期，格式为yyyyMMdd
 */

-- 1.用于hue ===================================================

WITH 
-- 判断订单是否出库各环节积压
t AS
(SELECT order_id
        ,order_sn
        ,depot_id
        ,depot_name
        ,is_problems_order
        ,is_shiped
        ,pay_time
        ,no_problems_order_uptime
        ,outing_stock_time
        ,picking_time
        ,picking_finish_time
        ,order_pack_time
        ,shipping_time
        ,(CASE WHEN ((is_shiped IN (7, 8) AND picking_finish_time IS NULL)   
                                   OR 
                                   picking_finish_time > concat(to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))),' 23:59:59')
                                   )
                      THEN 'picking_backlog'    -- 拣货积压
                        -- 拣货积压条件：
                        -- 1.未拣货：7/8代表未完成拣货， 没有拣货完成时间
                        -- 2.已拣货，但是拣货时间晚于当天24:00
                        -- 以上两个条件的任意一个，即为拣货积压
                     WHEN (is_shiped NOT IN (7, 8) AND picking_finish_time <= concat(to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))),' 23:59:59'))
                         AND (order_pack_time IS NULL OR order_pack_time > concat(to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))),' 23:59:59'))
                       THEN 'packing_backlog'    -- 打包积压
                        -- 打包积压：
                        -- 1.前提条件：已经在当天24点前完成拣货
                        -- 打包积压条件：
                        -- 1.未打包，没有打包时间
                        -- 2.已打包，但是打包时间晚于当天24:00
                      WHEN order_pack_time IS NOT NULL 
                         AND order_pack_time <= concat(to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))),' 23:59:59')
                        THEN 'shipping_backlog'
                        -- 发运积压：
                        -- 1.前提条件：已经在当天24点前完成打包
                        -- 发运积压条件：
                        -- 1.未发运
                        -- 2.已发运，但是发运时间晚于当天24:00
                        ELSE NULL
                        END) AS backlog_type
FROM zydb.dw_order_node_time
WHERE is_check = 1    -- 已审核
     AND order_status = 1       -- 已确认
     AND pay_status IN (1, 3)       -- 已支付或支付中
     AND is_shiped IN (1, 3, 6, 7, 8)
     AND is_problems_order != 1    -- 非问题单 0默认值 1问题单 2非问题单
     -- 前提条件1：当天18:00前可拣货
     AND outing_stock_time <= concat(to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))),' 18:00:00') 
     -- 前提条件2：未发运或者发运时间晚于当天24:00
     AND (is_shiped != 1 OR shipping_time > concat(to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))),' 23:59:59'))
)

-- 查询积压订单列表
SELECT * 
FROM t
WHERE backlog_type IS NOT NULL
     AND depot_id IN (4, 5, 6);


-- 2.用于客户端 ==================================================

WITH 
-- 判断订单是否出库各环节积压
t AS
(SELECT order_id
        ,order_sn
        ,depot_id
        ,depot_name
        ,is_problems_order
        ,is_shiped
        ,pay_time
        ,no_problems_order_uptime
        ,outing_stock_time
        ,picking_time
        ,picking_finish_time
        ,order_pack_time
        ,shipping_time
        ,(CASE WHEN ((is_shiped IN (7, 8) AND picking_finish_time IS NULL)   
                                   OR 
                                   picking_finish_time > concat(to_date(from_unixtime(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 23:59:59')
                                   )
                      THEN 'picking_backlog'    -- 拣货积压
                        -- 拣货积压条件：
                        -- 1.未拣货：7/8代表未完成拣货， 没有拣货完成时间
                        -- 2.已拣货，但是拣货时间晚于当天24:00
                        -- 以上两个条件的任意一个，即为拣货积压
                     WHEN (is_shiped NOT IN (7, 8) AND picking_finish_time <= concat(to_date(from_unixtime(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 23:59:59'))
                         AND (order_pack_time IS NULL OR order_pack_time > concat(to_date(from_unixtime(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 23:59:59'))
                       THEN 'packing_backlog'    -- 打包积压
                        -- 打包积压：
                        -- 1.前提条件：已经在当天24点前完成拣货
                        -- 打包积压条件：
                        -- 1.未打包，没有打包时间
                        -- 2.已打包，但是打包时间晚于当天24:00
                      WHEN order_pack_time IS NOT NULL 
                         AND order_pack_time <= concat(to_date(from_unixtime(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 23:59:59')
                        THEN 'shipping_backlog'
                        -- 发运积压：
                        -- 1.前提条件：已经在当天24点前完成打包
                        -- 发运积压条件：
                        -- 1.未发运
                        -- 2.已发运，但是发运时间晚于当天24:00
                        ELSE NULL
                        END) AS backlog_type
FROM zydb.dw_order_node_time
WHERE is_check = 1    -- 已审核
     AND order_status = 1       -- 已确认
     AND pay_status IN (1, 3)       -- 已支付或支付中
     AND is_shiped IN (1, 3, 6, 7, 8)
     AND is_problems_order != 1    -- 非问题单 0默认值 1问题单 2非问题单
     -- 前提条件1：当天18:00前可拣货
     AND outing_stock_time <= concat(to_date(from_unixtime(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 18:00:00') 
     -- 前提条件2：未发运或者发运时间晚于当天24:00
     AND (is_shiped != 1 OR shipping_time > concat(to_date(from_unixtime(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 23:59:59'))
)

-- 查询积压订单列表
SELECT * 
FROM t
WHERE backlog_type IS NOT NULL
     AND depot_id IN (4, 5, 6);


# 3. 用于推送 ======================================================

-- 作者：Neo(王政鸣)
-- 更新时间：2017-9-14
-- SQL脚本类型：hive

-- 0.选定zybiro库，zybiro账号只对zybiro库有DML和DDL操作权限
USE zybiro;

-- 1.DROP TABLE
DROP TABLE IF EXISTS neo_out_of_warehouse_backlog;

-- 2.CREATE TABLE
CREATE TABLE neo_out_of_warehouse_backlog
AS 
WITH 
-- 判断订单是否出库各环节积压
t AS
(SELECT order_id
        ,order_sn
        ,depot_id
        ,depot_name
        ,is_problems_order
        ,is_shiped
        ,pay_time
        ,no_problems_order_uptime
        ,outing_stock_time
        ,picking_time
        ,picking_finish_time
        ,order_pack_time
        ,shipping_time
        ,(CASE WHEN ((is_shiped IN (7, 8) AND picking_finish_time IS NULL)   
                                   OR 
                                   picking_finish_time > CONCAT_WS(' ',DATE_SUB(CURRENT_TIMESTAMP(), 0), '23:59:59')
                                   )
                      THEN 'picking_backlog'    -- 拣货积压
                        -- 拣货积压条件：
                        -- 1.未拣货：7/8代表未完成拣货， 没有拣货完成时间
                        -- 2.已拣货，但是拣货时间晚于当天24:00
                        -- 以上两个条件的任意一个，即为拣货积压
                     WHEN (is_shiped NOT IN (7, 8) AND picking_finish_time <= CONCAT_WS(' ',DATE_SUB(CURRENT_TIMESTAMP(), 0), '23:59:59'))
                         AND (order_pack_time IS NULL OR order_pack_time > CONCAT_WS(' ',DATE_SUB(CURRENT_TIMESTAMP(), 0), '23:59:59'))
                       THEN 'packing_backlog'    -- 打包积压
                        -- 打包积压：
                        -- 1.前提条件：已经在当天24点前完成拣货
                        -- 打包积压条件：
                        -- 1.未打包，没有打包时间
                        -- 2.已打包，但是打包时间晚于当天24:00
                      WHEN order_pack_time IS NOT NULL 
                         AND order_pack_time <= CONCAT_WS(' ',DATE_SUB(CURRENT_TIMESTAMP(), 0), '23:59:59')
                        THEN 'shipping_backlog'
                        -- 发运积压：
                        -- 1.前提条件：已经在当天24点前完成打包
                        -- 发运积压条件：
                        -- 1.未发运
                        -- 2.已发运，但是发运时间晚于当天24:00
                        ELSE NULL
                        END) AS backlog_type
FROM zydb.dw_order_node_time
WHERE is_check = 1    -- 已审核
     AND order_status = 1       -- 已确认
     AND pay_status IN (1, 3)       -- 已支付或支付中
     AND is_shiped IN (1, 3, 6, 7, 8)
     AND is_problems_order != 1    -- 非问题单 0默认值 1问题单 2非问题单
     -- 前提条件1：当天18:00前可拣货
     AND outing_stock_time <= CONCAT_WS(' ',DATE_SUB(CURRENT_TIMESTAMP(), 1), '18:00:00')
     -- 前提条件2：未发运或者发运时间晚于当天24:00
     AND (is_shiped != 1 OR shipping_time > CONCAT_WS(' ',DATE_SUB(CURRENT_TIMESTAMP(), 0), '23:59:59'))
)
-- 查询积压订单列表
SELECT * 
FROM t
WHERE backlog_type IS NOT NULL
     AND depot_id IN (4, 5, 6);

















