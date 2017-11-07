
/*
-- 每天创建全链路库存数据表
-- 表名：zybiro.neo_all_chain_stock_daily
-- 脚本类型：hive
 */

-- 先删除之前创建的测试表
--DROP TABLE IF EXISTS zybiro.neo_all_chain_stock_daily;

-- 创建表 ===================================================
CREATE TABLE zybiro.neo_all_chain_stock_daily
(depot_id int
,purchase_onway_num int
,instock_num int
,deliver_onway_num int
,return_onway_num int
,purchase_onway_cost double
,instock_cost double
,deliver_onway_cost double
,return_onway_cost double
,deliver_onway_amount double
,return_onway_amount double
,data_date varchar(10));

-- 插入数据 =================================================
WITH 
-- 1.采购在途商品数量 & 成本金额（人民币）
t100 AS
(SELECT p1.depot_id
        ,SUM(p1.pur_shiped_order_onway_num) AS purchase_onway_num      -- 采购在途数量
        ,SUM(p1.pur_shiped_order_onway_num * p2.in_price) AS purchase_onway_cost    --采购在途成本金额（人民币）
FROM jolly.who_wms_goods_stock_onway_total p1
LEFT JOIN jolly.who_sku_relation p2 ON p1.sku_id = p2.rec_id
WHERE p1.depot_id IN (4, 5, 6, 7)
GROUP BY p1.depot_id
),
-- 2.在库库存（含调拨在途库存，调拨库存计入调出仓库的在库库存中）
-- CN仓在库库存
t201 AS
(SELECT p1.depot_id
        ,SUM(p1.total_stock_num) AS instock_num
        ,SUM(p1.total_stock_num * p2.in_price) AS instock_cost
FROM jolly.who_wms_goods_stock_total_detail p1
LEFT JOIN jolly.who_sku_relation p2 ON p1.sku_id = p2.rec_id
WHERE p1.depot_id in (4, 5, 6)
GROUP BY p1.depot_id
),
-- SA仓在库库存
t202 AS
(SELECT p1.depot_id
        ,SUM(p1.stock_num) AS instock_num
        ,SUM(p1.stock_num * p2.in_price) AS instock_cost
FROM jolly_wms.who_wms_goods_stock_detail p1
LEFT JOIN jolly.who_sku_relation p2 ON p1.sku_id = p2.rec_id
GROUP BY p1.depot_id
),
-- UNION得到在库库存
t203 AS
(SELECT t201.* FROM t201
UNION ALL
SELECT t202.* FROM t202
),
-- 所有仓库调拨在途
t204 AS
(SELECT p1.depot_id
        ,SUM(p1.allocate_order_onway_num) AS allocate_onway_num      -- 采购在途数量
        ,SUM(p1.allocate_order_onway_num * p2.in_price) AS allocate_onway_cost    --采购在途成本金额（人民币）
FROM jolly.who_wms_goods_stock_onway_total p1
LEFT JOIN jolly.who_sku_relation p2 ON p1.sku_id = p2.rec_id
WHERE p1.depot_id in (4, 5, 6, 7)
GROUP BY p1.depot_id
),
-- 在库库存 = 实际在库库存 + 调出库存
t200 AS
(SELECT t203.depot_id
        ,(t203.instock_num + t204.allocate_onway_num) AS instock_num
        ,(t203.instock_cost + t204.allocate_onway_cost) AS instock_cost
FROM t203 
LEFT JOIN t204 ON t203.depot_id = t204.depot_id
),
-- 4.退货在途
-- 订单达到目的国的天数
t401 AS
(SELECT order_id
        ,FROM_UNIXTIME(destination_time, 'yyyy-MM-dd') AS destination_date
        ,DATEDIFF(CURRENT_TIMESTAMP(), FROM_UNIXTIME(destination_time)) AS destination_days
FROM jolly.who_prs_cod_order_shipping_time
),
-- 4.1 拒收和投递失败的退货在途
-- 有已入库的退货商品的订单列表
t4101 AS
(SELECT p1.returned_order_id
FROM jolly.who_wms_returned_order_goods p1
INNER JOIN jolly.who_wms_returned_order_info p2
                 ON p1.returned_rec_id = p2.returned_rec_id
WHERE p1.returned_stock_num > 0
     AND p1.stock_end_time > 0
     AND p1.stock_end_time < UNIX_TIMESTAMP(TO_DATE(CURRENT_TIMESTAMP()), 'yyyy-MM-dd')
     AND p2.returned_order_status = 1
GROUP BY p1.returned_order_id
),
-- 拒收和投递失败的订单
t4102 AS
(SELECT p1.order_id
        ,p1.order_sn
        ,p1.invoice_no
        ,(CASE WHEN p1.cod_check_status = 4 THEN '4投递失败' ELSE '6已拒收' END) AS cod_status
        ,FROM_UNIXTIME(CASE WHEN p1.prepare_pay_time = 0 THEN p1.pay_time ELSE p1.prepare_pay_time END, 'yyyy-MM-dd') AS pay_date
        ,FROM_UNIXTIME(p1.shipping_time, 'yyyy-MM-dd') AS shipping_date
        ,DATEDIFF(CURRENT_TIMESTAMP(), FROM_UNIXTIME(p1.shipping_time, 'yyyy-MM-dd')) AS shiped_days
        ,t401.destination_date
        ,t401.destination_days
        ,(CASE WHEN p1.real_shipping_id = 40 THEN 'Aramex' 
                     WHEN p1.real_shipping_id = 170 THEN 'fetchr'  
                     WHEN P1.real_shipping_id IN (168, 171) THEN 'SMSA' 
                     WHEN P1.real_shipping_id IN (172, 174, 176) THEN 'Naqel'  
                     ELSE 'Others' END) AS shipping_name
        ,p4.region_name AS country_name
        ,(CASE WHEN p4.region_name = 'Saudi Arabia' THEN 7 ELSE 6 END) AS depot_id
        ,'拒收或投递失败' AS return_type
        ,SUM(p2.goods_send_num) AS return_onway_num
        ,SUM(p2.goods_send_num * p2.in_price) AS return_onway_cost
        ,SUM(p2.goods_send_num * p2.goods_price * 6.6775) AS return_onway_amount
FROM jolly.who_order_info p1
LEFT JOIN jolly.who_order_goods p2
            ON p1.order_id = p2.order_id
LEFT JOIN jolly.who_order_user_info p3
            ON p1.order_id = p3.order_id
LEFT JOIN (SELECT region_id, region_name
                    FROM jolly.who_region
                    WHERE region_type = 0
                    AND region_status = 1) p4
            ON p3.country = p4.region_id
LEFT JOIN jolly.who_order_shipping_tracking p5
             ON p1.order_id = p5.order_id AND p5.shipping_state IN (3, 6, 8, 13)    -- 与在途待签收区分，避免重复计算
LEFT JOIN t401 ON p1.order_id = t401.order_id
WHERE p1.shipping_time < UNIX_TIMESTAMP(TO_DATE(CURRENT_TIMESTAMP()), 'yyyy-MM-dd')
    AND p1.pay_id = 41 
    AND p1.is_shiped = 1
    AND p1.cod_check_status in (4, 6)
GROUP BY p1.order_id
        ,p1.order_sn
        ,p1.invoice_no
        ,(CASE WHEN p1.cod_check_status = 4 THEN '4投递失败' ELSE '6已拒收' END)
        ,FROM_UNIXTIME(CASE WHEN p1.prepare_pay_time = 0 THEN p1.pay_time ELSE p1.prepare_pay_time END, 'yyyy-MM-dd') 
        ,FROM_UNIXTIME(p1.shipping_time, 'yyyy-MM-dd')
        ,DATEDIFF(CURRENT_TIMESTAMP(), FROM_UNIXTIME(p1.shipping_time, 'yyyy-MM-dd'))
        ,t401.destination_date
        ,t401.destination_days
        ,(CASE WHEN p1.real_shipping_id = 40 THEN 'Aramex' 
                     WHEN p1.real_shipping_id = 170 THEN 'fetchr'  
                     WHEN P1.real_shipping_id IN (168, 171) THEN 'SMSA' 
                     WHEN P1.real_shipping_id IN (172, 174, 176) THEN 'Naqel'  
                     ELSE 'Others' END)
        ,p4.region_name
        ,(CASE WHEN p4.region_name = 'Saudi Arabia' THEN 7 ELSE 6 END)
        ,'拒收或投递失败'
),
t400 AS
(SELECT depot_id
        ,SUM(return_onway_num) AS return_onway_num
        ,SUM(return_onway_cost) AS return_onway_cost
        ,SUM(return_onway_amount) AS return_onway_amount
FROM t4102
LEFT JOIN t4101 ON t4102.order_id = t4101.returned_order_id
WHERE t4101.returned_order_id IS NULL
GROUP BY depot_id
),
-- 3.发货在途
-- 发货在途待签收订单
t301 AS
(SELECT p1.order_id 
        ,p2.depot_id
        ,p5.region_name AS country_name
        ,(CASE WHEN p2.real_shipping_id = 40 THEN 'Aramex' 
                     WHEN p2.real_shipping_id = 170 THEN 'fetchr'  
                     WHEN p2.real_shipping_id IN (168, 171) THEN 'SMSA' 
                     WHEN p2.real_shipping_id IN (172, 174, 176) THEN 'Naqel'  
                     ELSE 'Others' END) AS shipping_name
        ,DATEDIFF(CURRENT_TIMESTAMP(), FROM_UNIXTIME(p2.shipping_time, 'yyyy-MM-dd')) AS shiped_days
        ,t401.destination_date
        ,t401.destination_days
FROM jolly.who_order_shipping_tracking p1
RIGHT JOIN jolly.who_order_info p2 
             ON p1.order_id = p2.order_id AND P2.depot_id IN (4, 5, 6, 7) AND p2.order_status = 1
LEFT JOIN t401 
             ON t401.order_id = p1.order_id
LEFT JOIN jolly.who_order_user_info p4 
             ON p1.order_id = p4.order_id
LEFT JOIN jolly.who_region p5 
             ON p4.country = p5.region_id AND p5.region_type = 0 AND p5.region_status = 1 
WHERE p1.shipping_state NOT IN (3, 6, 8, 13)    -- 不是已签收、已退回、已拒收和已丢失中的任何一项，就是还在途的
GROUP BY p1.order_id 
        ,p2.depot_id
        ,p5.region_name
        ,(CASE WHEN p2.real_shipping_id = 40 THEN 'Aramex' 
                     WHEN p2.real_shipping_id = 170 THEN 'fetchr'  
                     WHEN p2.real_shipping_id IN (168, 171) THEN 'SMSA' 
                     WHEN p2.real_shipping_id IN (172, 174, 176) THEN 'Naqel'  
                     ELSE 'Others' END)
        ,DATEDIFF(CURRENT_TIMESTAMP(), FROM_UNIXTIME(p2.shipping_time, 'yyyy-MM-dd'))
        ,t401.destination_date
        ,t401.destination_days
),
-- 汇总订单数量，商品数量，成本金额，销售金额
t302 AS
(SELECT t301.depot_id
        ,t301.country_name
        ,t301.shipping_name
        ,t301.shiped_days
        ,t301.destination_days
        ,COUNT(DISTINCT t301.order_id) AS order_num
        ,SUM(p3.goods_send_num) AS deliver_onway_num
        ,SUM(p3.goods_send_num * p3.in_price) AS deliver_onway_cost     -- 成本金额，人民币
        ,SUM(p3.goods_send_num * p3.goods_price * 6.6775) AS deliver_onway_amount    -- 销售金额，* 6.6775，转成人民币
FROM t301
LEFT JOIN jolly.who_order_goods p3 
             ON t301.order_id = p3.order_id
WHERE t301.country_name IS NOT NULL
GROUP BY t301.depot_id
        ,t301.country_name
        ,t301.shipping_name
        ,t301.shiped_days
        ,t301.destination_days
),
-- 分仓库统计
t300 AS
(SELECT t302.depot_id
        ,SUM(t302.deliver_onway_num) AS deliver_onway_num
        ,SUM(t302.deliver_onway_cost) AS deliver_onway_cost
        ,SUM(t302.deliver_onway_amount) AS deliver_onway_amount
FROM t302
GROUP BY t302.depot_id
)

INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
-- 汇总！汇总！汇总！
-- 全链路库存结构汇总表
SELECT t200.depot_id
        ,t100.purchase_onway_num
        ,t200.instock_num
        ,t300.deliver_onway_num
        ,t400.return_onway_num
        ,t100.purchase_onway_cost
        ,t200.instock_cost
        ,t300.deliver_onway_cost
        ,t400.return_onway_cost
        ,t300.deliver_onway_amount
        ,t400.return_onway_amount
        ,current_date() AS data_date
FROM t200
LEFT JOIN t100 ON t200.depot_id = t100.depot_id
LEFT JOIN t300 ON t200.depot_id = t300.depot_id
LEFT JOIN t400 ON t200.depot_id = t400.depot_id
ORDER BY t200.depot_id;

-- 查询数据 ==================================================
SELECT * 
FROM zybiro.neo_all_chain_stock_daily
LIMIT 10;




