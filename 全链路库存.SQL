/*
说明：全链路库存结构
作者：王政鸣
更新时间：2017-11-1
sql脚本类型：impala
*/

-- 业务数据说明：
/*
- 全链路库存结构分为：
- 1.采购在途
- 2.在仓（含调拨在途）
- 3.仓库发出在途（已发出，未签收）
- 4.客户拒收+退货在途（已退回，尚未进入仓库库存）
*/

-- 1.历史数据一周环比
-- zybiro.neo_all_chain_stock_daily
WITH t1 AS
(SELECT SUBSTR(data_date, 1, 7) AS month
        ,data_date
        ,SUM(purchase_onway_num) AS purchase_onway_num
        ,SUM(instock_num) AS instock_num
        ,SUM(deliver_onway_num) AS deliver_onway_num
        ,SUM(return_onway_num) AS return_onway_num
        ,SUM(purchase_onway_cost) AS purchase_onway_cost
        ,SUM(instock_cost) AS instock_cost
        ,SUM(deliver_onway_cost) AS deliver_onway_cost
        ,SUM(return_onway_cost) AS return_onway_cost
        ,SUM(deliver_onway_amount) AS deliver_onway_amount
        ,SUM(return_onway_amount) AS return_onway_amount
FROM zybiro.neo_all_chain_stock_daily
GROUP BY SUBSTR(data_date, 1, 7)
        ,data_date
)
SELECT month
        ,AVG(purchase_onway_num) AS purchase_onway_num
        ,AVG(instock_num) AS instock_num
        ,AVG(deliver_onway_num) AS deliver_onway_num
        ,AVG(return_onway_num) AS return_onway_num
        ,AVG(purchase_onway_cost) AS purchase_onway_cost
        ,AVG(instock_cost) AS instock_cost
        ,AVG(deliver_onway_cost) AS deliver_onway_cost
        ,AVG(return_onway_cost) AS return_onway_cost
        ,AVG(deliver_onway_amount) AS deliver_onway_amount
        ,AVG(return_onway_amount) AS return_onway_amount
FROM t1
GROUP BY month
ORDER BY month;

/* ================================= 1.采购在途（顶） =====================================
-- 1.采购在途商品数量 & 成本金额（人民币）
-- jolly.who_wms_goods_stock_onway_total（包含所有仓库的在途数据）
-- jolly.who_sku_relation
*/
WITH
t100 AS
(SELECT p1.depot_id
        ,SUM(p1.pur_shiped_order_onway_num) AS purchase_onway_num      -- 采购在途数量
        ,SUM(p1.pur_shiped_order_onway_num * p2.in_price) AS purchase_onway_cost    --采购在途成本金额（人民币）
FROM jolly_oms.who_wms_goods_stock_onway_total p1
LEFT JOIN jolly.who_sku_relation p2 ON p1.sku_id = p2.rec_id
WHERE p1.depot_id IN (4, 5, 6, 7, 8, 14, 15)
GROUP BY p1.depot_id
),


/* ================================= 2.在库库存（顶） =====================================
-- 2.在库库存（含调拨在途库存，调拨库存计入调出仓库的在库库存中）
-- CN仓 jolly.who_wms_goods_stock_total_detail
-- jolly.who_sku_relation
-- SA仓 jolly_wms.who_wms_goods_stock_total_detail
-- jolly_wms.who_sku_relation
*/
-- CN仓在库库存
t201 AS
(SELECT p1.depot_id
        ,SUM(p1.total_stock_num) AS instock_num
        ,SUM(p1.total_stock_num * p2.in_price) AS instock_cost
FROM jolly.who_wms_goods_stock_total_detail p1
LEFT JOIN jolly.who_sku_relation p2 ON p1.sku_id = p2.rec_id
WHERE p1.depot_id in (4, 5, 6, 14)
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
FROM jolly_oms.who_wms_goods_stock_onway_total p1
LEFT JOIN jolly.who_sku_relation p2 ON p1.sku_id = p2.rec_id
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


/* ====================================== 4.退货在途（顶） =================================
-- 4.退货在途
-- 抵达目的国时间 jolly.who_prs_cod_order_shipping_time
-- 退货单信息表 jolly.who_wms_returned_order_info
-- 退货单商品明细表 jolly.who_wms_returned_order_goods
*/

-- 订单达到目的国的天数
t401 AS
(SELECT order_id
        ,FROM_UNIXTIME(destination_time, 'yyyy-MM-dd') AS destination_date
        ,DATEDIFF(NOW(), FROM_UNIXTIME(destination_time)) AS destination_days
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
        ,p1.order_status
        ,p1.invoice_no
        ,p1.depot_id AS ship_depot_id
        ,(CASE WHEN p1.cod_check_status = 4 THEN '4投递失败' ELSE '6已拒收' END) AS cod_status
        ,FROM_UNIXTIME(CASE WHEN p1.prepare_pay_time = 0 THEN p1.pay_time ELSE p1.prepare_pay_time END, 'yyyy-MM-dd') AS pay_date
        ,FROM_UNIXTIME(p1.shipping_time, 'yyyy-MM-dd') AS shipping_date
        ,DATEDIFF(CURRENT_TIMESTAMP(), FROM_UNIXTIME(p1.shipping_time, 'yyyy-MM-dd')) AS shiped_days
        ,t401.destination_date
        ,t401.destination_days
        ,(CASE WHEN p1.real_shipping_id IN (40, 200) THEN 'Aramex'
               WHEN p1.real_shipping_id IN (170, 201, 257) THEN 'fetchr'
               WHEN P1.real_shipping_id IN (168, 171) THEN 'SMSA'
               WHEN P1.real_shipping_id IN (172, 174, 176) THEN 'Naqel'
               ELSE 'Others'
          END) AS shipping_name
        ,p4.region_name AS country_name
        ,p5.shipping_state
        ,(CASE WHEN p4.region_name = 'Saudi Arabia' THEN 7 ELSE 6 END) AS return_depot_id
        --,'拒收或投递失败' AS return_type
        ,SUM(NVL(p2.goods_send_num, 0)) AS return_onway_num
        ,SUM(NVL(p2.goods_send_num, 0) * NVL(p2.in_price, 0)) AS return_onway_cost
        ,SUM(NVL(p2.goods_send_num, 0) * NVL(p2.goods_price, 0) * 6.6775) AS return_onway_amount
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
       ON p1.order_id = p5.order_id
LEFT JOIN t401 ON p1.order_id = t401.order_id
WHERE p1.shipping_time < UNIX_TIMESTAMP(TO_DATE(CURRENT_TIMESTAMP()), 'yyyy-MM-dd')
  AND p1.pay_id = 41
  AND p1.is_shiped = 1
  AND p1.cod_check_status IN (4, 6)
  AND p5.shipping_state IN (3, 6, 8, 13)    -- 与在途待签收区分，避免重复计算
GROUP BY p1.order_id
        ,p1.order_sn
        ,p1.order_status
        ,p1.invoice_no
        ,p1.depot_id
        ,(CASE WHEN p1.cod_check_status = 4 THEN '4投递失败' ELSE '6已拒收' END)
        ,FROM_UNIXTIME(CASE WHEN p1.prepare_pay_time = 0 THEN p1.pay_time ELSE p1.prepare_pay_time END, 'yyyy-MM-dd')
        ,FROM_UNIXTIME(p1.shipping_time, 'yyyy-MM-dd')
        ,DATEDIFF(CURRENT_TIMESTAMP(), FROM_UNIXTIME(p1.shipping_time, 'yyyy-MM-dd'))
        ,t401.destination_date
        ,t401.destination_days
        ,(CASE WHEN p1.real_shipping_id IN (40, 200) THEN 'Aramex'
               WHEN p1.real_shipping_id IN (170, 201, 257) THEN 'fetchr'
               WHEN P1.real_shipping_id IN (168, 171) THEN 'SMSA'
               WHEN P1.real_shipping_id IN (172, 174, 176) THEN 'Naqel'
               ELSE 'Others'
          END)
        ,p4.region_name
        ,p5.shipping_state
        ,(CASE WHEN p4.region_name = 'Saudi Arabia' THEN 7 ELSE 6 END)
),
t400 AS
(SELECT depot_id
        ,SUM(return_onway_num) AS return_onway_num
        ,SUM(return_onway_cost) AS return_onway_cost
        ,SUM(return_onway_amount) AS return_onway_amount
FROM t4102
WHERE order_id NOT IN (SELECT * FROM t4101)
GROUP BY depot_id
),





-- 毛毛需求
-- 1.待退回的明细数据，先处理2015-2016年发货的订单
-- 销售退货签收表, 查询各个退货退款订单的状态和签收时间
-- join jolly and jolly_wms
t501 AS
(SELECT p1.*
        ,(CASE p1.status WHEN 1 THEN '1已签收'
                         WHEN 2 THEN '2已匹配'
                         WHEN 3 THEN '3部分收货'
                         WHEN 4 THEN '4全部收货'
                         WHEN 5 THEN '5客服处理中'
                         WHEN 6 THEN '6签收失败'
                         ELSE '其他'
          END) AS sale_return_status
        ,FROM_UNIXTIME(p1.gmt_created) AS return_receipt_time
FROM jolly.who_wms_sale_returned_receipt_info AS p1
WHERE p1.order_id >= 1
UNION ALL
SELECT p1.*
        ,(CASE p1.status WHEN 1 THEN '1已签收'
                         WHEN 2 THEN '2已匹配'
                         WHEN 3 THEN '3部分收货'
                         WHEN 4 THEN '4全部收货'
                         WHEN 5 THEN '5客服处理中'
                         WHEN 6 THEN '6签收失败'
                         ELSE '其他'
          END) AS sale_return_status
        ,FROM_UNIXTIME(p1.gmt_created) AS return_receipt_time
FROM jolly_wms.who_wms_sale_returned_receipt_info AS p1
WHERE p1.order_id >= 1
),
t502 AS
(SELECT t501.order_id
        ,t501.order_sn
        ,MAX(CONCAT_WS('--', p2.user_name, t501.sale_return_status, t501.return_receipt_time)) AS sale_return_info1
        ,MIN(CONCAT_WS('--', p2.user_name, t501.sale_return_status, t501.return_receipt_time)) AS sale_return_info2
FROM t501
LEFT JOIN jolly.who_rbac_user AS p2
       ON t501.admin_id = p2.user_id
GROUP BY t501.order_id
        ,t501.order_sn
)

-- 结果
SELECT t4102.*
        ,t502.sale_return_info1
        ,t502.sale_return_info2
FROM t4102
LEFT JOIN t502
       ON t4102.order_id = t502.order_id
WHERE t4102.shipping_date >= '2015-01-01'
  AND t4102.shipping_date < '2017-01-01'
  AND t4102.order_id NOT IN (SELECT * FROM t4101)
LIMIT 20
;

-- ====================================== 退货在途拆解 ==================================
/*
退货在途拆解（承运商、目的国、年份【含季度】）
 */
-- 承运商
SELECT shipping_name
        ,COUNT(DISTINCT order_id) AS order_num
        ,SUM(return_onway_num) AS return_onway_num
        ,SUM(return_onway_cost) AS return_onway_cost
        ,SUM(return_onway_amount) AS return_onway_amount
FROM t4102
WHERE order_id NOT IN (SELECT * FROM t4101)
GROUP BY shipping_name
ORDER BY return_onway_num DESC;
-- 目的国
SELECT country_name
        ,COUNT(DISTINCT order_id) AS order_num
        ,SUM(return_onway_num) AS return_onway_num
        ,SUM(return_onway_cost) AS return_onway_cost
        ,SUM(return_onway_amount) AS return_onway_amount
FROM t4102
WHERE order_id NOT IN (SELECT * FROM t4101)
GROUP BY country_name
ORDER BY return_onway_num DESC;
-- shiping_date(年份和季度后续处理)
SELECT shipping_date
        ,COUNT(DISTINCT order_id) AS order_num
        ,SUM(return_onway_num) AS return_onway_num
        ,SUM(return_onway_cost) AS return_onway_cost
        ,SUM(return_onway_amount) AS return_onway_amount
FROM t4102
WHERE order_id NOT IN (SELECT * FROM t4101)
GROUP BY shipping_date
ORDER BY shipping_date;


-- ======================================4.2 超时默认拒收  ====================================
-- 4.2 已发货，到达目的国超过设置天数仍未签收的，可默认是退回状态
-- cod订单到达目的国后多长时间内默认拒收？
    -- real_shipping_id in (40, 168) then 21天
    -- real_shipping_id in (170, 172, 174, 176) then 14天

-- 40Aramex / 168or171SMSA / Others: 到达目的国超过21天且仍未签收的订单
t4201 AS
(SELECT p1.order_id
        ,p1.order_sn
        ,p1.invoice_no
        ,'1未签收' AS cod_status
        ,FROM_UNIXTIME(CASE WHEN p1.prepare_pay_time = 0 THEN p1.pay_time ELSE p1.prepare_pay_time END, 'yyyy-MM-dd') AS pay_date
        ,FROM_UNIXTIME(p1.shipping_time, 'yyyy-MM-dd') AS shipping_date
        ,DATEDIFF(NOW(), FROM_UNIXTIME(p1.shipping_time, 'yyyy-MM-dd')) AS shiped_days
        ,t401.destination_date
        ,t401.destination_days
        ,(CASE WHEN p1.real_shipping_id = 40 THEN 'Aramex'
                     WHEN p1.real_shipping_id = 170 THEN 'fetchr'
                     WHEN P1.real_shipping_id IN (168, 171) THEN 'SMSA'
                     WHEN P1.real_shipping_id IN (172, 174, 176) THEN 'Naqel'
                     ELSE 'Others' END) AS shipping_name
        ,p4.region_name AS country_name
        ,(CASE WHEN p4.region_name = 'Saudi Arabia' THEN 7 ELSE 6 END) AS depot_id
        ,'超时未签收' AS return_type
        ,SUM(p3.goods_send_num) AS return_onway_num
        ,SUM(p3.goods_send_num * p3.in_price) AS return_onway_cost
        ,SUM(p3.goods_send_num * p3.goods_price * 6.6775) AS return_onway_amount
FROM jolly.who_order_info p1
LEFT JOIN jolly.who_order_shipping_tracking p0
             ON p0.order_id = p1.order_id AND (p0.shipping_state = 0 OR p0.shipping_state = 5)
INNER JOIN t401
                 ON p1.order_id = t401.order_id AND t401.destination_days >=21
INNER JOIN jolly.who_order_goods p3
                ON p1.order_id = p3.order_id
LEFT JOIN jolly.who_order_user_info p2
            ON p1.order_id = p2.order_id
LEFT JOIN (SELECT region_id, region_name
                    FROM jolly.who_region
                    WHERE region_type = 0
                    AND region_status = 1) p4
            ON p2.country = p4.region_id
WHERE P1.pay_id = 41
AND p1.depot_id IN (4, 5, 6, 7, 8, 14, 15)
AND P1.cod_check_status NOT IN (3, 4, 5, 6)
AND p1.real_shipping_id NOT IN (170, 172, 174, 176)
GROUP BY p1.order_id
        ,p1.order_sn
        ,p1.invoice_no
        ,'1未签收'
        ,FROM_UNIXTIME(CASE WHEN p1.prepare_pay_time = 0 THEN p1.pay_time ELSE p1.prepare_pay_time END, 'yyyy-MM-dd')
        ,FROM_UNIXTIME(p1.shipping_time, 'yyyy-MM-dd')
        ,DATEDIFF(NOW(), FROM_UNIXTIME(p1.shipping_time, 'yyyy-MM-dd'))
        ,t401.destination_date
        ,t401.destination_days
        ,(CASE WHEN p1.real_shipping_id = 40 THEN 'Aramex'
                     WHEN p1.real_shipping_id = 170 THEN 'fetchr'
                     WHEN P1.real_shipping_id IN (168, 171) THEN 'SMSA'
                     WHEN P1.real_shipping_id IN (172, 174, 176) THEN 'Naqel'
                     ELSE 'Others' END)
        ,p4.region_name
        ,(CASE WHEN p4.region_name = 'Saudi Arabia' THEN 7 ELSE 6 END)
        ,'超时未签收'
),
-- 170, 172, 174, 176:到达目的国超过14天且仍未签收的订单
t4202 AS
(SELECT p1.order_id
        ,p1.order_sn
        ,p1.invoice_no
        ,'1未签收' AS cod_status
        ,FROM_UNIXTIME(CASE WHEN p1.prepare_pay_time = 0 THEN p1.pay_time ELSE p1.prepare_pay_time END, 'yyyy-MM-dd') AS pay_date
        ,FROM_UNIXTIME(p1.shipping_time, 'yyyy-MM-dd') AS shipping_date
        ,DATEDIFF(NOW(), FROM_UNIXTIME(p1.shipping_time, 'yyyy-MM-dd')) AS shiped_days
        ,t401.destination_date
        ,t401.destination_days
        ,(CASE WHEN p1.real_shipping_id = 40 THEN 'Aramex'
                     WHEN p1.real_shipping_id = 170 THEN 'fetchr'
                     WHEN P1.real_shipping_id IN (168, 171) THEN 'SMSA'
                     WHEN P1.real_shipping_id IN (172, 174, 176) THEN 'Naqel'
                     ELSE 'Others' END) AS shipping_name
        ,p4.region_name AS country_name
        ,(CASE WHEN p4.region_name = 'Saudi Arabia' THEN 7 ELSE 6 END) AS depot_id
        ,'超时未签收' AS return_type
        ,SUM(p3.goods_send_num) AS return_onway_num
        ,SUM(p3.goods_send_num * p3.in_price) AS return_onway_cost
        ,SUM(p3.goods_send_num * p3.goods_price * 6.6775) AS return_onway_amount
FROM jolly.who_order_info p1
LEFT JOIN jolly.who_order_shipping_tracking p0
             ON p0.order_id = p1.order_id AND (p0.shipping_state = 0 OR p0.shipping_state = 5)
INNER JOIN t401 ON p1.order_id = t401.order_id AND t401.destination_days >=14
INNER JOIN jolly.who_order_goods p3
                ON p1.order_id = p3.order_id
LEFT JOIN jolly.who_order_user_info p2
            ON p1.order_id = p2.order_id
LEFT JOIN (SELECT region_id, region_name
                    FROM jolly.who_region
                    WHERE region_type = 0
                    AND region_status = 1) p4
            ON p2.country = p4.region_id
WHERE P1.pay_id = 41
AND p1.depot_id IN (4, 5, 6, 7, 8, 14, 15)
AND P1.cod_check_status NOT IN (3, 4, 5, 6)
AND p1.real_shipping_id IN (170, 172, 174, 176)
GROUP BY p1.order_id
        ,p1.order_sn
        ,p1.invoice_no
        ,'1未签收'
        ,FROM_UNIXTIME(CASE WHEN p1.prepare_pay_time = 0 THEN p1.pay_time ELSE p1.prepare_pay_time END, 'yyyy-MM-dd')
        ,FROM_UNIXTIME(p1.shipping_time, 'yyyy-MM-dd')
        ,DATEDIFF(NOW(), FROM_UNIXTIME(p1.shipping_time, 'yyyy-MM-dd'))
        ,t401.destination_date
        ,t401.destination_days
        ,(CASE WHEN p1.real_shipping_id = 40 THEN 'Aramex'
                     WHEN p1.real_shipping_id = 170 THEN 'fetchr'
                     WHEN P1.real_shipping_id IN (168, 171) THEN 'SMSA'
                     WHEN P1.real_shipping_id IN (172, 174, 176) THEN 'Naqel'
                     ELSE 'Others' END)
        ,p4.region_name
        ,(CASE WHEN p4.region_name = 'Saudi Arabia' THEN 7 ELSE 6 END)
        ,'超时未签收'
),
-- UNION得到合并结果
t4203 AS
(SELECT t4201.* FROM t4201
UNION ALL
SELECT t4202.* FROM t4202
UNION ALL
SELECT t4102.* FROM t4102
),
-- 最终结果
t4204 AS
(SELECT depot_id
        ,SUM(num) AS ret_num
        ,SUM(cost) AS ret_cost
        ,SUM(sales) AS ret_sales
FROM t4203
GROUP BY depot_id
),

-- 合并两种退货在途
t405 AS
(SELECT * FROM t4103
UNION ALL
SELECT * FROM t4204
),

-- =========================================================================
/*
各承运商两种待退货的分布
 */
SELECT shipping_name
        ,return_type
        ,COUNT(DISTINCT order_id) AS order_num
        ,SUM(num) AS need_return_num
        ,SUM(cost) AS need_return_cost
        ,SUM(sales) AS need_return_amount
FROM t4203
GROUP BY shipping_name
        ,return_type
ORDER BY shipping_name
        ,return_type;






-- =========================================================================


-- 得到两种退货在途的合计值
/*t406 AS
(SELECT depot_id
        ,SUM(ret_num) AS ret_num
        ,SUM(ret_cost) AS ret_cost
        ,SUM(ret_sales)  AS ret_sales
FROM t405
GROUP BY depot_id
),
*/
/*-- 订单明细
t4 AS
(SELECT order_id
        ,order_sn
        ,region_name
        ,shipping_name
        ,invoice_no
        ,cod_check_status
        ,pay_date
        ,shipping_date
        ,destination_date
        ,destination_days
        ,num
        ,cost
        ,sales
FROM t3
)
*/

/*
-- 全链路库存订单排查：
-- 以上两个类型的订单union
t000 AS
(SELECT * FROM t404
UNION ALL
SELECT * FROM t4
)

SELECT order_id AS 订单ID
        ,order_sn AS 订单编号
        ,region_name AS 目的国
        ,shipping_name AS 目的国承运商
        ,invoice_no AS 运单号
        ,cod_check_status AS 订单状态
        ,pay_date AS 支付日期
        ,shipping_date AS 发货日期
        ,destination_date AS 抵达目的国日期
        ,destination_days AS 已抵达目的国天数
        ,num AS 商品数量
        ,cost AS 成本金额
        ,sales AS 销售金额
FROM t000;*/


/* =================================== 3.发货在途 ===================================
-- 3.发货在途
-- jolly.who_order_shipping_tracking
-- jolly.who_order_goods
*/
-- 发货在途待签收订单
t301 AS
(SELECT p1.order_id
        ,p2.depot_id
        ,p5.region_name AS country_name
        ,(CASE WHEN p2.real_shipping_id IN (40, 200) THEN 'Aramex'
               WHEN p2.real_shipping_id IN (170, 201, 257) THEN 'fetchr'
               WHEN p2.real_shipping_id IN (168, 171) THEN 'SMSA'
               WHEN p2.real_shipping_id IN (172, 174, 176) THEN 'Naqel'
               ELSE 'Others'
          END) AS shipping_name
        ,DATEDIFF(NOW(), FROM_UNIXTIME(p2.shipping_time, 'yyyy-MM-dd')) AS shiped_days
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
        ,(CASE WHEN p2.real_shipping_id IN (40, 200) THEN 'Aramex'
               WHEN p2.real_shipping_id IN (170, 201, 257) THEN 'fetchr'
               WHEN p2.real_shipping_id IN (168, 171) THEN 'SMSA'
               WHEN p2.real_shipping_id IN (172, 174, 176) THEN 'Naqel'
               ELSE 'Others'
          END)
        ,DATEDIFF(NOW(), FROM_UNIXTIME(p2.shipping_time, 'yyyy-MM-dd'))
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


-- 汇总！汇总！汇总！ ============================================================================
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
FROM t200
LEFT JOIN t100 ON t200.depot_id = t100.depot_id
LEFT JOIN t300 ON t200.depot_id = t300.depot_id
LEFT JOIN t400 ON t200.depot_id = t400.depot_id
ORDER BY t200.depot_id;


-- ================================================================
-- 发货在途：加入国家、承运商、发货天数等维度
-- 利用发货在途的t300和t301两个表，在t301中取数进行汇总即可
-- 1. 国家
SELECT country_name
        ,SUM(order_num) AS order_num
        ,SUM(deliver_onway_num) AS deliver_onway_num
        ,SUM(deliver_onway_cost) AS deliver_onway_cost
        ,SUM(deliver_onway_amount) AS deliver_onway_amount
FROM t301
GROUP BY country_name
ORDER BY order_num;
-- 2.承运商
SELECT shipping_name
        ,SUM(order_num) AS order_num
        ,SUM(deliver_onway_num) AS deliver_onway_num
        ,SUM(deliver_onway_cost) AS deliver_onway_cost
        ,SUM(deliver_onway_amount) AS deliver_onway_amount
FROM t301
GROUP BY shipping_name
ORDER BY order_num;
-- 3.发货天数
SELECT (CASE WHEN shiped_days >= 90 THEN '90天以上'
                      WHEN (shiped_days >= 80 AND shiped_days < 90) THEN '80-90天'
                      WHEN (shiped_days >= 70 AND shiped_days < 80) THEN '70-80天'
                      WHEN (shiped_days >= 60 AND shiped_days < 70) THEN '60-70天'
                      WHEN (shiped_days >= 50 AND shiped_days < 60) THEN '50-60天'
                      WHEN (shiped_days >= 40 AND shiped_days < 50) THEN '40-50天'
                      WHEN (shiped_days >= 30 AND shiped_days < 40) THEN '30-40天'
                      WHEN (shiped_days >= 20 AND shiped_days < 30) THEN '20-30天'
                      WHEN (shiped_days >= 10 AND shiped_days < 20) THEN '10-20天'
                      WHEN (shiped_days >= 0 AND shiped_days < 10) THEN '00-10天'
                      ELSE 'NAN' END) AS shiped_days_class
        ,SUM(order_num) AS order_num
        ,SUM(deliver_onway_num) AS deliver_onway_num
        ,SUM(deliver_onway_cost) AS deliver_onway_cost
        ,SUM(deliver_onway_amount) AS deliver_onway_amount
FROM t301
GROUP BY (CASE WHEN shiped_days >= 90 THEN '90天以上'
                      WHEN (shiped_days >= 80 AND shiped_days < 90) THEN '80-90天'
                      WHEN (shiped_days >= 70 AND shiped_days < 80) THEN '70-80天'
                      WHEN (shiped_days >= 60 AND shiped_days < 70) THEN '60-70天'
                      WHEN (shiped_days >= 50 AND shiped_days < 60) THEN '50-60天'
                      WHEN (shiped_days >= 40 AND shiped_days < 50) THEN '40-50天'
                      WHEN (shiped_days >= 30 AND shiped_days < 40) THEN '30-40天'
                      WHEN (shiped_days >= 20 AND shiped_days < 30) THEN '20-30天'
                      WHEN (shiped_days >= 10 AND shiped_days < 20) THEN '10-20天'
                      WHEN (shiped_days >= 0 AND shiped_days < 10) THEN '00-10天'
                      ELSE 'NAN' END)
ORDER BY shiped_days_class;

-- ================================================================


-- 4.0 各仓退货在途商品数 & 成本金额
-- 退货在途的退货单
t401 AS
(SELECT returned_rec_id
,returned_depot_id
FROM jolly.who_wms_returned_order_info
WHERE return_status = 1 OR return_status = 3 OR return_status = 6
),
t402 AS
(SELECT t401.returned_depot_id AS depot_id
,SUM(p1.returned_num) AS returned_onway_num
,SUM(p1.returned_num * p2.in_price) AS returned_onway_amount
FROM t401
LEFT JOIN jolly.who_wms_returned_order_goods p1 ON p1.returned_rec_id = t401.returned_rec_id
LEFT JOIN jolly.who_sku_relation p2 ON p1.sku_id = p2.rec_id
GROUP BY t401.returned_depot_id
)


# jolly和jolly_wms都有who_wms_returned_order_info
# 状态的字段不一样，而且取值含义也不同

-- 退货在途：加入国家、承运商、下单时间、是否cod、仓库等维度
WITH
-- 退货在途的退货单
t401 AS
(SELECT returned_rec_id
,returned_depot_id
,returned_order_id
,returned_order_sn
,return_back_shipping_name
,FROM_UNIXTIME(apply_time, 'yyyy-mm') AS return_apply_month
FROM jolly.who_wms_returned_order_info
WHERE return_status = 1 OR return_status = 3 OR return_status = 6
),
-- 获取每个国家的id和名称
t302 AS
(SELECT region_id
,region_name
FROM jolly.who_region
WHERE region_type = 0
    AND region_status = 1  p4
                ON p3.country = p4.region_id
WHERE region_type = 0  --国家
AND region_status = 1  -- 状态为：启用
)
-- 结果表
SELECT t401.returned_depot_id
,t401.return_apply_month
,p3.depot_id AS fa_depot_id
,(CASE WHEN p3.pay_id = 41 THEN 'cod' ELSE 'not_cod' END) AS is_cod
,t302.region_name AS country
,t401.return_back_shipping_name AS return_ship_name
,UPPER(p5.shipping_name) AS fa_ship_name
,FROM_UNIXTIME(p3.add_time, 'yyyy-mm') AS order_add_month
,SUM(p1.returned_num) AS returned_onway_num
,SUM(p1.returned_num * p2.in_price) AS returned_onway_amount
FROM t401
LEFT JOIN jolly.who_wms_returned_order_goods p1 ON p1.returned_rec_id = t401.returned_rec_id
LEFT JOIN jolly.who_sku_relation p2 ON p1.sku_id = p2.rec_id
LEFT JOIN jolly.who_order_info p3 ON t401.returned_order_id = p3.order_id
LEFT JOIN jolly.who_order_user_info p4 ON t401.returned_order_id = p4.order_id
LEFT JOIN t302 ON t302.region_id = p4.country
LEFT JOIN jolly.who_shipping p5 ON p3.real_shipping_id = p5.shipping_id
GROUP BY t401.returned_depot_id
,t401.return_apply_month
,p3.depot_id
,(CASE WHEN p3.pay_id = 41 THEN 'cod' ELSE 'not_cod' END)
,t302.region_name
,t401.return_back_shipping_name
,UPPER(p5.shipping_name)
,FROM_UNIXTIME(p3.add_time, 'yyyy-mm')
ORDER BY order_add_month
,return_apply_month;

-- 统计各订单在途数量
SELECT p2.depot_id AS fa_depot_id
,t302.region_name AS country
,UPPER(p4.shipping_name) AS fa_shipping_name
,SUM(p1.goods_number) AS deliver_onway_num
,SUM(p1.goods_number * p1.in_price) AS deliver_onway_cost_amount
,SUM(p1.goods_number * p1.goods_price) AS deliver_onway_sale_amount
FROM jolly.who_order_goods p1
LEFT JOIN jolly.who_order_info p2 ON p1.order_id = p2.order_id
LEFT JOIN jolly.who_order_user_info p3 ON p2.order_id = p3.order_id
LEFT JOIN t302 ON t302.region_id = p3.country
LEFT JOIN jolly.who_shipping p4 ON p2.real_shipping_id = p4.shipping_id
WHERE p1.order_id in (SELECT * FROM t301)
GROUP BY p2.depot_id
,t302.region_name
,UPPER(p4.shipping_name)
ORDER BY fa_depot_id
,country
,fa_shipping_name
,deliver_onway_num DESC
;













-- 总退货在途数
-- 计算方式一
SELECT return_status
,SUM(returned_goods_num)
FROM jolly.who_wms_returned_order_info
GROUP BY return_status
ORDER BY return_status;
-- 计算方式二
WITH
-- 退货在途的退货单
t1 AS
(SELECT returned_rec_id
FROM jolly.who_wms_returned_order_info
WHERE return_status = 1 OR return_status = 3
)
SELECT SUM(returned_num)
FROM jolly.who_wms_returned_order_goods
WHERE returned_rec_id in (SELECT * FROM t1);


-- 4.0 各仓退货在途商品数 & 成本金额
-- 计算方法一
-- 退货在途的退货单
t401 AS
(SELECT returned_rec_id
FROM jolly.who_wms_returned_order_info
WHERE return_status = 1 OR return_status = 3
),
-- 退货在途
t402 AS
(SELECT p2.returned_depot_id AS depot_id
,SUM(p1.returned_num) AS returned_onway_num
,SUM(p1.returned_num * p3.in_price) AS returned_onway_amount
FROM jolly.who_wms_returned_order_goods p1
LEFT JOIN jolly.who_wms_returned_order_info p2 ON p1.returned_rec_id = p2.returned_rec_id
LEFT JOIN jolly.who_sku_relation p3 ON p1.sku_id = p3.rec_id
WHERE p1.returned_rec_id in (SELECT * FROM t401)
GROUP BY p2.returned_depot_id
)


-- 计算方法二
WITH
-- 退货在途的退货单
t1 AS
(SELECT returned_rec_id
,returned_depot_id
FROM jolly.who_wms_returned_order_info
WHERE return_status = 1 OR return_status = 3
),
t2 AS
(SELECT t1.returned_rec_id
,t1.returned_depot_id
,p1.returned_num
FROM t1
LEFT JOIN jolly.who_wms_returned_order_goods p1 ON p1.returned_rec_id = t1.returned_rec_id
)
-- 结果表
SELECT returned_depot_id
,SUM(returned_num) AS returned_onway_num
FROM t2
GROUP BY returned_depot_id
ORDER BY returned_depot_id;

--计算方法三
WITH
-- 退货在途的退货单
t1 AS
(SELECT returned_rec_id
,returned_depot_id
FROM jolly.who_wms_returned_order_info
WHERE return_status = 1 OR return_status = 3
)
SELECT t1.returned_depot_id
,SUM(p1.returned_num) AS returned_onway_num
,SUM(p1.returned_num * p2.in_price) AS returned_onway_amount
FROM t1
LEFT JOIN jolly.who_wms_returned_order_goods p1 ON p1.returned_rec_id = t1.returned_rec_id
LEFT JOIN jolly.who_sku_relation p2 ON p1.sku_id = p2.rec_id
GROUP BY t1.returned_depot_id
ORDER BY t1.returned_depot_id;

-- SUM(jolly.who_wms_returned_order_goods .returned_num)
-- 2017-8-2查询结果
/*
4   1037130
7   920999
*/
-- 2017-8-7查询结果
/*
4   47401.0
7   583063.0
*/
-- 为什么变化如此大？变化在哪里？怎么可以查到？


/*
-- 退货的以下维度：
国家
承运商
订单时间（支付时间）
cod/非cod
发货仓库、退货仓库
*/












-- 4.1 退货的快递公司
WITH
-- 退货在途的退货单
t1 AS
(SELECT returned_rec_id
,returned_depot_id
,returned_order_id
,returned_order_sn
,return_back_shipping_name
FROM jolly.who_wms_returned_order_info
WHERE return_status = 1 OR return_status = 3 OR return_status = 6
)
SELECT t1.returned_depot_id
,t1.return_back_shipping_name
,SUM(p1.returned_num) AS returned_onway_num
FROM t1
LEFT JOIN jolly.who_wms_returned_order_goods p1 ON t1.returned_rec_id = p1.returned_rec_id
GROUP BY t1.returned_depot_id
,t1.return_back_shipping_name
ORDER BY t1.returned_depot_id;


-- 4.2 退货在途商品数量在各国的分布



-- 4.3 申请退货时间的商品数分布
WITH
-- 退货在途的退货单
t1 AS
(SELECT returned_rec_id
,returned_depot_id
,return_back_shipping_name
,FROM_UNIXTIME(apply_time, 'yyyy-mm-dd') AS apply_return_date
FROM jolly.who_wms_returned_order_info
WHERE return_status <> 2
GROUP BY returned_rec_id
,returned_depot_id
,return_back_shipping_name
,FROM_UNIXTIME(apply_time, 'yyyy-mm-dd')
)
SELECT t1.returned_depot_id
,t1.return_back_shipping_name
,t1.apply_return_date
,SUM(p1.returned_num) AS returned_onway_num
FROM t1
LEFT JOIN jolly.who_wms_returned_order_goods p1 ON t1.returned_rec_id = p1.returned_rec_id
GROUP BY t1.returned_depot_id
,t1.return_back_shipping_name
,t1.apply_return_date
ORDER BY t1.returned_depot_id;


-- 4.4 退货完成时间
-- 完成退货的商品数没有太大波动
SELECT FROM_UNIXTIME(returned_time, 'yyyy-mm-dd') AS returned_date
,SUM(returned_goods_num) AS returned_num
FROM jolly.who_wms_returned_order_info
WHERE returned_time >= UNIX_TIMESTAMP('2017-07-01')
GROUP BY FROM_UNIXTIME(returned_time, 'yyyy-mm-dd')
ORDER BY returned_date;


-- 4.5 审核时间
SELECT FROM_UNIXTIME(audit_time, 'yyyy-mm-dd') AS audit_date
,SUM(returned_goods_num) AS returned_num
FROM jolly.who_wms_returned_order_info
WHERE audit_time >= UNIX_TIMESTAMP('2017-07-01')
GROUP BY FROM_UNIXTIME(audit_time, 'yyyy-mm-dd')
ORDER BY audit_date;




-- tracking_detail
WITH
t1 AS
(SELECT returned_rec_id
,SUM(returned_goods_num) AS returned_num
FROM jolly.who_wms_returned_order_info
WHERE return_status in (5, 6, 7)
GROUP BY returned_rec_id
)
SELECT FROM_UNIXTIME(p1.gmt_created, 'yyyy-mm-dd')  AS gmt_date
,SUM(t1.returned_num) AS returned_num
,count(p1.returned_rec_id) AS id_num
FROM t1
LEFT JOIN jolly.who_wms_returned_order_tracking_detail p1 ON t1.returned_rec_id = p1.returned_rec_id
GROUP BY FROM_UNIXTIME(p1.gmt_created, 'yyyy-mm-dd')
ORDER BY gmt_date;





SELECT return_status
,SUM(returned_goods_num)
FROM jolly.who_wms_returned_order_info
GROUP BY return_status
ORDER BY return_status;
/*
0   8.0
1   272.0
3   622352.0
4   57.0
5   4659837.0
6   243878.0
7   2356248.0
*/

-- 退货商品上架信息
jolly.who_wms_return_on_shelf_info
jolly.who_wms_return_on_shelf_goods




-- 全部订单来看

# shipping_status 商品配送情况：0未发货,1已发货,2已收货,3备货中,4退货
# pay_status 0未付款,1已付款,2部分付款,3付款中,4付款失败
# order_status 1已确认，2已取消，3已退货，4已拆分
# cod_check_status 货到付款订单审核状态，0未审核，1审核通过，2审核失败, 3 cod已支付(投递成功),4 cod投递失败,5 cod已回款 ,6 已拒收

WITH
-- 退货订单中有商品退回到jc仓的订单
t1 AS
(SELECT distinct c.returned_order_id
FROM jolly.who_wms_returned_order_goods AS c
INNER JOIN jolly.who_wms_returned_order_info AS d
       ON c.returned_rec_id=d.returned_rec_id
WHERE c.returned_stock_num>0
AND c.stock_end_time<UNIX_TIMESTAMP('2017-08-16')
AND d.returned_order_status=1
AND c.stock_end_time>0
),
-- 订单明细，标记是否已退回仓库
t2 AS
(SELECT p1.order_id
,decode(p1.pay_id, 41, 'cod', '非cod') AS pay_type
,decode(p1.shipping_status, 0, '未发货', 1, '已发货', 2, '已收货', 3, '备货中', 4, '退货') AS ship_status
,decode(p1.cod_check_status, 0, '未审核', 1, '审核通过', 2, '审核失败', 3,  'cod已支付(投递成功)', 4, 'cod投递失败', 5, 'cod已回款', 6, '已拒收') AS cod_status
,decode(p2.shipping_state, 0, '在途中', 1, '已揽收', 2, '疑难', 3, '已签收', 4, '退签', 5, '同城派送中', 6, '退回', 7, '转单', 8, '已拒收') AS ship_state
,t1.returned_order_id
,p1.goods_num
FROM jolly.who_order_info p1
LEFT JOIN jolly.who_order_shipping_tracking p2
    ON p1.order_id = p2.order_id
LEFT JOIN t1
    ON p1.order_id = t1.returned_order_id
WHERE p1.order_status = 1
AND p1.pay_status = 1
AND p1.is_shiped = 1
)

-- 结果
SELECT pay_type
,ship_status
,cod_status
,ship_state
,(CASE WHEN returned_order_id is null THEN '在途' ELSE '已入仓' END) AS return_status
,SUM(t2.goods_num) AS goods_num
,count(distinct order_id)
FROM t2
GROUP BY pay_type
,ship_status
,cod_status
,ship_state
,(CASE WHEN returned_order_id is null THEN '在途' ELSE '已入仓' END)
ORDER BY pay_type
,ship_status
,cod_status
,ship_state
,return_status;




/*
-- total
2.0292095e7

-- shipping_status
1   4350333.0
2   1.5941762e7

-- shipping_status AND cod_check_status
1   0   165525.0
1   1   1776701.0
1   3   1258.0
1   4   2376531.0
1   6   30318.0
2   0   2024237.0
2   1   81.0
2   3   2174273.0
2   4   120.0
2   5   1.1743013e7
2   6   38.0

*/

-- 退货的已退回仓库的商品数量
SELECT SUM(in_stock_num)
FROM jolly.who_wms_returned_order_goods
WHERE is_stock = 1
/*
1491201.0
*/




-- 2017-9-15 财务部 彭章华
-- 发货在途待签收订单中各商品发出天数的分布
WITH
-- 发货在途待签收订单的发货天数及商品数量
t1 AS
(SELECT DATEDIFF(NOW(), FROM_UNIXTIME(p2.shipping_time, 'yyyy-MM-dd')) AS shiped_days
        ,SUM(p3.goods_send_num) AS goods_send_num
        ,SUM(p3.goods_send_num * p3.in_price) AS goods_in_price
FROM jolly.who_order_shipping_tracking p1
LEFT JOIN jolly.who_order_info p2 ON p1.order_id = p2.order_id
LEFT JOIN jolly.who_order_goods p3 ON p1.order_id = p3.order_id
WHERE p1.shipping_state = 0 OR p1.shipping_state = 5  -- 在途
GROUP BY DATEDIFF(NOW(), FROM_UNIXTIME(p2.shipping_time, 'yyyy-MM-dd'))
),
t2 AS
(SELECT *
        ,(CASE WHEN shiped_days >= 90 THEN '90天以上'
                      WHEN (shiped_days >= 80 AND shiped_days < 90) THEN '80-90天'
                      WHEN (shiped_days >= 70 AND shiped_days < 80) THEN '70-80天'
                      WHEN (shiped_days >= 60 AND shiped_days < 70) THEN '60-70天'
                      WHEN (shiped_days >= 50 AND shiped_days < 60) THEN '50-60天'
                      WHEN (shiped_days >= 40 AND shiped_days < 50) THEN '40-50天'
                      WHEN (shiped_days >= 30 AND shiped_days < 40) THEN '30-40天'
                      WHEN (shiped_days >= 20 AND shiped_days < 30) THEN '20-30天'
                      WHEN (shiped_days >= 10 AND shiped_days < 20) THEN '10-20天'
                      WHEN (shiped_days >= 0 AND shiped_days < 10) THEN '00-10天'
                      ELSE 'NAN' END) AS shiped_days_class
FROM t1
)

SELECT *
FROM t2
ORDER BY shiped_days;





-- who_order_shipping_tracking:订单派送跟踪表



-- who_wms_returned_order_info_cod:cod退回订单管理






WITH
-- 到达目的国日期和天数
t401 AS
(SELECT order_id
        ,DATEDIFF(FROM_UNIXTIME(UNIX_TIMESTAMP()), FROM_UNIXTIME(destination_time)) AS destination_days
        ,FROM_UNIXTIME(destination_time, 'yyyy-MM-dd') AS destination_date
FROM jolly.who_prs_cod_order_shipping_time
),
-- 3.发货在途 -- ========================================================================================
-- 发货在途待签收订单
t300 AS
(SELECT p1.order_id
        ,p2.order_sn
        ,p2.depot_id
        ,p5.region_name AS country_name
        ,(CASE WHEN p2.real_shipping_id = 40 THEN 'Aramex'
                     WHEN p2.real_shipping_id = 170 THEN 'fetchr'
                     WHEN p2.real_shipping_id IN (168, 171) THEN 'SMSA'
                     WHEN p2.real_shipping_id IN (172, 174, 176) THEN 'Naqel'
                     ELSE 'Others' END) AS shipping_name
        ,p1.shipping_state
        ,FROM_UNIXTIME(CASE WHEN p2.prepare_pay_time=0 THEN p2.pay_time ELSE p2.prepare_pay_time END) AS pay_time
        ,FROM_UNIXTIME(p2.shipping_time) AS shipping_time
        ,DATEDIFF(NOW(), FROM_UNIXTIME(p2.shipping_time, 'yyyy-MM-dd')) AS shiped_days
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
        ,p2.order_sn
        ,p2.depot_id
        ,p5.region_name
        ,p1.shipping_state
        ,(CASE WHEN p2.prepare_pay_time=0 THEN p2.pay_time ELSE p2.prepare_pay_time END)
        ,p2.shipping_time
        ,(CASE WHEN p2.real_shipping_id = 40 THEN 'Aramex'
                     WHEN p2.real_shipping_id = 170 THEN 'fetchr'
                     WHEN p2.real_shipping_id IN (168, 171) THEN 'SMSA'
                     WHEN p2.real_shipping_id IN (172, 174, 176) THEN 'Naqel'
                     ELSE 'Others' END)
        ,DATEDIFF(NOW(), FROM_UNIXTIME(p2.shipping_time, 'yyyy-MM-dd'))
        ,t401.destination_date
        ,t401.destination_days
)
SELECT *
FROM t300
WHERE shiped_days >= 90
LIMIT 10;


-- 每天签收数量
SELECT FROM_UNIXTIME(p1.update_time, 'yyyy-MM-dd') AS data_date
        ,SUM(p2.goods_number) AS receive_num
        ,SUM(p2.order_total_amount) AS receive_amount
FROM jolly.who_order_shipping_tracking p1
LEFT JOIN zydb.dw_order_sub_order_fact p2
             ON p1.order_id = p2.order_id
          AND p1.update_time > UNIX_TIMESTAMP(p2.shipping_time)
WHERE p1.shipping_state=3
     AND p1.update_time >= UNIX_TIMESTAMP('2017-08-01')
     AND p1.update_time < UNIX_TIMESTAMP(TO_DATE(NOW()))
GROUP BY FROM_UNIXTIME(p1.update_time, 'yyyy-MM-dd')
;

-- 每天销售数量
-- zydb.dw_order_sub_order_fact
SELECT p1.depod_id AS depot_id
        ,SUBSTR(CASE WHEN p1.pay_id = 41 THEN p1.pay_time ELSE p1.result_pay_time END, 1, 10) AS data_date
        ,SUM(p1.goods_number) AS sale_num
FROM zydb.dw_order_sub_order_fact p1
WHERE p1.depod_id IN (4, 5, 6, 7)
     AND p1.order_status = 1
     AND p1.pay_status IN (1, 3)
     AND (CASE WHEN p1.pay_id = 41 THEN p1.pay_time ELSE p1.result_pay_time END) >= '2017-08-01'
GROUP BY p1.depod_id
        ,SUBSTR(CASE WHEN p1.pay_id = 41 THEN p1.pay_time ELSE p1.result_pay_time END, 1, 10)
;

-- 各仓每天销售出库数&退货入库数
WITH
-- GZ2 + DG1 + HK
t1 AS
(SELECT p1.depot_id
        ,FROM_UNIXTIME(p1.change_time, 'yyyy-MM-dd') AS data_date
        ,SUM(CASE WHEN p1.change_type = 5 THEN p1.change_num ELSE 0 END) AS whout_num
        ,SUM(CASE WHEN p1.change_type = 3 THEN p1.change_num ELSE 0 END) AS return_num
FROM jolly.who_wms_goods_stock_detail_log p1
WHERE p1.depot_id IN (4, 5, 6)
     AND p1.change_time >= unix_timestamp('2017-08-01')
GROUP BY p1.depot_id
        ,FROM_UNIXTIME(p1.change_time, 'yyyy-MM-dd')
),
-- SA
t2 AS
(SELECT p1.depot_id
        ,FROM_UNIXTIME(p1.change_time, 'yyyy-MM-dd') AS data_date
        ,SUM(CASE WHEN p1.change_type = 5 THEN p1.change_num ELSE 0 END) AS whout_num
        ,SUM(CASE WHEN p1.change_type = 3 THEN p1.change_num ELSE 0 END) AS return_num
FROM jolly_wms.who_wms_goods_stock_detail_log p1
WHERE p1.depot_id = 7
     AND p1.change_time >= unix_timestamp('2017-08-01')
GROUP BY p1.depot_id
        ,FROM_UNIXTIME(p1.change_time, 'yyyy-MM-dd')
),
-- UNION
t3 AS
(SELECT * FROM t1
UNION ALL
SELECT * FROM t2
)
SELECT *
FROM t3
LIMIT 10;





SELECT p1.order_id
        ,p1.order_sn
        ,p1.invoice_no
        ,(CASE WHEN p1.cod_check_status = 4 THEN '4投递失败' ELSE '6已拒收' END) AS cod_status
        ,FROM_UNIXTIME(CASE WHEN p1.prepare_pay_time = 0 THEN p1.pay_time ELSE p1.prepare_pay_time END, 'yyyy-MM-dd') AS pay_date
        ,FROM_UNIXTIME(p1.shipping_time, 'yyyy-MM-dd') AS shipping_date
        ,DATEDIFF(NOW(), FROM_UNIXTIME(p1.shipping_time, 'yyyy-MM-dd')) AS shiped_days
        ,(CASE WHEN p1.real_shipping_id = 40 THEN 'Aramex'
                     WHEN p1.real_shipping_id = 170 THEN 'fetchr'
                     WHEN P1.real_shipping_id IN (168, 171) THEN 'SMSA'
                     WHEN P1.real_shipping_id IN (172, 174, 176) THEN 'Naqel'
                     ELSE 'Others' END) AS shipping_name
        ,p2.return_time
FROM jolly.who_order_info p1
LEFT JOIN zydb.dw_order_shipping_tracking_node p2
            ON p1.order_id = p2.order_id
WHERE p1.is_shiped = 1
     AND p1.shipping_time >= UNIX_TIMESTAMP('2017-07-01')
     AND p1.shipping_time < UNIX_TIMESTAMP(TRUNC(NOW(), 'DD'))
     AND p1.cod_check_status in (4, 6)
LIMIT 100;
