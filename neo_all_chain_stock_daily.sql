
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
,data_date string);

-- 插入数据 =================================================
WITH
-- 1.采购在途商品数量 & 成本金额（人民币）
t100 AS
(SELECT p1.depot_id
        ,SUM(p1.pur_shiped_order_onway_num) AS purchase_onway_num      -- 采购在途数量
        ,SUM(p1.pur_shiped_order_onway_num * p2.in_price) AS purchase_onway_cost    --采购在途成本金额（人民币）
FROM jolly_oms.who_wms_goods_stock_onway_total p1
LEFT JOIN jolly.who_sku_relation p2 ON p1.sku_id = p2.rec_id
--WHERE p1.depot_id IN (4, 5, 6, 7, 8, 14, 15)
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
--WHERE p1.depot_id in (4, 5, 6, 7, 8, 14, 15)
GROUP BY p1.depot_id
),
-- 在库库存 = 实际在库库存 + 调出库存
t200 AS
(SELECT t203.depot_id
        ,(t203.instock_num + t204.allocate_onway_num) AS instock_num
        ,(t203.instock_cost + t204.allocate_onway_cost) AS instock_cost
FROM t203
INNER JOIN t204 ON t203.depot_id = t204.depot_id
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
             ON p1.order_id = p2.order_id AND P2.depot_id IN (4, 5, 6, 7, 8, 14, 15) AND p2.order_status = 1
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
        ,NVL(t100.purchase_onway_num, 0) AS purchase_onway_num
        ,NVL(t200.instock_num, 0) AS instock_num
        ,NVL(t300.deliver_onway_num, 0) AS deliver_onway_num
        ,NVL(t400.return_onway_num, 0) AS return_onway_num
        ,NVL(t100.purchase_onway_cost, 0) AS purchase_onway_cost
        ,NVL(t200.instock_cost, 0) AS instock_cost
        ,NVL(t300.deliver_onway_cost, 0) AS deliver_onway_cost
        ,NVL(t400.return_onway_cost, 0) AS return_onway_cost
        ,NVL(t300.deliver_onway_amount, 0) AS deliver_onway_amount
        ,NVL(t400.return_onway_amount, 0) AS return_onway_amount
        ,current_date() AS data_date
FROM t200
LEFT JOIN t100 ON t200.depot_id = t100.depot_id
LEFT JOIN t300 ON t200.depot_id = t300.depot_id
LEFT JOIN t400 ON t200.depot_id = t400.depot_id
ORDER BY t200.depot_id;


-- 插入之前查询到的数据 ==========================================

-- 8-25(已导入)
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(4, 59409, 228132, 607695, 0, 1442650.69, 6455796.41, 16921593.45, 0, 45710755.57, 0, '2017-08-25');
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(5, 92009, 437405, 1043490, 0, 2201616.16, 13768791.73, 28637146.5, 0, 77251939.44, 0, '2017-08-25');
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(6, 29431, 87077, 110326, 91361, 746559.92, 3486438.15, 2975769.39, 2798448.4, 7911004.12, 7600549, '2017-08-25');
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(7, 97, 521310, 179309, 1988294, 7812.32, 17338683.42, 5055798.84, 62861954.68, 13148327.27, 173616466, '2017-08-25');
-- 9-20(已导入)
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(4, 62938, 290673, 626294, 0, 1487903, 8265543, 18074344, 0, 51292721, 0, '2017-09-20');
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(5, 97716, 556370, 1049333, 0, 2332419, 16973952, 29805235, 0, 84966323, 0, '2017-09-20');
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(6, 42215, 94579, 105562, 115900, 1065660, 3565154, 2906598, 3510060, 8226679, 9451199, '2017-09-20');
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(7, 25, 602812, 223944, 1750177, 32957, 21640985, 6016457, 55614815, 16875094, 152621346, '2017-09-20');
--9-29(已导入)
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(4, 86511, 388067, 802054, 0, 2475939, 10472184, 22369870, 0, 59622936, 0, '2017-09-29');
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(5, 138216, 730600, 1304234, 0, 3903853, 21216917, 36013949, 0, 95969675, 0, '2017-09-29');
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(6, 32552, 68655, 111889, 126963, 786911, 2739530, 3063740, 3864896, 8084285, 10412356, '2017-09-29');
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(7, 345, 640527, 218793, 2031350, 188385, 22904866, 5779697, 63889349, 15333960, 175370383, '2017-09-29');
--10-09(已导入)
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(4, 48305, 359272, 750115, 0, 1248273, 10382637, 21348846, 0, 56673963, 0, '2017-10-09');
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(5, 78591, 680442, 1239093, 0, 2016039, 20776467, 35009730, 0, 92816983, 0, '2017-10-09');
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(6, 31309, 64455, 86326, 129181, 791259, 2627924, 2323086, 3910697, 6129900, 10222551, '2017-10-09');
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(7, 179, 757874, 272058, 1858236, 176760, 27048493, 6961310, 58930664, 18140497, 157665440, '2017-10-09');
--10-26(已导入)
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(4, 33525, 310075, 793286, 0, 1005094, 8437716, 22585459, 0, 58871306, 0, '2017-10-26');
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(5, 53986, 598701, 1432307, 0, 1663327, 17703999, 40354765, 0, 105092145, 0, '2017-10-26');
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(6, 37600, 67618, 91815, 147959, 954289, 2645337, 2586382, 4434269, 6659534, 11630622, '2017-10-26');
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(7, 817, 808162, 239897, 1706812, 196005, 29132929, 6331104, 54428979, 16515492, 145730433, '2017-10-26');
-- 11-3(已导入)
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(4, 22614, 271132, 665765, 0, 610285, 7533608, 19545191, 0, 52324427, 0, '2017-11-03');
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(5, 45536, 547418, 1246839, 0, 1213974, 16554134, 36286962, 0, 97777939, 0, '2017-11-03');
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(6, 56, 106723, 102235, 154049, 167795, 3602304, 2911352, 4628620, 7840949, 12137601, '2017-11-03');
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(7, 813, 811922, 226325, 1677061, 190509, 29088512, 6042465, 53449342, 16015619, 142701623, '2017-11-03');
-- 11-7(已导入)
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(4, 92337, 349591, 675805, 0, 2024759.39, 9399294.18, 19591228.72, 0, 52459397.35, 0, '2017-11-07');
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(5, 167198, 666090, 1233651, 0, 3798816.62, 19255281.9, 35556093.08, 0, 95824256.59, 0, '2017-11-07');
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(6, 168, 95857, 115699, 158108, 11448, 3395917.69, 3272168.99, 4750803.01, 8812943.533, 12466195.44, '2017-11-07');
INSERT INTO TABLE zybiro.neo_all_chain_stock_daily
VALUES(7, 627, 813030, 243109, 1629857, 164780.4, 29136346.85, 6432316.29, 52204447.23, 16967725.75, 139258044.4, '2017-11-07');


-- 查询数据 ==================================================
SELECT *
FROM zybiro.neo_all_chain_stock_daily
WHERE depot_id = 5
ORDER BY data_date DESC
;

-- 按月汇总求平均
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



