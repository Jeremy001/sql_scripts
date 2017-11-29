/*
内容：打包数据挖掘项目
时间：20171129
作者：Neo王政鸣
 */


-- 1.包裹称重信息表：jolly.who_wms_weigh_package_info
-- 2.订单包裹打包信息表：jolly.who_wms_order_package
-- 3.订单商品表：jolly.who_order_goods
-- 4.订单表：jolly.who_order_info
-- 5.商品属性表：zydb.dim_jc_goods
-- 6.订单发运信息表 jolly.who_wms_order_shipping_info
-- 7.物料信息表：jolly.who_wms_material

WITH 
-- 明细数据
t1 AS
(SELECT p1.order_id    -- 订单id
        ,p1.order_sn    -- 订单编号
        --,p1.depot_id    -- 订单所属仓库id
        --,p2.result_code    -- 称重结果代码
        --,p2.remark    -- 称重结果说明
        --,p3.shipping_no    -- 货运单号
        
        -- 以下是打包耗材的信息
        ,p3.material_id    -- 打包耗材ID
        ,p3.material_sn    -- 打包耗材编码
        ,p3.material_name    -- 打包耗材名称
        ,p3.material_weight    -- 打包耗材重量
        ,p3.material_volume    -- 打包耗材体积
        ,p3.material_standard   -- 打包耗材的标准尺寸
        
        -- 以下是包裹的信息，包括各个重量，体积，尺寸等
        ,p2.weight      -- 包裹重量
        ,p6.package_weight    -- 包裹重量
        ,p6.express_paper_weight    -- 面单重量，即承运商用于计算费用的重量
        ,p6.package_size    -- 包裹尺寸规格
        ,p6.package_type    -- 0箱子， 1袋子
        --,p6.real_shipping_id    -- 物流承运商id
        --,p6.real_shipping_name    -- 物流承运商名称
        ,p6.package_num AS package_goods_num   -- 包裹内商品数量
        ,p6.goods_weight AS total_goods_weight    -- 商品总重量
        ,p6.real_shipping_price    -- 系统计算出的实际运费
        ,p6.total_packages    -- 包裹数量
        ,p6.package_volume_weight    -- 包裹抛重，即根据体积计算
        ,p6.total_volume    -- 包裹总体积
        
        -- 以下是商品信息
        ,p4.goods_id    -- 订单所含商品id
        ,p4.sku_id    -- 订单所含skuid
        ,p4.sku_value    -- sku属性（序列化）
        ,p4.goods_number    -- 商品数量
        ,p4.goods_price    -- 商品成交价格（美元）
        ,p4.in_price     -- 商品采购价格（人民币）
        ,p5.goods_weight     -- 商品重量
        ,p5.cat_level1_name    -- 商品的一级类目名称
        ,p5.cat_level1_id    -- 商品的一级类目id
        ,p5.cat_level2_name    -- 商品的二级类目名称
        ,p5.cat_level2_id    -- 商品的二级类目id
        ,p5.cat_level3_name    -- -- 商品的三级类目名称
        ,p5.cat_level3_id    -- 商品的三级类目id
FROM jolly.who_order_info p1
LEFT JOIN jolly.who_wms_weigh_package_info p2 
             ON p1.order_id = p2.order_id
          AND p2.result_code IN (0, 4)    -- 称重成功和抛重过高
LEFT JOIN jolly.who_wms_order_package p3
             ON p1.order_id = p3.order_id
LEFT JOIN jolly.who_wms_order_shipping_info p6
             ON p1.order_id = p6.order_id
LEFT JOIN jolly.who_order_goods p4
             ON p1.order_id = p4.order_id
LEFT JOIN zydb.dim_jc_goods p5
             ON p4.goods_id = p5.goods_id
WHERE p1.is_shiped = 1
     AND p1.order_status = 1
     AND p1.depot_id IN (4, 5, 6)
)

-- 数据行数
SELECT COUNT(*)
FROM t1;

-- 前20条数据
SELECT * 
FROM t1
LIMIT 20;


-- 查询打包物料
-- jolly.who_wms_material 
SELECT p1.* 
FROM jolly.who_wms_material p1
WHERE p1.material_type = 3    -- 3代表打包物料
     AND p1.status = 1    -- 1代表正常，非禁用
;

-- 查询物料的库存数量
-- jolly.who_wms_material_stock_detail




/*
-- 主题：打包项目，降低运费成本
-- 时间：20171129
-- 作者：Neo王政鸣
 */

-- 2017年总发运订单数及Aramex发运订单数
-- 国内3仓：real_shipping_id = 40, real_shipping_name = Aramex(HK)
-- 沙特仓：real_shipping_id = 200, real_shipping_name = Aramex
SELECT SUBSTR(p1.shipping_time, 1, 7) AS ship_month
        ,COUNT(p1.order_id) AS total_order_num
        ,SUM(CASE WHEN p1.real_shipping_id = 40 OR p1.real_shipping_id = 200 THEN 1 ELSE 0 END) AS aramex_order_num
FROM zydb.dw_order_sub_order_fact p1
WHERE p1.shipping_time >= '2017-01-01'
     AND p1.shipping_time < '2017-11-29'
     AND p1.is_shiped = 1 
     AND p1.order_status = 1
GROUP BY SUBSTR(p1.shipping_time, 1, 7)
ORDER BY SUBSTR(p1.shipping_time, 1, 7);

-- 2018年预测订单数
-- from jojo


-- Aramex在9/10/11月有多少比例的订单抛重
-- b.shipping_time：从2017-09-06 14:31:41开始
WITH 
-- 订单包裹重量
t1 AS
(SELECT b.customer_order_id AS order_id
        ,a.tracking_no
        ,FROM_UNIXTIME(b.shipped_time, 'yyyy-MM') AS ship_month
        ,a.total_volume_weight
        ,a.total_actual_weight
        ,(CASE WHEN (CEIL(a.total_volume_weight * 2) / 2) > (CEIL(a.total_actual_weight * 2) / 2) THEN 1 ELSE 0 END) AS is_paozhong
        --,MAX(a.total_volume_weight, a.total_actual_weight) AS charge_weight
FROM jolly_tms_center.tms_order_package AS a
INNER JOIN jolly_tms_center.tms_order_info AS b
                 ON a.tms_order_id=b.tms_order_id
INNER JOIN zydb.dw_order_sub_order_fact p1
                ON b.customer_order_id = p1.order_id
WHERE b.shipped_time >= unix_timestamp('2017-05-01')
     AND b.shipped_time < unix_timestamp('2017-12-01')
     AND p1.order_status = 1
     AND p1.is_shiped = 1
     AND p1.real_shipping_id IN (40, 200)
)
-- 计算抛重后的费用，以及在不抛重条件下所需的费用
-- 计算方式太复杂了，暂时用excel来计算

-- 抛重订单数量和比例, 订单平均重量
-- Aramex平均有一半的订单是抛重的
SELECT ship_month
        ,COUNT(order_id) AS total_order_num
        ,SUM(is_paozhong) AS paozhong_order_num
        ,AVG(total_actual_weight) AS total_actual_weight_mean
        ,AVG(CASE WHEN is_paozhong = 1 THEN total_actual_weight ELSE NULL END) AS paozhong_actual_weight_mean
FROM t1
GROUP BY ship_month
ORDER BY ship_month
;

SELECT order_id
        ,total_actual_weight
        ,total_volume_weight
        ,is_paozhong
FROM t1
WHERE ship_month = '2017-10'
LIMIT 10
;


-- 物流承运商价格表
-- jolly_tms_center.tms_carrier_price_zone_algorithm
-- 还不知道怎么使用这张表，计费规则据说非常复杂
SELECT * 
FROM jolly_tms_center.tms_carrier_price_zone_algorithm
WHERE zone_code = 'ARAMEX(HK)-2'
     --AND weight = 0.5
ORDER BY weight;



-- 物流商的运费这么贵？来看看占笔单价有多少？
SELECT SUBSTR(shipping_time, 1, 7) AS ship_month
        ,COUNT(order_id) AS order_num
        ,SUM(order_amount_no_bonus) AS order_amount
        ,SUM(order_amount_no_bonus) / COUNT(order_id) AS per_order_amount
FROM zydb.dw_order_sub_order_fact
WHERE real_shipping_id IN (40, 200)
     AND order_status = 1
     AND is_shiped = 1
     AND shipping_time >= '2017-01-01'
     AND shipping_time < '2017-12-01'
GROUP BY SUBSTR(shipping_time, 1, 7)
ORDER BY SUBSTR(shipping_time, 1, 7)
;



