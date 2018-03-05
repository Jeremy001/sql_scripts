/*
内容：打包数据挖掘项目
时间：20180305
作者：Neo王政鸣
*/

-- 1.包裹称重信息表：jolly.who_wms_weigh_package_info
-- 2.订单包裹打包信息表：jolly.who_wms_order_package, 如果进行了裁箱，那么表中的纸箱尺寸是裁箱后的尺寸；
-- 3.订单商品表：jolly.who_order_goods
-- 4.订单表：jolly.who_order_info
-- 5.商品属性表：zydb.dim_jc_goods
-- 6.订单发运信息表 jolly.who_wms_order_shipping_info
-- 7.物料信息表：jolly.who_wms_material

WITH
-- 明细数据
t1 AS
(SELECT p1.order_id    -- 订单id
        --,p1.order_sn    -- 订单编号
        --,p1.depod_id    -- 订单所属仓库id
        --,p3.shipping_no    -- 货运单号

        -- 以下是打包耗材的信息
        ,p3.material_id    -- 打包耗材ID
        ,p3.material_sn    -- 打包耗材编码
        ,p3.material_name    -- 打包耗材名称
        ,p3.material_weight    -- 打包耗材重量
        ,p3.material_volume    -- 打包耗材体积
        ,p3.material_standard   -- 打包耗材的最终尺寸

        -- 以下是包裹的信息，包括各个重量，体积，尺寸等
        ,p6.package_size    -- 包裹尺寸规格
        ,p6.package_num AS package_goods_num   -- 包裹内商品数量
        ,p6.goods_weight AS total_goods_weight    -- 商品总重量
        ,p6.total_packages    -- 包裹数量
        ,p6.total_volume    -- 包裹总体积
        ,p6.package_volume_weight    -- 包裹抛重，即根据体积计算
        ,(CASE WHEN (CEIL(p6.package_volume_weight * 2) / 2) > (CEIL(p6.express_paper_weight * 2) / 2) THEN 1 ELSE 0 END) AS is_paozhong
        ,p6.package_type    -- 0箱子， 1袋子  -- 根据秋瑾的信息，total_volume >0表示箱子， total_volume=0:其他
        ,p6.package_weight    -- 包裹重量
        ,p6.express_paper_weight    -- 面单重量，即承运商用于计算费用的重量
        ,p6.real_shipping_id    -- 物流承运商id
        ,p6.real_shipping_name    -- 物流承运商名称
        --,p6.real_shipping_price    -- 系统计算出的实际运费
        ,p8.weighing_factor AS carrier_domestic_transport_weighing_factor         -- 体积重因子，0表示不计抛，体积重(kg) = 长(cm) * 宽(cm) * 高(cm) / (体积重因子)
        ,p9.weighing_factor AS carrier_export_customsclearance_weighing_factor         -- 体积重因子，0表示不计抛，体积重(kg) = 长(cm) * 宽(cm) * 高(cm) / (体积重因子)
        ,p10.weighing_factor AS carrier_international_line_weighing_factor         -- 体积重因子，0表示不计抛，体积重(kg) = 长(cm) * 宽(cm) * 高(cm) / (体积重因子)
        ,p11.weighing_factor AS carrier_aimcountry_customsclearance_weighing_factor         -- 体积重因子，0表示不计抛，体积重(kg) = 长(cm) * 宽(cm) * 高(cm) / (体积重因子)
        ,p12.weighing_factor AS carrier_aimcountry_line_weighing_factor         -- 体积重因子，0表示不计抛，体积重(kg) = 长(cm) * 宽(cm) * 高(cm) / (体积重因子)
        ,p13.weighing_factor AS carrier_terminal_delivery_weighing_factor         -- 体积重因子，0表示不计抛，体积重(kg) = 长(cm) * 宽(cm) * 高(cm) / (体积重因子)

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
        ,p5.goods_season    -- 商品季节
FROM zydb.dw_order_sub_order_fact p1
LEFT JOIN jolly.who_wms_order_package p3
       ON p1.order_id = p3.order_id
LEFT JOIN jolly.who_wms_order_shipping_info p6
       ON p1.order_id = p6.order_id
LEFT JOIN jolly.who_order_goods p4
       ON p1.order_id = p4.order_id
LEFT JOIN zydb.dim_jc_goods p5
       ON p4.goods_id = p5.goods_id
LEFT JOIN jolly_tms_center.tms_shipping p7
       ON p1.real_shipping_id = p7.shipping_id
LEFT JOIN jolly_tms_center.tms_carrier p8
       ON p7.carrier_domestic_transport_id = p8.carrier_id
LEFT JOIN jolly_tms_center.tms_carrier p9
       ON p7.carrier_domestic_transport_id = p9.carrier_id
LEFT JOIN jolly_tms_center.tms_carrier p10
       ON p7.carrier_domestic_transport_id = p10.carrier_id
LEFT JOIN jolly_tms_center.tms_carrier p11
       ON p7.carrier_domestic_transport_id = p11.carrier_id
LEFT JOIN jolly_tms_center.tms_carrier p12
       ON p7.carrier_domestic_transport_id = p12.carrier_id
LEFT JOIN jolly_tms_center.tms_carrier p13
       ON p7.carrier_domestic_transport_id = p13.carrier_id
WHERE p1.is_shiped = 1
  AND p1.order_status = 1
  AND p1.depod_id IN (4, 5, 6, 14)
  AND p1.pay_time >= '2017-01-01'        -- 2017年
)

SELECT t1.*
FROM t1
LEFT JOIN jolly.who_sku_relation p1
       ON t1.sku_id = p1.rec_id




-- 数据行数
SELECT COUNT(*)
FROM t1;

-- 前20条数据
SELECT *
FROM t1
WHERE package_type = 0            -- 打包耗材是箱子
  AND package_volume_weight >= 0.01       -- package_volume_weight>0的订单才是计抛的订单
  AND real_shipping_id = 40  -- Aramex(HK)
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

-- 商品尺寸表
-- jolly.who_goods_size_property
SELECT *
FROM jolly.who_goods_size_property
LIMIT 10;

WITH t1 AS
(SELECT goods_id
        ,count(1) AS num
FROM jolly.who_goods_size_property
WHERE property_name = 'SizeDetails'
GROUP BY goods_id
)
SELECT count(*)
FROM t1
--WHERE num >= 4
;


-- 商品属性
-- jolly.who_sku_relation.sku_valued
SELECT *
from jolly.who_sku_relation
WHERE sku_value LIKE '%MATERIAL%'
LIMIT 20;

-- 每天销售商品的属性值
SELECT *
FROM zybiro.t_yf_heiwu_auc_daily_repot_07
WHERE vname = 'MATERIAL'
AND sku_id = cast(4681492 as STRING)
LIMIT 100;



-- 各物流商计算体积重时，除以多少？
-- 物流商会承运不同的段（一共有6个段），不同段可能收/不收抛重费用，或计抛的计算方式也不同
-- carrier_domestic_transport_id，国内运输承运商ID
-- carrier_export_customsclearance_id，出口清关承运商ID
-- carrier_international_line_id，国际干线承运商ID
-- carrier_aimcountry_customsclearance_id，目的国清关承运商ID
-- carrier_aimcountry_line_id，目的国干线承运商ID
-- carrier_terminal_delivery_id，终端派送承运商ID
SELECT *
FROM jolly_tms_center.tms_shipping
WHERE shipping_id = 40;
-- real_shipping_id表示的是线路，实际上是把上面的六个段拼接到一起
-- 以上六个字段记录了每个段是由哪个物流商来承运

-- 查询部分订单（已发运）
SELECT *
FROM zydb.dw_order_sub_order_fact
WHERE pay_time >= '2017-11-11'
     AND is_shiped = 1
LIMIT 100;

-- 那么各个物流商是否计抛，就要看下表了：
-- jolly_tms_center.tms_carrier
-- weighing_factor = 5000, 表示：体积重 = 长 * 宽 * 高 / 5000
-- weighing_factor = 0, 表示：不计抛
SELECT *
FROM jolly_tms_center.tms_carrier
LIMIT 10;


/*
-- 主题：打包项目，降低运费成本
-- 时间：20171129
-- 作者：Neo王政鸣
 */

-- 2017年总发运订单数及主要物流商发运订单数
-- Aramex: real_shipping_id IN (40, 200)
-- Naqel: real_shipping_id IN (172, 174, 176)
-- Fetchr: real_shipping_id IN (170, 201, 257)
-- SMSA: real_shipping_id IN (168, 171)
SELECT SUBSTR(p1.shipping_time, 1, 7) AS ship_month
        ,COUNT(p1.order_id) AS total_order_num
        ,SUM(CASE WHEN p1.real_shipping_id IN (40, 200) THEN 1 ELSE 0 END) AS aramex_order_num
        ,SUM(CASE WHEN p1.real_shipping_id IN (172, 174, 176) THEN 1 ELSE 0 END) AS naqel_order_num
        ,SUM(CASE WHEN p1.real_shipping_id IN (170, 201, 257) THEN 1 ELSE 0 END) AS fetchr_order_num
        ,SUM(CASE WHEN p1.real_shipping_id IN (168, 171) THEN 1 ELSE 0 END) AS smsa_order_num
FROM zydb.dw_order_sub_order_fact p1
WHERE p1.shipping_time >= '2017-01-01'
     AND p1.shipping_time < '2017-12-01'
     AND p1.is_shiped = 1
     AND p1.order_status = 1
GROUP BY SUBSTR(p1.shipping_time, 1, 7)
ORDER BY SUBSTR(p1.shipping_time, 1, 7);

-- 2018年预测订单数
-- from jojo


-- 各承运商在9/10/11月有多少比例的订单抛重
-- b.shipping_time：从2017-09-06 14:31:41开始
WITH
-- 订单包裹重量
t1 AS
(SELECT b.customer_order_id AS order_id
        ,a.tracking_no
        ,p1.depot_id
        ,p1.real_shipping_id AS shipping_id
        ,p1.real_shipping_name AS shipping_name
        ,p2.is_cubic_weight         -- 是否计收抛重， 1=是，0=否
        ,FROM_UNIXTIME(b.shipped_time, 'yyyy-MM') AS ship_month
        ,a.total_volume_weight
        ,a.total_actual_weight
        ,(CASE WHEN (CEIL(a.total_volume_weight * 2) / 2) > (CEIL(a.total_actual_weight * 2) / 2) THEN 1 ELSE 0 END) AS is_paozhong
FROM jolly_tms_center.tms_order_package AS a
INNER JOIN jolly_tms_center.tms_order_info AS b
                 ON a.tms_order_id=b.tms_order_id
INNER JOIN zydb.dw_order_sub_order_fact p1
                ON b.customer_order_id = p1.order_id
INNER JOIN jolly.who_shipping p2
                 ON p1.real_shipping_id = p2.shipping_id
WHERE b.shipped_time >= unix_timestamp('2017-10-01')
     AND b.shipped_time < unix_timestamp('2017-12-01')
     AND p1.order_status = 1
     AND p1.is_shiped = 1
)
-- 计算抛重后的费用，以及在不抛重条件下所需的费用
-- 计算方式太复杂了，暂时用excel来计算

-- 抛重订单数量和比例, 订单平均重量
-- Aramex平均有一半的订单是抛重的
SELECT ship_month
        ,shipping_name
        ,COUNT(order_id) AS total_order_num
        ,SUM(is_cubic_weight) AS cubic_order_num
        ,SUM(CASE WHEN is_cubic_weight = 1 THEN is_paozhong ELSE 0 END) AS paozhong_order_num
        ,AVG(total_actual_weight) AS total_actual_weight_mean
        ,AVG(CASE WHEN is_paozhong = 1 THEN total_actual_weight ELSE NULL END) AS paozhong_actual_weight_mean
FROM t1
GROUP BY ship_month
        ,shipping_name
ORDER BY ship_month
        ,total_order_num
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

-- 核查商品重量
-- 1.zydb.dim_jc_goods，同jolly.who_goods
SELECT *
FROM zydb.dim_jc_goods p1
LIMIT 10;

SELECT count(p1.goods_id) AS total_goods_count
        ,sum(CASE WHEN p1.goods_weight IS NULL OR p1.goods_weight = 0 THEN 0 ELSE 1 END) AS weight_nnull_goods_count
FROM zydb.dim_jc_goods p1
;
-- 1045504 605301  57.90%

-- 统计商品的各种属性，分析无重量的商品主要分布

-- 1.从在售状态来看：1.以前的商品，已经下架的商品；2.当前在售商品；
-- is_on_sale:商品销售状态,1销售,0下架
-- is_forever_offsale:0临时下架，1永久下架

-- 2.从添加时间来看，分年份和月份
-- add_time

WITH
t1 AS
(SELECT FROM_UNIXTIME(p1.add_time, 'yyyy') AS add_year
        ,FROM_UNIXTIME(p1.add_time, 'yyyy-MM') AS add_month
        ,p1.provider_code
        ,(CASE WHEN p1.is_on_sale = 1 THEN 'yes' ELSE 'no' END) AS is_onsale
        ,(CASE WHEN p1.is_forever_offsale = 1 THEN 'yes' ELSE 'no' END) AS is_forever_offsale
        ,(CASE WHEN p1.goods_weight >= 0.00001 THEN 'yes' ELSE 'no' END) AS have_weight
        ,COUNT(p1.goods_id) AS goods_count
FROM jolly.who_goods AS p1
GROUP BY FROM_UNIXTIME(p1.add_time, 'yyyy')
        ,FROM_UNIXTIME(p1.add_time, 'yyyy-MM')
        ,p1.provider_code
        ,(CASE WHEN p1.is_on_sale = 1 THEN 'yes' ELSE 'no' END)
        ,(CASE WHEN p1.is_forever_offsale = 1 THEN 'yes' ELSE 'no' END)
        ,(CASE WHEN p1.goods_weight >= 0.00001 THEN 'yes' ELSE 'no' END)
)
SELECT *
FROM t1
;

-- 3.分供应商来看，是不是某些供应商不提供商品重量的情况比较多
WITH
t1 AS
(SELECT p1.provider_code
        ,COUNT(p1.goods_id) AS total_goods_count
        ,SUM(CASE WHEN p1.goods_weight >= 0.00001 THEN 1 ELSE 0 END) AS have_weight_goods_count
        ,SUM(CASE WHEN p1.goods_weight >= 0.00001 THEN 1 ELSE 0 END) / COUNT(p1.goods_id) AS have_weight_rate
        ,SUM(CASE WHEN p1.is_on_sale = 1 THEN 1 ELSE 0 END) AS onsale_goods_count
        ,SUM(CASE WHEN p1.is_on_sale = 1 THEN 1 ELSE 0 END) / COUNT(p1.goods_id) AS onsale_goods_rate
FROM jolly.who_goods AS p1
GROUP BY p1.provider_code
)
SELECT *
FROM t1
ORDER BY total_goods_count DESC
LIMIT 20
;


-- 在售商品表：zydb.dw_goods_on_sale
-- 分区字段：ds,格式：'yyyymmdd'
-- 有重复goods_id，因为同一个goods_id，在不同国家（仓库）的在售状态可能不同：A国在售B国不在售
SELECT *
FROM zydb.dw_goods_on_sale AS p1
WHERE ds = '20180108'
LIMIT 10;



-- jolly.who_goods_onsale_log，查看商品在售状态修改记录


-- 2.jolly.who_sku_relation
SELECT *
FROM jolly.who_sku_relation p1
LIMIT 10;

SELECT count(p1.rec_id) AS total_sku_count
        ,sum(CASE WHEN p1.sku_weight IS NULL OR p1.sku_weight = 0 THEN 0 ELSE 1 END) AS weight_nnull_sku_count
FROM jolly.who_sku_relation p1
;

-- jolly.who_sku_relation表中相同goods_id，不同sku_id的sku_weight相等，实际上是goods_weight
-- 对于衣服、鞋子这些常规的商品，是同一个重量。大件商品，比如家居的额装饰画，不同尺寸是有不同重量的
WITH
t1 AS
(SELECT p1.goods_id
        ,COUNT(DISTINCT p1.sku_weight) AS goods_count
FROM jolly.who_sku_relation AS p1
GROUP BY p1.goods_id
)
SELECT COUNT(goods_id)
FROM t1
WHERE t1.goods_count >= 2
;

-- 关联jolly.who_goods和jolly.who_sku_relation，比较goods_weight和sku_weight
WITH
-- 两个goods_weight
t1 AS
(SELECT p1.goods_id
        ,p1.goods_weight AS goods_weight_1
        ,p2.goods_weight AS goods_weight_2
FROM jolly.who_goods AS p1
LEFT JOIN jolly.who_product_pool AS p2
       ON p1.goods_id = p2.goods_id
),
-- join
t2 AS
(SELECT p1.rec_id
        ,p1.sku_weight
        ,(CASE WHEN t1.goods_weight_1 = 0 THEN t1.goods_weight_2 ELSE t1.goods_weight_1 END) AS goods_weight
FROM jolly.who_sku_relation AS p1
LEFT JOIN t1
       ON t1.goods_id = p1.goods_id
)
-- 统计各种null和not_null
SELECT COUNT(t2.rec_id) AS sku_count
        ,SUM(CASE WHEN t2.sku_weight = 0 THEN 0 ELSE 1 END) AS notnull_sku
        ,SUM(CASE WHEN t2.goods_weight = 0 THEN 0 ELSE 1 END) AS notnull_goods
        ,SUM(CASE WHEN t2.goods_weight > 0 AND t2.sku_weight > 0 THEN 1 ELSE 0 END) AS notnull_both
        ,SUM(CASE WHEN t2.goods_weight = 0 AND t2.sku_weight = 0 THEN 1 ELSE 0 END) AS null_both
        ,SUM(CASE WHEN t2.goods_weight > 0 OR t2.sku_weight > 0 THEN 1 ELSE 0 END) AS notnull_sku_or_notnull_goods
        ,SUM(CASE WHEN t2.goods_weight > 0 AND t2.sku_weight = 0 THEN 1 ELSE 0 END) AS null_sku_notnull_goods
FROM t2
;
-- 7817266  2954399 7800055 2954184 17152   7787123 4832724
-- 果然，看倒数第二个数字，如果用goods_weight来替换sku_weight，约可以覆盖99.6的sku


-- 3.jolly.who_product_pool

WITH
t1 AS
(SELECT FROM_UNIXTIME(p1.gmt_created, 'yyyy') AS add_year
        ,FROM_UNIXTIME(p1.gmt_created, 'yyyy-MM') AS add_month
        ,p1.supp_code
        ,COUNT(p1.goods_id) AS total_goods_count
        ,SUM(CASE WHEN p1.goods_weight >= 0.00001 THEN 1 ELSE 0 END) AS have_weight_goods_count
        ,SUM(CASE WHEN p1.goods_weight >= 0.00001 THEN 1 ELSE 0 END) / COUNT(p1.goods_id) AS have_weight_rate
FROM jolly.who_product_pool AS p1
GROUP BY FROM_UNIXTIME(p1.gmt_created, 'yyyy')
        ,FROM_UNIXTIME(p1.gmt_created, 'yyyy-MM')
        ,p1.supp_code
)
SELECT *
FROM t1
;

-- 关联who_goods和who_product_pool,得到两个表中的重量
WITH
t1 AS
(SELECT p1.goods_id AS goods_id_1
        ,p1.goods_weight AS goods_weight_1
        ,p2.goods_weight AS goods_weight_2
FROM jolly.who_goods AS p1
LEFT JOIN jolly.who_product_pool AS p2
       ON p1.goods_id = p2.goods_id
)
SELECT COUNT(t1.goods_id_1) AS goods_count
        ,SUM(CASE WHEN t1.goods_weight_1 >= 0.00001 THEN 1 ELSE 0 END) AS notnull_1
        ,SUM(CASE WHEN t1.goods_weight_2 >= 0.00001 THEN 1 ELSE 0 END) AS notnull_2
        ,SUM(CASE WHEN t1.goods_weight_1 >= 0.00001 AND t1.goods_weight_2 >= 0.00001 THEN 1 ELSE 0 END) AS notnull_both
        ,SUM(CASE WHEN t1.goods_weight_1 = 0 AND t1.goods_weight_2 = 0 THEN 1 ELSE 0 END) AS null_both
        ,SUM(CASE WHEN t1.goods_weight_1 >= 0.00001 OR t1.goods_weight_2 >= 0.00001 THEN 1 ELSE 0 END) AS notnull_1_or_notnull_2
        ,SUM(CASE WHEN t1.goods_weight_1 = 0 AND t1.goods_weight_2 >= 0.0001 THEN 1 ELSE 0 END) AS null_1_notnull_2
FROM t1
;
-- 1045511  605308  765594  330887  2478    1040015 434707
-- 果然，蓄水池表中的商品重量是最全的，有434707个goods_id在who_goods表中没有重量但在who_product_pool表中有商品重量
-- 加上这部分数据，一共有1040015个goods_id是有重量的，占比99.47%（只差5000多个goods_id）；

-- 商品的外包装尺寸在jolly.who_sku_goods_relation表中
SELECT *
FROM jolly.who_sku_goods_relation AS p1
WHERE p1.goods_id = 2086448
;


-- 更新goods_weight和sku_weight ================================================
/*
  1.jolly.who_goods表中的goods_weight如果为空，则用jolly.who_product_pool表的goods_weight替代；
  2.对于sku_weight = goods_weight的类目,如果jolly.who_sku_relation表中的sku_weight为空，则用1得到的goods_weight来替代；
*/

-- 1.先更新goods_weight
UPDATE who_goods
SET goods_weight = p1.goods_weight
FROM who_product_pool AS p1
WHERE who_goods.goods_weight = 0
  AND who_goods.goods_id = p1.goods_id
;

-- 2.再更新sku_weight
UPDATE who_sku_relation
SET sku_weight = p1.goods_weight
FROM who_goods AS p1
WHERE who_sku_relation.sku_weight = 0
  AND who_sku_relation.goods_id = p1.goods_id
;



-- 计算裁箱比例
-- 裁箱只会裁纸箱的高度
-- 纸箱的尺寸aa*bb*cc
WITH
-- 耗材
t1 AS
(SELECT p1.material_sn
        ,CAST(SUBSTR(p1.standard, 7, 2) AS int) AS height_standard
        ,p1.standard AS material_size_standard
        ,p1.material_shape
FROM jolly.who_wms_material p1
WHERE p1.material_type = 3    -- 3代表打包物料
  AND p1.status = 1    -- 1代表正常，非禁用
),
-- join得到结果
t2 AS
(SELECT p1.order_id    -- 订单id
        ,SUBSTR(p1.order_pack_time, 1, 7) AS pack_month
        ,p3.material_id    -- 打包耗材ID
        ,p3.material_sn    -- 打包耗材编码
        ,p3.material_name    -- 打包耗材名称
        ,t1.material_size_standard
        ,p3.material_standard AS material_size_new  -- 裁箱后的长宽高
        ,t1.height_standard
        ,CAST(SUBSTR(p3.material_standard, 7, 2) AS int) AS height_new   -- 纸箱的最终高度，若裁箱，则高度小于标准高度
        ,(t1.height_standard - CAST(SUBSTR(p3.material_standard, 7, 2) AS int)) AS height_minus      --标准尺寸-裁箱后尺寸，裁箱几厘米？
FROM zydb.dw_order_sub_order_fact p1
LEFT JOIN jolly.who_wms_order_package p3
       ON p1.order_id = p3.order_id
LEFT JOIN t1
       ON p3.material_sn = t1.material_sn
WHERE p1.is_shiped = 1
     AND p1.order_status = 1
     AND p1.depod_id IN (4, 5, 6, 14)
     AND p1.order_pack_time >= '2017-07-01'        -- 2017年
     AND p1.order_pack_time < '2018-01-01'        -- 2017年
     AND p3.material_volume >= 0.0001
)
-- 汇总-月度裁箱比例
SELECT t2.pack_month
        ,COUNT(*) AS total_num
        ,SUM(CASE WHEN t2.height_minus >= 1 THEN 1 ELSE 0 END) AS caixiang_num
        ,SUM(CASE WHEN t2.height_minus >= 1 THEN 1 ELSE 0 END) / COUNT(*) AS caixiang_rate
FROM t2
WHERE t2.height_minus >= 0
GROUP BY t2.pack_month
ORDER BY t2.pack_month
;
-- 汇总-各规格的裁箱比例
SELECT t2.material_size_standard
        ,COUNT(*) AS total_num
        ,SUM(CASE WHEN t2.height_minus >= 1 THEN 1 ELSE 0 END) AS caixiang_num
        ,SUM(CASE WHEN t2.height_minus >= 1 THEN 1 ELSE 0 END) / COUNT(*) AS caixiang_rate
FROM t2
WHERE t2.height_minus >= 0
GROUP BY t2.material_size_standard
ORDER BY t2.material_size_standard
;

-- 汇总-各规格裁箱尺寸:裁1cm/2cm/3cm/4cm/5cm/5+cm
SELECT t2.material_size_standard
        ,COUNT(*) AS total_num
        ,SUM(CASE WHEN t2.height_minus >= 1 THEN 1 ELSE 0 END) AS caixiang_num
        ,SUM(CASE WHEN t2.height_minus = 1 THEN 1 ELSE 0 END) AS caixiang_num_1cm
        ,SUM(CASE WHEN t2.height_minus = 2 THEN 1 ELSE 0 END) AS caixiang_num_2cm
        ,SUM(CASE WHEN t2.height_minus = 3 THEN 1 ELSE 0 END) AS caixiang_num_3cm
        ,SUM(CASE WHEN t2.height_minus = 4 THEN 1 ELSE 0 END) AS caixiang_num_4cm
        ,SUM(CASE WHEN t2.height_minus = 5 THEN 1 ELSE 0 END) AS caixiang_num_5cm
        ,SUM(CASE WHEN t2.height_minus >= 6 THEN 1 ELSE 0 END) AS caixiang_num_6cm
FROM t2
WHERE t2.height_minus >= 0
GROUP BY t2.material_size_standard
ORDER BY t2.material_size_standard
;


-- 总体数据：各月发出包裹数，纸箱包裹数，抛重包裹数
SELECT SUBSTR(p1.order_pack_time, 1, 7) AS pack_month
        ,COUNT(p1.order_id) AS order_num
        ,SUM(CASE WHEN p3.material_volume = 0 THEN 1 ELSE 0 END) AS pakdai_num
        ,SUM(CASE WHEN p3.material_volume = 0 THEN 0 ELSE 1 END) AS zhixiang_num
        ,SUM(CASE WHEN p3.material_volume = 0 THEN 0 ELSE 1 END) / COUNT(p1.order_id) AS zhixiang_rate
FROM zydb.dw_order_node_time p1
LEFT JOIN jolly.who_wms_order_package p3
       ON p1.order_id = p3.order_id
WHERE p1.is_shiped = 1
  AND p1.depot_id IN (4, 5, 6, 14)
  AND p1.order_pack_time >= '2017-01-01'        -- 2017年
GROUP BY SUBSTR(p1.order_pack_time, 1, 7)
ORDER BY pack_month
;


-- 模型效果评估 =================================================================

CREATE TABLE zybiro.neo_material_new
AS
SELECT p1.id_new
        ,p1.guige
FROM zybiro.neo_wms_material_new AS p1
GROUP BY p1.id_new
        ,p1.guige

-- 新旧耗材ID对比表：zybiro.neo_material_new
-- 订单推荐纸箱id表：zybiro.neo_recomend_material_list
-- 推荐结果表：zybiro.neo_recommend_order_list_result

WITH
-- 耗材
t1 AS
(SELECT p1.material_sn
        ,CAST(SUBSTR(p1.standard, 7, 2) AS int) AS height_standard
        ,p1.standard AS material_size_standard
        ,p1.material_shape
FROM jolly.who_wms_material p1
WHERE p1.material_type = 3    -- 3代表打包物料
  AND p1.status = 1    -- 1代表正常，非禁用
),
-- join得到结果
t2 AS
(SELECT p1.order_id    -- 订单id
        ,p2.package_weight
        ,p3.material_id AS real_material_id    --真实使用的打包耗材ID
        ,t1.material_size_standard
        ,p3.material_standard AS material_size_new
        ,t1.height_standard
        ,CAST(SUBSTR(p3.material_standard, 7, 2) AS int) AS height_new   -- 纸箱的最终高度，若裁箱，则高度小于标准高度
        ,(t1.height_standard - CAST(SUBSTR(p3.material_standard, 7, 2) AS int)) AS height_minus      --标准尺寸-裁箱后尺寸，裁箱几厘米？
        ,p1.material_id1 AS recom_material_id1    --推荐的第1个纸箱
        ,p4.guige AS recom_material_size1
FROM zybiro.neo_recomend_material_list p1
LEFT JOIN jolly.who_wms_order_shipping_info p2
       ON p1.order_id = p2.order_id
LEFT JOIN jolly.who_wms_order_package p3
       ON p1.order_id = p3.order_id
LEFT JOIN t1
       ON p3.material_sn = t1.material_sn
LEFT JOIN zybiro.neo_material_new AS p4
       ON p1.material_id1 = p4.id_new
)

SELECT *
FROM t2
;

-- 纸箱尺寸长度
SELECT LENGTH(t2.material_size_standard) AS size_len
        ,COUNT(order_id)
FROM t2
GROUP BY LENGTH(t2.material_size_standard)
ORDER BY size_len
;


SELECT p1.recom_material_id1
        ,p1.recom_material_size1
        ,p1.is_caixiang
        ,p1.is_paozhong
        ,(CASE WHEN p1.material_volume_standard < p1.material_volume_recom1 THEN 'smaller'
               WHEN p1.material_volume_standard = p1.material_volume_recom1 THEN 'equal'
               WHEN p1.material_volume_standard > p1.material_volume_recom1 THEN 'bigger'
               ELSE 'Others'
          END) AS size_duibi
        ,COUNT(p1.order_id) AS order_num
FROM zybiro.neo_recommend_order_list_result AS p1
WHERE p1.material_volume_standard IS NOT NULL
  AND p1.material_volume_recom1 IS NOT NULL
  AND p1.recom_material_id1 IS NOT NULL
GROUP BY p1.recom_material_id1
        ,p1.recom_material_size1
        ,p1.is_caixiang
        ,p1.is_paozhong
        ,(CASE WHEN p1.material_volume_standard < p1.material_volume_recom1 THEN 'smaller'
               WHEN p1.material_volume_standard = p1.material_volume_recom1 THEN 'equal'
               WHEN p1.material_volume_standard > p1.material_volume_recom1 THEN 'bigger'
               ELSE 'Others'
          END)
ORDER BY p1.recom_material_id1
;


-- 打包员抛重订单比例排名 ========================================================
WITH
-- 各打包员订单明细
t1 AS
(SELECT b.customer_order_id AS order_id
        ,(CASE WHEN p1.depod_id = 4 THEN 4 ELSE 5 END) AS depot_id
        ,CONCAT_WS('-', CAST(p3.order_pack_admin_id AS STRING), p4.user_name) AS pack_user
        ,FROM_UNIXTIME(b.shipped_time, 'yyyy-MM') AS ship_month
        ,a.total_volume
        ,
        ,a.total_volume_weight
        ,a.total_actual_weight
        ,(CASE WHEN (CEIL(a.total_volume_weight * 2) / 2) > (CEIL(a.total_actual_weight * 2) / 2) THEN 1 ELSE 0 END) AS is_paozhong
FROM jolly_tms_center.tms_order_package AS a
INNER JOIN jolly_tms_center.tms_order_info AS b
        ON a.tms_order_id=b.tms_order_id
INNER JOIN zydb.dw_order_sub_order_fact p1
        ON b.customer_order_id = p1.order_id
INNER JOIN jolly.who_shipping p2
        ON p1.real_shipping_id = p2.shipping_id
INNER JOIN jolly.who_order_info AS p3
        ON b.customer_order_id = p3.order_id
INNER JOIN jolly.who_rbac_user AS p4
        ON p3.order_pack_admin_id = p4.user_id
WHERE b.shipped_time >= unix_timestamp('2017-10-01')
  AND b.shipped_time < unix_timestamp('2018-03-01')
  AND p1.order_status = 1
  AND p1.is_shiped = 1
  AND p1.depod_id IN (4, 5, 14)
  AND a.total_volume_weight >= 0.00001
)
-- 汇总各打包员抛重订单比例
SELECT t1.depot_id
        ,t1.pack_user
        ,COUNT(t1.order_id) AS order_num
        ,SUM(t1.is_paozhong) AS paozhong_order_num
        ,SUM(t1.is_paozhong) / COUNT(t1.order_id) AS paozhong_order_rate
FROM t1
GROUP BY t1.depot_id
        ,t1.pack_user
ORDER BY t1.depot_id
        ,paozhong_order_rate
;
