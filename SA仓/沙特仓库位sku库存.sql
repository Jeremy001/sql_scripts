
-- SA仓商品库存（goods_id, sku_id, 货位号）
WITH t AS
(SELECT j.goods_sn
        ,j.goods_id
        ,j.cat_level1_name
        ,j.cat_level2_name
        ,j.goods_season
        ,e.sku_id
        ,h.prop_price    -- sku售价
        ,CONCAT(d.depot_sn, '-', c.depot_area_sn, g.channel_sn, '-', b.shelf_sn, '-', a.shelf_area_sn) AS shelf_area_sn_total
        ,d.depot_sn
        ,c.depot_area_sn
        ,g.channel_sn
        ,b.shelf_sn
        ,a.shelf_area_sn
        ,e.stock_num
FROM jolly_wms.who_wms_depot_shelf_area a
         ,jolly_wms.who_wms_depot_shelf b
         ,jolly_wms.who_wms_depot_area c
         ,jolly_wms.who_wms_depot d
         ,jolly_wms.who_wms_goods_stock_detail e
         ,jolly_wms.who_wms_depot_channel g
         ,jolly.who_sku_relation h
         ,zydb.dim_jc_goods j
WHERE a.depot_shelf_id = b.shelf_id
     AND c.depot_area_id = g.depot_area_id
     AND g.channel_id=b.depot_channel_id
     AND c.depot_id = d.depot_id
     AND a.shelf_area_id=e.shelf_area_id
     AND e.sku_id = h.rec_id
     AND h.goods_id = j.goods_id
     AND c.depot_area_type_id = 1       -- =1表示正品
     AND e.stock_num > 0
     AND e.depot_id = 7
),
-- 各个货位号的库存量
t2 AS
(SELECT goods_sn
        ,goods_id
        ,cat_level1_name
        ,cat_level2_name
        ,sku_id
        ,prop_price    -- sku售价
        ,shelf_area_sn_total
        ,SUM(stock_num) AS stock_num
FROM t
WHERE cat_level1_name IS NOT NULL
     --AND depot_area_sn NOT IN ('Q', 'V', 'X', 'Y', 'Z')    -- SA仓这些货区叫DP zone，这些货区存放的是diffective products
GROUP BY goods_sn
        ,goods_id
        ,cat_level1_name
        ,cat_level2_name
        ,sku_id
        ,prop_price    -- sku售价
        ,shelf_area_sn_total
)
SELECT COUNT(*)
FROM t2
;

-- depot_area_sn IN ('A', 'B', 'C', 'D', 'E', 'F', 'G')


-- 用表t，查询一定分类下的库存量
SELECT cat_level1_name
        ,cat_level2_name
        ,cat_level3_name
        ,goods_season
        ,SUM(stock_num) AS stock_num
FROM t
GROUP BY cat_level1_name
        ,cat_level2_name
        ,cat_level3_name
        ,goods_season
;


-- 沙特仓各sku自由库存和近1周销售件数
WITH
-- 最近1天的自由库存
t1 AS
(SELECT p1.sku_id
        ,SUM(p1.total_stock_num - p1.total_order_lock_num - p1.total_allocate_lock_num - p1.total_return_lock_num) AS free_stock_num
FROM ods.ods_who_wms_goods_stock_total_detail AS p1
WHERE p1.depot_id = 7
  AND p1.data_date = '20180318'
GROUP BY p1.sku_id
),
-- 近1周在沙特仓的销售件数
t2 AS
(SELECT p2.sku_id
        ,SUM(p2.goods_number) AS sales_num
FROM dw.dw_order_node_time AS p1
LEFT JOIN dw.dw_order_goods_fact AS p2
       ON p1.order_id = p2.order_id
WHERE p1.order_status = 1
  AND p1.depot_id = 7
  AND p1.pay_status IN (1, 3)
  AND p1.pay_time >= '2018-03-12'
  AND p1.pay_time <  '2018-03-19'
GROUP BY p2.sku_id
),
-- JOIN得到结果
t3 AS
(SELECT COALESCE(t1.sku_id, t2.sku_id) AS sku_id
        ,t1.free_stock_num
        ,t2.sales_num
FROM t1
FULL OUTER JOIN t2
             ON t1.sku_id = t2.sku_id
WHERE t1.free_stock_num >= 1
   OR t2.sales_num >= 1
)
SELECT *
FROM t3
;
