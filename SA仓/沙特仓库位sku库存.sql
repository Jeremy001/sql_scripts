
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



