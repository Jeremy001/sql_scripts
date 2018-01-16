/*
说明：查询sku在各个货位上的库存
作者：Neo王政鸣
更新时间：2017-10-10
 */

-- 国内仓 =======================================================
-- 查询sku在各个货位号的库存

WITH
-- 库存明细
t1 AS
(SELECT f.depot_id
        ,f.sku_id
        ,CONCAT(e.depot_sn, '-', d.depot_area_sn, c.channel_sn, '-', b.shelf_sn, '-', a.shelf_area_sn) as shelf_area_sn
        ,(f.stock_num - f.order_lock_num - f.lock_allocate_num - f.return_lock_num) AS free_num
FROM jolly.who_wms_depot_shelf_area a
            ,jolly.who_wms_depot_shelf b
            ,jolly.who_wms_depot_channel c
            ,jolly.who_wms_depot_area d
            ,jolly.who_wms_depot e
            ,jolly.who_wms_goods_stock_detail f
WHERE a.depot_shelf_id = b.shelf_id
     AND b.depot_channel_id = c.channel_id
     AND c.depot_area_id = d.depot_area_id
     AND d.depot_id = e.depot_id
     AND f.shelf_area_id = a.shelf_area_id
     AND f.depot_id = 5
     --AND f.sku_id = 2461762
     AND f.stock_num > 0
)

SELECT *
FROM t1
LIMIT 10;

-- 沙特仓 ==========================================================================
-- 各库位号sku总库存
WITH t AS
(SELECT e.sku_id
        ,j.cat_level1_name
        ,concat(d.depot_sn,'-',c.depot_area_sn,g.channel_sn,'-',b.shelf_sn,'-',a.shelf_area_sn) AS shelf_area_sn
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
     AND e.stock_num>0
     AND e.depot_id=7
)

select *
from t
where cat_level1_name is not null
LIMIT 10;

