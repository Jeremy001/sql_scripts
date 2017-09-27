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






-- 脚本二
SELECT e.sku_id
        ,e.shelf_area_id
        ,d.depot_sn
        ,c.depot_area_sn
        ,g.channel_sn
        ,b.shelf_sn
        ,a.shelf_area_sn
        ,concat(d.depot_sn,'-',c.depot_area_sn,g.channel_sn,'-',b.shelf_sn,'-',a.shelf_area_sn) AS shelf_area_sn
        ,e.stock_num
FROM jolly_wms.who_wms_goods_stock_detail e
LEFT JOIN jolly_wms.who_wms_depot_shelf_area a ON a.shelf_area_id=e.shelf_area_id 
LEFT JOIN jolly_wms.who_wms_depot_shelf b ON a.depot_shelf_id = b.shelf_id
LEFT JOIN jolly_wms.who_wms_depot_channel g ON g.channel_id=b.depot_channel_id
LEFT JOIN jolly_wms.who_wms_depot_area c ON c.depot_area_id = g.depot_area_id
LEFT JOIN jolly_wms.who_wms_depot d ON c.depot_id = d.depot_id 
WHERE e.depot_id=7
     AND e.sku_id = 3168690
     AND e.stock_num>0 
;