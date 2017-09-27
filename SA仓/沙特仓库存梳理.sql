WITH 
-- 一些计算
T1 AS
(SELECT T.*
    ,(GREATEST(CNT_330_360T ,CNT_300_330T,CNT_270_300T,CNT_240_270T ,CNT_210_240T ,CNT_180_210T)/30) AS MAX_DAY_NUM
    ,(t.FRE_STK_CK07 * t.SKU_IN_PRICE * 6.88) AS IN_PRICE
    ,(CASE WHEN p1.goods_seASon = 1 THEN '春'
                 WHEN p1.goods_seASon = 2 THEN '夏'
                 WHEN p1.goods_seASon = 3 THEN '秋'
                 WHEN p1.goods_seASon = 4 THEN '冬'
                 WHEN p1.goods_seASon = 5 THEN '春夏'
                 WHEN p1.goods_seASon = 6 THEN '春秋'
                 WHEN p1.goods_seASon = 7 THEN '春冬'
                 WHEN p1.goods_seASon = 8 THEN '夏秋'
                 WHEN p1.goods_seASon = 9 THEN '夏冬'
                 WHEN p1.goods_seASon = 10 THEN '秋冬'
                 ELSE '其他' END) AS sku_seASon
FROM ZYBIRO.T_YF_STOCK_MONITOR_V1_13_2 T
LEFT JOIN zydb.dim_jc_goods p1 ON t.goods_id = CAST(p1.goods_id AS string)
WHERE DS = '20170907'
     AND cnt_000_060t <= 0
     AND FRE_STK_CK07 > 0
),
-- 可销天数
T2 AS
(SELECT T1.*
    ,(FRE_STK_CK07 / MAX_DAY_NUM) AS CAN_SALE_DAYS
FROM T1
),
-- 库存明细（货位号）
t3 AS
(SELECT concat(d.depot_sn,'-',c.depot_area_sn,g.channel_sn,'-',b.shelf_sn,'-',a.shelf_area_sn) AS shelf_area_sn
        ,CAST(e.sku_id AS string) AS sku_id
        ,(e.stock_num - e.order_lock_num - e.lock_allocate_num - e.return_lock_num) AS free_num
FROM jolly_wms.who_wms_depot_shelf_area a
         ,jolly_wms.who_wms_depot_shelf b
         ,jolly_wms.who_wms_depot_area c
         ,jolly_wms.who_wms_depot d
         ,jolly_wms.who_wms_goods_stock_detail e
         ,jolly_wms.who_wms_depot_channel g
WHERE a.depot_shelf_id = b.shelf_id
     AND c.depot_area_id = g.depot_area_id
     AND g.channel_id=b.depot_channel_id
     AND c.depot_id = d.depot_id 
     AND a.shelf_area_id=e.shelf_area_id 
     AND e.depot_id=7
),

-- 可销天数30+、30-，货位号，违禁品
t5 AS
(SELECT T2.*
        ,(CASE WHEN CAN_SALE_DAYS >30 THEN '30D+'
             WHEN CAN_SALE_DAYS <=30 THEN '30D-'
             ELSE '其他'
        END) AS CAN_SALE_DAYS_2
        ,t3.shelf_area_sn
        ,t3.free_num
FROM T2
LEFT JOIN t3 ON t2.sku_id = t3.sku_id
),

t100 AS
(SELECT sku_id
        ,goods_id
        ,goods_sn
        ,goodssuppcode AS supp_code
        ,supp_name
        ,cat1_name
        ,cat2_name
        ,sku_in_price
        ,'1滞销1级' AS zx_level
        ,sku_seASon
        ,round(max_day_num, 1) AS max_day_num
        ,fre_stk_ck07 AS 总库存数
        ,in_price
        ,can_sale_days
        ,can_sale_days_2
        ,shelf_area_sn
        ,free_num AS 库位上最新自由库存数
        ,cnt_330_360t
        ,cnt_300_330t
        ,cnt_270_300t
        ,cnt_240_270t
        ,cnt_210_240t
        ,cnt_180_210t
        ,cnt_150_180t
        ,cnt_120_150t
        ,cnt_090_120t
        ,cnt_060_090t
        ,cnt_030_060t
        ,cnt_000_030t
FROM t5
)

SELECT *
FROM t100
LIMIT 10;


-- ======================================================================
-- 查询之前滞销列表中的SKU中，近期有销售的SKU
WITH 
-- 滞销sku
t2 AS 
(SELECT sku_id
FROM ZYBIRO.T_YF_STOCK_MONITOR_V1_13_2 T
WHERE DS = '20170911'
     AND cnt_000_060t <= 0
     AND FRE_STK_CK07 > 0
),
-- 近期有销售的订单对应的sku的销售数量
t3 AS
(SELECT CAST(p1.sku_id AS string) AS sku_id
        ,SUM(p1.goods_number) AS goods_num
FROM zydb.dw_order_node_time p2 
LEFT JOIN jolly.who_order_goods p1 ON p2.order_id = p1.order_id
WHERE p2.pay_time >= '2017-09-11' 
      AND p2.pay_time < '2017-09-26' 
      AND p2.depot_id = 7
      AND p2.order_status = 1
      AND p2.pay_status IN (1, 3)
      AND p2.is_shiped = 1
GROUP BY CAST(p1.sku_id AS string) 
),
t4 AS
(SELECT t2.sku_id
        ,t3.goods_num
FROM t2 
LEFT JOIN t1 ON t2.sku_id = t1.sku_id
)

SELECT COUNT(*)
FROM t4
WHERE goods_num IS NOT NULL;