-- 2017-7-24
-- 仓库需求：查询某段时间内拋重过高的订单中主要包含哪些类目和SKU

 -- jolly.who_wms_weigh_package_info 包裹称重信息表
 -- 有的订单可能会有多个包裹，这些订单就会有多条记录，记录每个包裹的重量

-- jolly.who_wms_order_shipping_info

-- 疑问：包裹和订单的关联关系在哪张表？
-- 一个订单有多个包裹a/b/c，怎么查询这三个包裹分别包含哪些商品？【目前无法查询，wms没区分】

SELECT *
FROM jolly.who_wms_weigh_package_info
LIMIT 10;

-- 订单包裹打包信息表jolly.who_wms_order_package
SELECT * 
FROM jolly.who_wms_order_package
LIMIT 10;

--订单发运信息表 jolly.who_wms_order_shipping_info
SELECT * 
FROM jolly.who_wms_order_shipping_info
LIMIT 10;

-- jolly.who_wms_order_shipping_info_extra


-- RESULT_CODE
SELECT RESULT_CODE
        ,REMARK
        ,COUNT(*)
FROM jolly.who_wms_weigh_package_info
WHERE GMT_CREATED >= UNIX_TIMESTAMP('2017-07-14 11:28:00')
    AND GMT_CREATED <= UNIX_TIMESTAMP('2017-08-09 16:12:00')
GROUP BY RESULT_CODE
        ,REMARK
ORDER BY RESULT_CODE;
/*

 result_code remark
0   称重成功 
1   订单号对应的订单不存在
2   订单发货状态错误
3   订单已经退货
4   抛重过高
6   称重重量错误
 */

-- 一段时间内订单的抛重情况，细分到二三级类目
-- 通过T0来剔除掉有多个包裹的订单
WITH 
T0 AS
(SELECT order_id
        ,COUNT(*) AS pkg_num
FROM jolly.who_wms_weigh_package_info
WHERE GMT_CREATED >= UNIX_TIMESTAMP('2017-01-01')
GROUP BY order_id
),

/*
 -- 查看包裹数量
SELECT pkg_num
        ,count(order_id)
FROM T0
GROUP BY pkg_num
ORDER BY pkg_num;
 */


-- 一段时间内的订单明细
T1 AS
(SELECT P1.order_id
        ,P1.ORDER_SN
        ,P1.WEIGHT
        ,P2.PACKAGE_VOLUME_WEIGHT
        ,(P1.WEIGHT / P2.PACKAGE_VOLUME_WEIGHT) AS WEIGHT_PROP
        ,P1.DEPOT_ID
        ,P1.REMARK
        ,FROM_UNIXTIME(P1.GMT_CREATED, 'yyyy-MM-dd HH:mm') AS GMT_TIME
FROM jolly.who_wms_weigh_package_info P1
INNER JOIN T0 ON P1.order_id = T0.order_id AND T0.pkg_num = 1    -- 剔除有多个包裹的订单
LEFT JOIN jolly.WHO_WMS_ORDER_SHIPPING_INFO P2 ON P1.order_id = P2.order_id
WHERE P1.RESULT_CODE = 0
        OR P1.RESULT_CODE = 4
),
-- 查询二三级类目
T2 AS
(SELECT T1.*
        ,(CASE WHEN WEIGHT_PROP >=0 AND WEIGHT_PROP <= 1.0 THEN '[0, 100%]' 
                     WHEN WEIGHT_PROP >1.0 AND WEIGHT_PROP <= 1.1 THEN '(100%, 110%]'
                     WHEN WEIGHT_PROP >1.1 AND WEIGHT_PROP <= 1.2 THEN '(110%, 120%]'
                     WHEN WEIGHT_PROP >1.2 AND WEIGHT_PROP <= 1.3 THEN '(120%, 130%]'
                     WHEN WEIGHT_PROP >1.3 AND WEIGHT_PROP <= 1.4 THEN '(130%, 140%]'
                     WHEN WEIGHT_PROP >1.4 AND WEIGHT_PROP <= 1.5 THEN '(140%, 150%]'
                     WHEN WEIGHT_PROP >1.5 AND WEIGHT_PROP <= 1.6 THEN '(150%, 160%]'
                     WHEN WEIGHT_PROP >1.6 AND WEIGHT_PROP <= 1.7 THEN '(160%, 170%]'
                     WHEN WEIGHT_PROP >1.7 AND WEIGHT_PROP <= 1.8 THEN '(170%, 180%]'
                     WHEN WEIGHT_PROP >1.8 AND WEIGHT_PROP <= 1.9 THEN '(180%, 190%]'
                     WHEN WEIGHT_PROP >1.9 AND WEIGHT_PROP <= 2.0 THEN '(190%, 200%]'
                     ELSE  '200%+'
        END) AS WEIGHT_PROP_CLASS
        ,P1.GOODS_ID
        ,P2.CAT_LEVEL2_ID
        ,P3.CN_CAT_NAME AS CN_CAT2_NAME
        ,P2.CAT_LEVEL3_ID
        ,P4.CN_CAT_NAME AS CN_CAT3_NAME
FROM T1
LEFT JOIN jolly.WHO_ORDER_GOODS P1 ON T1.order_id = P1.order_id
LEFT JOIN ZYDB.DIM_JC_GOODS P2 ON P1.GOODS_ID = P2.GOODS_ID
LEFT JOIN jolly.WHO_CATEGORY P3 ON P2.CAT_LEVEL2_ID = P3.CAT_ID
LEFT JOIN jolly.WHO_CATEGORY P4 ON P2.CAT_LEVEL3_ID = P4.CAT_ID
WHERE T1.PACKAGE_VOLUME_WEIGHT > 0
),
-- 结果表
T3 AS
(SELECT ORDER_SN
        ,CN_CAT2_NAME
        ,CN_CAT3_NAME
        ,DEPOT_ID
        ,WEIGHT
        ,PACKAGE_VOLUME_WEIGHT
        ,WEIGHT_PROP
        ,WEIGHT_PROP_CLASS
        ,REMARK
        ,GMT_TIME
FROM T2
GROUP BY ORDER_SN
        ,CN_CAT2_NAME
        ,CN_CAT3_NAME
        ,DEPOT_ID
        ,WEIGHT
        ,PACKAGE_VOLUME_WEIGHT
        ,WEIGHT_PROP
        ,WEIGHT_PROP_CLASS
        ,REMARK
        ,GMT_TIME
)
SELECT *
FROM T3
ORDER BY ORDER_SN
        ,CN_CAT2_NAME
        ,CN_CAT3_NAME
;

-- 2.SKU
WITH 
-- 一段时间内的拋重过高订单明细
T1 AS
(SELECT order_id
FROM jolly.who_wms_weigh_package_info
WHERE REMARK = '抛重过高'
    AND GMT_CREATED >= UNIX_TIMESTAMP('2017-07-14 11:17:00')
    AND GMT_CREATED <= UNIX_TIMESTAMP('2017-07-21 05:50:00')
),
-- 查询SKU
T2 AS
(SELECT P2.ORDER_SN
        ,P1.GOODS_ID
        ,P1.SKU_ID
FROM jolly.WHO_ORDER_GOODS P1
LEFT JOIN jolly.WHO_ORDER_INFO P2 ON P1.order_id = P2.order_id
WHERE P1.order_id IN (SELECT * FROM T1)
)
-- 结果，只保留SKU_ID与ORDER_SN
SELECT SKU_ID
        ,COUNT(DISTINCT ORDER_SN) AS ORDER_NUM
FROM T2
GROUP BY SKU_ID
ORDER BY ORDER_NUM DESC;




