/*
内容：采购需求达成率：按需48h到货率和滚动备货96h到货率
计算逻辑：以推送出的采购需求商品数量为分母，以满足条件的到货商品数量为分子
更新时间：20171124
 */

WITH 
-- 采购推送需求商品明细
p0 AS
(SELECT  p1.rec_id              -- 需求id
        ,p1.depot_id
        ,p1.review_status        -- 审核状态
        ,p1.pur_type                 -- 根据采购类型区分按需和滚动备货
        ,p1.goods_id
        ,p1.sku_id
        ,p1.send_num              -- 发货数量
        ,p1.check_time            -- 审核时间
        ,COALESCE(p1.pur_order_goods_id,p2.demand_rec_id) AS pur_order_goods_rec_id   -- 对应商品   
FROM jolly.who_wms_pur_goods_demand p1 
LEFT JOIN jolly.who_wms_demand_goods_relation p2 
             ON p1.rec_id=p2.pur_order_goods_rec_id  
WHERE p1.review_status in (1,2)
),
-- 采购单到货签收时间
p5 AS 
(SELECT t.pur_order_id
        ,MAX(p5.gmt_created) AS receipt_time        -- 签收时间
FROM jolly.who_wms_pur_order_tracking_info t    
LEFT JOIN zydb.ods_wms_pur_deliver_receipt p5 
             ON TRIM(t.tracking_no) = TRIM(p5.tracking_no)                                                   
GROUP BY t.pur_order_id
),
-- 采购单商品明细表去重
-- 注意：2017-8-13 04:06:45之前的数据会有重复的情况，因此尽量取这之后的数据
-- 取数方法：
-- 2017-8-13之前的数据，取is_new=0
-- 2017-8-13开始的数据，取is_new=1
p1 AS
(SELECT p1.rec_id
        ,p1.pur_order_id
        ,p1.supp_num
        ,p1.check_num
        ,p1.exp_num
        ,p1.gmt_created
FROM jolly.who_wms_pur_order_goods p1
WHERE gmt_created < UNIX_TIMESTAMP('20170813', 'yyyyMMdd')
     AND is_new = 0
UNION ALL
SELECT p1.rec_id
        ,p1.pur_order_id
        ,p1.supp_num
        ,p1.check_num
        ,p1.exp_num
        ,p1.gmt_created
FROM jolly.who_wms_pur_order_goods p1
WHERE gmt_created >= UNIX_TIMESTAMP('20170813', 'yyyyMMdd')
     AND is_new = 1
),
-- 同一个商品对应多个上架单，上架的最晚时间
p7 AS
(SELECT p7.pur_order_goods_rec_id
        ,MAX(p7.gmt_created) AS onshelf_time
FROM jolly.who_wms_pur_deliver_goods p7 
GROUP BY p7.pur_order_goods_rec_id
),
-- 汇总数据
t3 AS 
(SELECT p0.rec_id AS demand_id
        ,p0.depot_id
        ,p0.pur_type
        ,p0.goods_id
        ,p0.sku_id
        ,p0.review_status
        ,p0.send_num
        ,(CASE WHEN p0.review_status = 1 THEN p0.check_time ELSE p1.gmt_created END) AS push_time
        ,p1.rec_id
        ,p1.pur_order_id
        ,p1.supp_num
        ,p1.check_num
        ,p1.exp_num
        ,p5.receipt_time
        ,p7.onshelf_time     -- 开始上架时间
        ,(CASE WHEN COALESCE(p5.receipt_time,4102416000) - COALESCE(p7.onshelf_time,4102416000) > 0 THEN p7.onshelf_time ELSE p5.receipt_time END) AS qianshou_time
FROM p0
LEFT JOIN p1          -- 采购单商品明细表[一个采购单对应多个需求的商品]
             ON p0.pur_order_goods_rec_id = p1.rec_id    -- 关联需求对应的商品id
LEFT JOIN p5          -- 获得签收时间
             ON p1.pur_order_id=p5.pur_order_id
LEFT JOIN p7          -- 获得上架时间
             ON p0.pur_order_goods_rec_id = p7.pur_order_goods_rec_id 
)

-- 计算每天推送的数量和未来48小时内到货的数量
SELECT depot_id
        ,FROM_UNIXTIME(push_time, 'yyyy-MM-dd') AS push_date
        ,SUM(CASE WHEN pur_type IN (1, 2, 7) THEN send_num ELSE 0 END) AS need_pur_num
        ,SUM(CASE WHEN pur_type IN (1, 2, 7) AND ((qianshou_time - push_time)/3600) < 24 THEN send_num ELSE 0 END) AS pur_receive24h_num
        ,SUM(CASE WHEN pur_type IN (1, 2, 7) AND ((qianshou_time - push_time)/3600) < 48 THEN send_num ELSE 0 END) AS pur_receive48h_num
        ,SUM(CASE WHEN pur_type IN (1, 2, 7) AND ((qianshou_time - push_time)/3600) < 72 THEN send_num ELSE 0 END) AS pur_receive72h_num
        ,SUM(CASE WHEN pur_type IN (1, 2, 7) AND ((qianshou_time - push_time)/3600) < 96 THEN send_num ELSE 0 END) AS pur_receive96h_num
        ,SUM(CASE WHEN pur_type IN (1, 2, 7) AND ((qianshou_time - push_time)/3600) < 120 THEN send_num ELSE 0 END) AS pur_receive120h_num
FROM t3
WHERE push_time >= unix_timestamp('2017-10-01') 
     AND push_time < unix_timestamp('2017-11-27')
GROUP BY depot_id
        ,FROM_UNIXTIME(push_time, 'yyyy-MM-dd')
ORDER BY depot_id
        ,FROM_UNIXTIME(push_time, 'yyyy-MM-dd')
;



-- 查询明细 ====================================================
WITH 
-- 采购推送需求商品明细
p0 AS
(SELECT  p1.rec_id              -- 需求id
        ,p1.depot_id
        ,p1.review_status        -- 审核状态
        ,p1.pur_type                 -- 根据采购类型区分按需和滚动备货
        ,p1.goods_id
        ,p1.sku_id
        ,p1.send_num              -- 发货数量
        ,p1.check_time            -- 审核时间
        ,p1.supp_name
        ,COALESCE(p1.pur_order_goods_id,p2.demand_rec_id) AS pur_order_goods_rec_id   -- 对应商品   
FROM jolly.who_wms_pur_goods_demand p1 
LEFT JOIN jolly.who_wms_demand_goods_relation p2 
             ON p1.rec_id=p2.pur_order_goods_rec_id  
WHERE p1.review_status in (1,2)
),
-- 采购单到货签收时间
p5 AS 
(SELECT t.pur_order_id
        ,t.tracking_no
        ,t.tracking_id
        ,MAX(p5.gmt_created) AS receipt_time        -- 签收时间
FROM jolly.who_wms_pur_order_tracking_info t    
LEFT JOIN zydb.ods_wms_pur_deliver_receipt p5 
             ON TRIM(t.tracking_no) = TRIM(p5.tracking_no)           
WHERE t.is_new = 1                                        
GROUP BY t.pur_order_id
        ,t.tracking_no
        ,t.tracking_id
),
-- 明细数据
t0 AS
(SELECT p0.rec_id AS demand_id
        ,p0.sku_id
        ,p0.supp_name
        ,p0.send_num
        ,FROM_UNIXTIME(p0.check_time) AS push_time 
        ,p1.rec_id
        ,p1.pur_order_id
        ,b.pur_order_sn
        ,FROM_UNIXTIME(p1.gmt_created) AS gmt_created
        ,p1.supp_num
        ,p5.tracking_no
        ,FROM_UNIXTIME(p5.receipt_time) AS receipt_time
FROM p0
LEFT JOIN jolly.who_wms_pur_order_goods p1 
             ON p0.pur_order_goods_rec_id = p1.rec_id 
LEFT JOIN jolly.who_wms_pur_order_info b 
             ON p1.pur_order_id = b.pur_order_id
LEFT JOIN p5 
             ON p1.pur_order_id=p5.pur_order_id
WHERE p1.is_new = 1
     AND b.is_new = 1
     AND (CASE WHEN p0.review_status = 1 THEN p0.check_time ELSE p1.gmt_created END) >= UNIX_TIMESTAMP('2017-11-24')
     AND (CASE WHEN p0.review_status = 1 THEN p0.check_time ELSE p1.gmt_created END) < UNIX_TIMESTAMP('2017-11-26')
)

SELECT SUBSTR(push_time, 1, 10) AS push_date
        ,COUNT(*)
        ,SUM(send_num)
        ,SUM(supp_num)
FROM t0
GROUP BY SUBSTR(push_time, 1, 10) 
;



select FROM_UNIXTIME(check_time, 'yyyy-MM-dd') AS check_date
        ,sum(supp_num)
        ,sum(send_num)
FROM jolly.who_wms_pur_goods_demand 
where check_time>= unix_timestamp('2017-11-24') 
and check_time< unix_timestamp('2017-11-26')
GROUP BY FROM_UNIXTIME(check_time, 'yyyy-MM-dd') 
;