-- 春节备货商品的发货情况  ==================================
WITH 
-- 01.备货需求
t1 AS
(SELECT p1.rec_id
        ,p2.pur_order_goods_rec_id
        ,p1.goods_id
        ,p1.sku_id
        ,p1.depot_id
        ,p1.supp_name
        ,p3.cat_level1_name
        ,p1.supp_num
        ,p1.oos_num
        ,p1.send_num
        ,FROM_UNIXTIME(p1.gmt_created) AS create_time   -- 需求生成时间
        ,FROM_UNIXTIME(p1.check_time) AS check_time     -- 需求审核时间
FROM jolly_spm.jolly_spm_pur_goods_demand p1
LEFT JOIN jolly_spm.jolly_spm_pur_goods_demand_relation p2
             ON p1.rec_id = p2.demand_rec_id
LEFT JOIN zydb.dim_jc_goods p3
             ON p1.goods_id = p3.goods_id
WHERE p1.pur_type = 11          -- 11表示节日备货
     AND p1.gmt_created >= UNIX_TIMESTAMP('2017-12-19', 'yyyy-MM-dd')       -- 春节备货的需求在12月19日开始生成需求
),
t2 AS
(SELECT t1.rec_id
        ,t1.pur_order_goods_rec_id
        ,t1.goods_id
        ,t1.sku_id
        ,t1.depot_id
        ,t1.supp_name
        ,t1.cat_level1_name
        ,t1.supp_num
        ,t1.oos_num
        ,t1.send_num
        ,t1.create_time
        ,t1.check_time
        ,p5.pur_order_sn
        ,MIN(FROM_UNIXTIME(p5.gmt_created)) AS min_send_time        -- 供应商发货时间
        --,p8.tracking_no       -- 同一个采购需求，可能对应了多个tracking_no
        ,MIN(FROM_UNIXTIME(p9.gmt_created)) AS min_receipt_time      -- 到货签收时间
FROM t1
LEFT JOIN jolly_spm.jolly_spm_pur_order_goods p4        -- 采购单详情表
             ON t1.pur_order_goods_rec_id = p4.rec_id 
LEFT JOIN jolly_spm.jolly_spm_pur_order_info p5        -- 采购单表
             ON p4.pur_order_id = p5.pur_order_id 
LEFT JOIN jolly.who_wms_delivered_order_info p7        -- 发货单表
             ON p5.pur_order_sn = p7.delivered_order_sn
LEFT JOIN jolly.who_wms_delivered_order_tracking_info p8        -- 采购单物流信息表
             ON p7.delivered_order_id = p8.delivered_order_id
LEFT JOIN jolly.who_wms_delivered_receipt_info p9       -- 到货签收单
             ON TRIM(p8.tracking_no) = TRIM(p9.tracking_no)
GROUP BY t1.rec_id
        ,t1.pur_order_goods_rec_id
        ,t1.goods_id
        ,t1.sku_id
        ,t1.depot_id
        ,t1.supp_name
        ,t1.cat_level1_name
        ,t1.supp_num
        ,t1.oos_num
        ,t1.send_num
        ,t1.create_time
        ,t1.check_time
        ,p5.pur_order_sn
        --,p8.tracking_no
),
t3 AS 
-- 按照供应商和一级类目汇总
(SELECT supp_name
        ,cat_level1_name
        ,COUNT(DISTINCT goods_id) AS goods_count
        ,COUNT(DISTINCT sku_id) AS sku_count
        ,SUM(supp_num) AS demand_num
        ,SUM(send_num) AS send_num
        ,SUM(CASE WHEN receipt_time IS NULL THEN 0 ELSE send_num END) AS receipt_num
        ,SUM(supp_num - send_num) AS didnt_send_num
        ,SUM(oos_num) AS oos_num
FROM t2
GROUP BY supp_name
        ,cat_level1_name
)
SELECT *
FROM t3
ORDER BY demand_num DESC
LIMIT 10;

/*
1   96080252
2   95966454
3   95923822
4   96000116
5   95909024
6   96632682
 */

select sum(supp_num)
FROM t2;



select *
from t2
where rec_id = 96080252
;

,
t3 as 
(select rec_id
        ,count(*) AS num
from t2
group by rec_id
)
select rec_id
from t3
where num >= 2
LIMIT 10;