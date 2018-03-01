/*
-- 内容：缺货数据明细
-- 作者：cherish
-- 时间：2018-3-1
*/

-- 建表，存过程数据
use zybiro;
Drop table if exists  zybiro.yf_oos_detail_stock;
Create table zybiro.yf_oos_detail_stock
as
select depot_id,goods_id,sku_id,
sum(total_stock_num-total_order_lock_num-total_allocate_lock_num-total_return_lock_num) free_num
from  zydb.ods_who_wms_goods_stock_total_detail  s
where 1=1
and depot_id in (4,5,6,7)
and s.data_date = REGEXP_REPLACE(TO_DATE(DATE_SUB(CURRENT_TIMESTAMP(), 1)), '-', '');
and total_stock_num-total_order_lock_num-total_allocate_lock_num-total_return_lock_num>0
group by depot_id, goods_id,sku_id
;

-- 建表，存结果数据
Drop table  zybiro.yf_oos_detail_b;
Create table zybiro.yf_oos_detail_b
as
SELECT
        b.order_sn, b.site_id, b.goods_number  order_goods_number,b.order_amount_no_bonus,
        case when b.pay_id=41 then  b.pay_time else b.result_pay_time end as pay_time,
        f.supp_name,
        c.goods_id,c.goods_number,c.goods_price,
        a.sku_id,
        a.TYPE,
        a.oos_num,
        FROM_UNIXTIME(a.create_time) create_time,
        f.provide_code,
       f.cat_level1_name  cat_name,
       f.cat_level2_name sub_cat_name,
       b.depod_id,
      f.first_on_sale_time,
     c2.status,
     c2.is_stock,
     t.free_num
FROM   jolly.who_wms_order_oos_log a
   inner join         zydb.dw_order_sub_order_fact b  on  a.order_id=b.order_id
     left  join     zydb.dw_order_goods_fact c        on  b.order_id=c.order_id  and a.sku_id=c.sku_id
     left  join     jolly.who_sku_depot_coverage_area_status c2  on  c2.goods_id=c.goods_id and c2.sku_id=a.sku_id and depot_coverage_area_id =1
     left join       zydb.dim_jc_goods f              on  f.goods_id = c.goods_id
     left join     zybiro.yf_oos_detail_stock   t       on b.depod_id = t.depot_id  and a.sku_id=t.sku_id
WHERE  1=1
AND to_date(from_unixtime(a.create_time)) >= date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1)
AND to_date(from_unixtime(a.create_time)) < date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),0)
;


-- 推送脚本，从结果表中查询数据
select b.order_sn,b.site_id,b.order_goods_number,b.pay_time, b.order_amount_no_bonus,
        b.supp_name,
        b.goods_id,b.goods_number, b.goods_price,
        b.sku_id,
        b.TYPE,
        b.oos_num,
      b.create_time,
       b.provide_code,
       b.cat_name,
       b.sub_cat_name,
       b.depod_id,
      b.first_on_sale_time,
     b.status,
     b.is_stock,
   b.free_num
   from   zybiro.yf_oos_detail_b b
