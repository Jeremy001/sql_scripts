
WITH 
-- 订单信息（金额除外）
a AS
(select a.order_id,(case 
                  when a.site_id=2 then 602
                  when a.site_id=1 then 601
                  when a.site_id=0 and a.order_source =5 then 900
                  when a.site_id=0 and a.order_source in (3,4) then 600
                  when a.site_id=0 and a.order_source in (6)   then 700
                  when a.site_id=0 and p3.email in ('jollyalie365@gmail.com','jollycorp2016@gmail.com',
                    'jollychicam@gmail.com','jollycorp20130613@gmail.com','jollycorp.asos@gmail.com',
                    'jollycorpuk@gmail.com','jollychicc@hotmail.com','meetu2016@hotmail.com',
                    'meetu2015@hotmail.com')  then 800
                  else 400
                  end
             )site_id,a.is_shiped,a.order_sn,a.depot_id,a.order_status,a.goods_num,a.pay_status,
round(CAST(a.pay_money AS Float) + CAST(a.surplus AS Float) + CAST(a.order_amount AS Float) ,2) as order_total_amount,
FROM_UNIXTIME(IF(prepare_pay_time = 0,a.pay_time,prepare_pay_time)) as pay_time,
FROM_UNIXTIME(a.no_problems_order_uptime) no_problems_order_uptime,
FROM_UNIXTIME(a.shipping_time) shipping_time,
case when a.pay_id =41 then 'cod' else 'not_cod' end  as  is_cod,
case when p3.source_order_id is not null then 1 else 0 end as is_split
 from default.who_order_info a 
left join default.who_order_user_info p3 on  p3.source_order_id=a.order_id --_brands_zy702
where 1=1
and a.depot_id in (4,5,6,7,14)
and a.pay_status in (1,3)
and IF(prepare_pay_time = 0,a.pay_time,prepare_pay_time)>=  UNIX_TIMESTAMP(DATE_SUB(now(),interval 10 day)) 
),

-- 未配商品数量
s2 AS 
(select  order_id, 
 sum(num) still_need_number
 from default.who_wms_goods_need_lock_detail s2  
 group by order_id
 ),


-- 明细结果
t0 AS
(select  to_date(pay_time) AS pay_date,
round((unix_timestamp(now())-unix_timestamp(pay_time))/3600,0) pay_duration,
a.order_id,a.order_sn,a.site_id,
(case when a.depot_id=4 then '广州2仓'
 when a.depot_id=5 then '东莞1仓'
 when a.depot_id =14 then '东莞2仓'
       when a.depot_id=6 then '香港仓'
        when a.depot_id=7 then '沙特仓' else null end) as depot, 
case when a.is_shiped=0 then '未开始配或不必配' when a.is_shiped=4 then '部分匹配'
      when  a.is_shiped=5 then  '完全匹配' when  a.is_shiped=6 then  '拣货完成'
  when  a.is_shiped=7 then  '待拣货' when  a.is_shiped=8 then  '拣货中'
         when  a.is_shiped=2 then  '部分发货' when  a.is_shiped=3 then  '待发货'
      when  a.is_shiped=1 then  '已发货' else null end as is_shiped,   
a.order_status,
a.goods_num,
s2.still_need_number,
a.order_total_amount,
a.pay_time,
a.no_problems_order_uptime,
a.shipping_time,
a.is_cod
FROM a
left join s2
on  a.order_id=s2.order_id  
-- left join zydb.dw_order_node_time t1
-- ON a.order_id = t1.order_id
where  a.is_split=0
and a.depot_id in (4,5,6,7,14)
and a.pay_status in (1,3)
and a.is_shiped in (0,2,3,4,5,6,7,8)
 and a.order_status not in (2,3) 
),

t00 AS
(select depot
        ,pay_date
        ,goods_num
        ,still_need_number
        ,(case when order_total_amount < 20 then '[0, 20)'
                    when order_total_amount < 40 then '[20, 40)'
                    when order_total_amount < 60 then '[40, 60)'
                    when order_total_amount < 80 then '[60, 80)'
                    when order_total_amount < 120 then '[80, 120)'
                    when order_total_amount < 150 then '[120, 150)'
                    when order_total_amount < 200 then '[150, 200)'
                    when order_total_amount >= 200 then '[200, )'
                    else 'NULL' end) AS order_amount_class
        ,count(order_id) AS order_num
        ,sum(goods_num) AS total_goods_num
        ,sum(still_need_number) AS total_still_need_num
        ,sum(order_total_amount) as order_total_amount2
from t0
group by depot
        ,pay_date
        ,goods_num
        ,still_need_number
        ,(case when order_total_amount < 20 then '[0, 20)'
                    when order_total_amount < 40 then '[20, 40)'
                    when order_total_amount < 60 then '[40, 60)'
                    when order_total_amount < 80 then '[60, 80)'
                    when order_total_amount < 120 then '[80, 120)'
                    when order_total_amount < 150 then '[120, 150)'
                    when order_total_amount < 200 then '[150, 200)'
                    when order_total_amount >= 200 then '[200, )'
                    else 'NULL' end)
)

select * 
FROM t00
;



WITH 
-- 订单信息（金额除外）
a AS
(select a.order_id,(case 
                  when a.site_id=2 then 602
                  when a.site_id=1 then 601
                  when a.site_id=0 and a.order_source =5 then 900
                  when a.site_id=0 and a.order_source in (3,4) then 600
                  when a.site_id=0 and a.order_source in (6)   then 700
                  when a.site_id=0 and p3.email in ('jollyalie365@gmail.com','jollycorp2016@gmail.com',
                    'jollychicam@gmail.com','jollycorp20130613@gmail.com','jollycorp.asos@gmail.com',
                    'jollycorpuk@gmail.com','jollychicc@hotmail.com','meetu2016@hotmail.com',
                    'meetu2015@hotmail.com')  then 800
                  else 400
                  end
             )site_id,a.is_shiped,a.order_sn,a.depot_id,a.order_status,a.goods_num,a.pay_status,
round(CAST(a.pay_money AS Float) + CAST(a.surplus AS Float) + CAST(a.order_amount AS Float) ,2) as order_total_amount,
FROM_UNIXTIME(IF(prepare_pay_time = 0,a.pay_time,prepare_pay_time)) as pay_time,
FROM_UNIXTIME(a.no_problems_order_uptime) no_problems_order_uptime,
FROM_UNIXTIME(a.shipping_time) shipping_time,
case when a.pay_id =41 then 'cod' else 'not_cod' end  as  is_cod,
case when p3.source_order_id is not null then 1 else 0 end as is_split
 from default.who_order_info a 
left join default.who_order_user_info p3 on  p3.source_order_id=a.order_id --_brands_zy702
where 1=1
and a.depot_id in (4,5,6,7,14)
and a.pay_status in (1,3)
and IF(prepare_pay_time = 0,a.pay_time,prepare_pay_time)>=  UNIX_TIMESTAMP(DATE_SUB(now(),interval 10 day)) 
)

select  s2.depot_id
        ,s2.num
        ,count(s2.sku_id)
from default.who_wms_goods_need_lock_detail s2  
inner join a ON a.order_id = s2.order_id
where num > 0 
group by s2.depot_id
        ,s2.num
order by s2.depot_id
        ,s2.num;






