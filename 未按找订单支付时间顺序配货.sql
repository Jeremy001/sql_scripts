with 
-- 20-23号未配货sku
t1 AS
(select p1.order_id
        ,p1.order_sn
        ,p1.sku_id
        ,p1.num
        ,p1.org_num
        ,p1.depot_id
        ,(case when pay_id=41 then p2.pay_time else p2.result_pay_time end) AS pay_time
from jolly.who_wms_goods_need_lock_detail p1
inner join zydb.dw_order_sub_order_fact p2 on p1.order_id = p2.order_id
where p1.num >= 1
and (case when pay_id=41 then p2.pay_time else p2.result_pay_time end) >= '2017-11-20'
and (case when pay_id=41 then p2.pay_time else p2.result_pay_time end) < '2017-11-24'
and p2.order_status = 1
and p2.pay_status IN (1, 3)
),
-- 24号及以后已配货sku
t2 AS
(select p1.order_id
        ,p1.order_sn
        ,p1.sku_id
        ,p1.num
        ,p1.org_num
        ,p1.depot_id
        ,(case when pay_id=41 then p2.pay_time else p2.result_pay_time end) AS pay_time
        ,p1.last_modified_time
from jolly.who_wms_goods_need_lock_detail p1
inner join zydb.dw_order_sub_order_fact p2 on p1.order_id = p2.order_id
where p1.last_modified_time > 0
and (case when pay_id=41 then p2.pay_time else p2.result_pay_time end) >= '2017-11-24'
and p2.order_status = 1
and p2.pay_status IN (1, 3)
)

select t1.depot_id
        ,count(distinct t1.order_id) AS order_num
        ,sum(t1.num) AS num
from t1
inner join t2 on t1.depot_id = t2.depot_id and t1.sku_id = t2.sku_id 
group by t1.depot_id
order by t1.depot_id
;


select t1.*
        ,t2.order_id AS order_id2
        ,t2.order_sn AS order_sn2
        ,t2.sku_id AS sku_id2
        ,t2.num AS num2
        ,t2.org_num AS org_num2
        ,t2.pay_time AS pay_time2
        ,from_unixtime(t2.last_modified_time) AS last_modified_time2
from t1
inner join t2 on t1.depot_id = t2.depot_id and t1.sku_id = t2.sku_id 
where t1.depot_id = 5
limit 10
;


select t1.*
        ,t2.order_id AS order_id2
        ,t2.order_sn AS order_sn2
        ,t2.sku_id AS sku_id2
        ,t2.num AS num2
        ,t2.org_num AS org_num2
        ,t2.pay_time AS pay_time2
        ,from_unixtime(t2.last_modified_time) AS last_modified_time2
from t1
inner join t2 on t1.depot_id = t2.depot_id and t1.sku_id = t2.sku_id 
;




-- 东莞仓
select count(*)
from t1
where t1.depot_id = 5
and t1.sku_id in (select sku_id from t2 where t2.depot_id = 5)
;


select *
from default.who_wms_goods_need_lock_detail s2  
limit 10;


-- order_id:34833569; sku_id:8454472, 6356532
-- JARI17112422204944432400
-- JARA17112703312120455300


select * 
from jolly.who_wms_goods_need_lock_detail
where order_id = 34833569
and num > 0;



SELECT from_unixtime(max(cod_create_time))
FROM default.who_order_user_info
--LIMIT 10;
;


select  depot_id
        ,num
        ,count(sku_id)
from default.who_wms_goods_need_lock_detail s2  
where num > 0 
group by depot_id
        ,num
order by depot_id
        ,num;



SELECT order_total_amount
        ,order_amount
        ,order_amount_no_bonus
FROM zydb.dw_order_sub_order_fact
LIMIT 100
;


SELECT *
from default.who_order_info a 
LIMIT 10;


SELECT *
FROM zydb.dw_order_node_time
LIMIT 100
;


SELECT max(pay_time)
from zydb.dw_order_node_time
;



from_unixtime(max(last_modified_time))

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
        
        
        
        
select *
from default.who_wms_goods_need_lock_detail s2  
where s2.last_modified_time is null
limit 10;


select *
from jolly.who_wms_goods_need_lock_detail
where last_modified_time = 0
limit 10
;
