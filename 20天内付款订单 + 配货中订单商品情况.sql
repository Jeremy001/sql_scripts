  --------------查汇总 20天内付款订单 + 配货中订单商品情况
WITH 
t1 AS
(select  to_date(a.pay_time)  '订单付款日', 
case when a.depot_id  =4 then '广州2仓' 
      when a.depot_id in (5) then '东莞1仓' 
      when a.depot_id in (14) then '东莞2仓' 
      when a.depot_id =6  then '香港仓'
        when  a.depot_id =7 then '沙特仓'
          else null end  as '仓库',
count( a.order_id)  as '付款订单数',
count( case when a.is_shiped = 1 then a.order_id else null end)  as '已发运',
count( case when a.is_shiped <>  1 and  a.order_status in (2,3)  then a.order_id else null end)  as '已取消',
count( case when a.is_shiped <>  1 and  a.order_status =1 then a.order_id else null end)  as '待发货',
count( case when is_shiped in (0,4) and a.order_status =1  then a.order_id else null  end)  as '配货中',
sum(case when is_shiped in (0,4) and a.order_status =1  then a.goods_num else 0 end ) '配货中订单总商品数' ,
count( case when is_shiped =5 and a.order_status =1 and a.no_problems_order_uptime < '2012-01-01' or a.no_problems_order_uptime is null  then a.order_id else null  end ) '配货完待审核' ,
count(case when is_shiped in (7,8,6,3,2) and a.order_status not in (2,3)  then a.order_id else null  end) '出库中' ,
sum(case when is_shiped in (0,4) and a.order_status =1  then a.goods_num-s2.still_need_number else 0 end) '锁定商品数',
sum(case when is_shiped in (0,4) and a.order_status =1  then s2.still_need_number  else 0 end) '仍需商品数'
  from 
( ----实时查询订单表当前仍在配货状态中
select  distinct a.order_id,a.is_shiped,a.order_sn,a.depot_id,a.order_status,a.goods_num,a.pay_status,
FROM_UNIXTIME(IF(prepare_pay_time = 0,a.pay_time,prepare_pay_time)) as pay_time,
FROM_UNIXTIME(a.no_problems_order_uptime) no_problems_order_uptime,
FROM_UNIXTIME(a.shipping_time) shipping_time,
case when a.pay_id =41 then 'cod' else 'not_cod' end  as  is_cod,
case when p3.source_order_id is not null then 1 else 0 end as is_split
 from default.who_order_info a 
left join default.who_order_user_info p3 on  p3.source_order_id=a.order_id --_brands_zy702
where 1=1
 --订单基础表只限制付款时间
and a.depot_id in (4,5,6,7,14)
and IF(prepare_pay_time = 0,a.pay_time,prepare_pay_time)<=  UNIX_TIMESTAMP(DATE_SUB(now(),interval 0 day)) 
and IF(prepare_pay_time = 0,a.pay_time,prepare_pay_time)> UNIX_TIMESTAMP(DATE_SUB(now(),interval 12  day)) 
) a
left join 
( ----实时匹配当前配货中的订单的 锁定商品件数 和 需要商品件数
 select  s2.order_id,  sum(num) still_need_number
 from  jolly_oms.who_wms_goods_need_lock_detail s2  ----jolly_oms  who_wms_goods_need_lock_detail:订单采购商品审核锁定明细表 共15个字段
 group by order_id
)  s2 on  a.order_id=s2.order_id  
where 1=1
and a.is_split=0
 group by to_date(a.pay_time)  ,
case when a.depot_id  =4 then '广州2仓' 
      when a.depot_id in (5) then '东莞1仓' 
      when a.depot_id in (14) then '东莞2仓' 
      when a.depot_id =6  then '香港仓'
        when  a.depot_id =7 then '沙特仓'
          else null end, to_date(a.pay_time) 
  order by 
case when a.depot_id  =4 then '广州2仓' 
      when a.depot_id in (5) then '东莞1仓' 
      when a.depot_id in (14) then '东莞2仓' 
      when a.depot_id =6  then '香港仓'
        when  a.depot_id =7 then '沙特仓'
          else null end, to_date(a.pay_time) 
 )

SELECT * 
FROM t1
WHERE 仓库 = '香港仓'
LIMIT 10;
