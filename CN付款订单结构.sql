--------------- 2、CN付款订单结构
SELECT
  a.paytime 
  ,count(distinct a.order_id)as paid_ordernum_CN
  ---,sum(a.org_total_money)as org_orderrevenue_CN
  ---,sum(e.org_goodsnum) as org_goodsnum_CN
  ,sum(e.org_goodsnum)/count(distinct a.order_id) as order_goodsnum_CN
FROM
  (select regexp_replace(substr(case when a.pay_id=41 then from_unixtime(a.prepare_pay_time) else from_unixtime(a.pay_time) end,1,10 ),'-','') as paytime,a.order_id,a.pay_money+a.surplus+a.order_amount as org_total_money 
  from jolly.who_order_info a
   where regexp_replace(substr(case when a.pay_id=41 then from_unixtime(a.prepare_pay_time) else from_unixtime(a.pay_time) end,1,10 ),'-','')>='${startdate}' 
   and a.pay_status=1 and a.depot_id in (4,5,6,14) )a
inner join 
    (select e.order_id,sum(e.original_goods_number)as org_goodsnum 
     from jolly.who_order_goods e
     where regexp_replace(substr(from_unixtime(e.gmt_created),1,10),'-','')>='${startdate}' 
     group by e.order_id)e
on a.order_id=e.order_id
left join 
    (select g.source_order_id from jolly.who_order_user_info g where g.source_order_id<>0)g
on a.order_id=g.source_order_id
WHERE
    g.source_order_id is null
group by a.paytime
order by a.paytime;