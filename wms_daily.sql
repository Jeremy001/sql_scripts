
/*
ldate_start=`date +"%Y-%m-%d" -d'-1 day'`
ldate_end=`date +"%Y-%m-%d"`
ldate_end2=`date +"%Y-%m-%d" -d'-3 day'`
 */

-- 附件2：long_time_complete_unshiped
with t1 as
(select t.order_sn,t.pay_id,
case when t.depot_id=2 then '广州仓' 
     when t.depot_id=4 then '广州2仓'
     when t.depot_id=5 then '东莞仓'
     when t.depot_id=6 then '香港仓'
 end  仓库ID
,
case when t.is_shiped=3 then '待发运' 
     when t.is_shiped=6 then '待打包' 
     when t.is_shiped=7 then '待拣货'
     when t.is_shiped=8 then '拣货中'
else '仓库不可操作'
end  发运状态
,
t.is_shiped,
t.is_problems_order 是否已标非,
FROM_UNIXTIME(t.prepare_pay_time) prepare_pay_time, 
FROM_UNIXTIME(t.pay_time) pay_time,
FROM_UNIXTIME(t.shipping_time) 发运时间,
FROM_UNIXTIME(t.order_pack_time) 打包完成时间,
FROM_UNIXTIME(拣货完成时间) 拣货完成时间,
FROM_UNIXTIME(t.no_problems_order_uptime) 标非时间, 
FROM_UNIXTIME(定时可捡货时间) 定时可捡货时间,
FROM_UNIXTIME(if(t.pay_id=41,t.prepare_pay_time,t.pay_time)) as 付款时间,
 FROM_UNIXTIME(定时可捡货时间) as 可捡货时间,
FROM_UNIXTIME(配货完成时间) 配货完成时间,
datediff(now(), FROM_UNIXTIME(if(t.pay_id=41,t.prepare_pay_time,t.pay_time))) 付款日期距现在的天数
 from jolly.who_order_info t 
left  join (select l.order_sn,max(u.finish_time) 拣货完成时间 from jolly.who_wms_picking_goods_detail  l 
inner join jolly.who_wms_picking_info u
on l.picking_id=u.picking_id
group by l.order_sn) l
on t.order_sn=l.order_sn
left  join (select k.order_sn,max(k.gmt_created)  定时可捡货时间 from jolly.who_wms_outing_stock_detail k
group by k.order_sn) k
on t.order_sn=k.order_sn
left join (select r.order_sn,max(r.last_modified_time) 配货完成时间 from jolly.who_wms_goods_need_lock_detail r
where r.status=2
group by r.order_sn) r
on t.order_sn=r.order_sn
where if(t.pay_id=41,t.prepare_pay_time,t.pay_time)<UNIX_TIMESTAMP('2017-09-28')    -- 三天前
-- where if(t.pay_id=41,t.prepare_pay_time,t.pay_time)<UNIX_TIMESTAMP('$ldate_end2')
and t.shipping_time < UNIX_TIMESTAMP('2011-01-01')
and t.order_status=1
and t.is_shiped!=1
and t.is_check=1
and t.pay_status=1
and t.depot_id in (4,5,6)
and t.is_shiped in (3,6,5,7,8)
and t.is_problems_order in(0,2)
)

select * 
from t1
limit 10;
/*
into outfile '$DATA_DIR/long_time_complete_picking_unshipped.csv'
fields terminated by ','
optionally enclosed by '≌'
lines terminated by '|@@|\n'
;*/


-- 附件1：shipping_order_detail
with t2 as
(select t.order_sn,t.depot_id,t.is_shiped,case when s.returned_order_sn is not null then 1 else 0 end 是否为异常订单,
FROM_UNIXTIME(t.prepare_pay_time) prepare_pay_time, 
FROM_UNIXTIME(t.pay_time) pay_time,
FROM_UNIXTIME(t.shipping_time) 发运时间,
FROM_UNIXTIME(t.order_pack_time) 打包完成时间,
FROM_UNIXTIME(拣货完成时间) 拣货完成时间,
FROM_UNIXTIME(t.no_problems_order_uptime) 标非时间, 
FROM_UNIXTIME(定时可捡货时间) 定时可捡货时间,
FROM_UNIXTIME(if(t.pay_id=41,t.prepare_pay_time,t.pay_time)) as 付款时间,
 FROM_UNIXTIME(if(t.no_problems_order_uptime>=定时可捡货时间,t.no_problems_order_uptime,定时可捡货时间)) as 可捡货时间,
FROM_UNIXTIME(配货完成时间) 配货完成时间
 from jolly.who_order_info t 
left  join (select l.order_sn,max(u.finish_time) 拣货完成时间 from jolly.who_wms_picking_goods_detail  l 
inner join jolly.who_wms_picking_info u
on l.picking_id=u.picking_id
group by l.order_sn) l
on t.order_sn=l.order_sn
left  join (select k.order_sn,max(k.gmt_created)  定时可捡货时间 from jolly.who_wms_outing_stock_detail k
group by k.order_sn) k
on t.order_sn=k.order_sn
left join 
(select r.order_sn,max(r.last_modified_time) 配货完成时间 from jolly.who_wms_goods_need_lock_detail r
where r.status=2
group by r.order_sn) r
on t.order_sn=r.order_sn
left join (select  distinct s.returned_order_sn from jolly.who_wms_returned_order_info s
where s.returned_order_status in (3,6,7,8)
and s.return_reason in (10,14)
and s.returned_time < UNIX_TIMESTAMP('2017-09-30')
-- and s.returned_time < UNIX_TIMESTAMP('$ldate_end')
) s
on t.order_sn=s.returned_order_sn
where t.shipping_time < UNIX_TIMESTAMP('2017-09-30')
AND t.shipping_time >= UNIX_TIMESTAMP('2017-09-29')
-- where t.shipping_time < UNIX_TIMESTAMP('$ldate_end')
-- AND t.shipping_time >= UNIX_TIMESTAMP('$ldate_start')
and t.is_shiped=1
)
select * 
from t2 
limit 10;



/*
into outfile '$DATA_DIR/shipping_order_detail.csv'
fields terminated by ','
optionally enclosed by '≌'
lines terminated by '|@@|\n'
;*/


-- ====================================================================

select t.order_sn,t.pay_id,
case when t.depot_id=5 then '东莞仓' 
     when t.depot_id=4 then '广州2仓'
     when t.depot_id=6 then '香港仓'

      end  仓库ID
,
case when t.is_shiped=3 then '待发运' 
     when t.is_shiped=6 then '待打包' 
     when t.is_shiped=7 then '待拣货'
     when t.is_shiped=8 then '拣货中'
else '仓库不可操作'
end  发运状态
,
FROM_UNIXTIME(t.shipping_time) 发运时间,
FROM_UNIXTIME(t.order_pack_time) 打包完成时间,
FROM_UNIXTIME(拣货完成时间) 拣货完成时间,
FROM_UNIXTIME(t.no_problems_order_uptime) 标非时间, 
FROM_UNIXTIME(定时可捡货时间) 定时可捡货时间,
FROM_UNIXTIME(if(t.pay_id=41,t.prepare_pay_time,t.pay_time)) as 付款时间,
FROM_UNIXTIME(if(t.no_problems_order_uptime>=定时可捡货时间,t.no_problems_order_uptime,定时可捡货时间)) as 可捡货时间,
FROM_UNIXTIME(配货完成时间) 配货完成时间
 from who_order_info t  
left  join (select l.order_sn,max(u.finish_time) 拣货完成时间 
from who_wms_picking_goods_detail  l 
inner join who_wms_picking_info u 
on l.picking_id=u.picking_id
group by l.order_sn) l
on t.order_sn=l.order_sn
left  join (select k.order_sn,max(k.gmt_created)  定时可捡货时间 from 
who_wms_outing_stock_detail k 
group by k.order_sn) k
on t.order_sn=k.order_sn
left join (select r.order_sn,max(r.last_modified_time) 配货完成时间 
from who_wms_goods_need_lock_detail r 
where r.status=2
group by r.order_sn) r
on t.order_sn=r.order_sn
where date_format(now(),'%Y-%m-%d 00:00:00')-FROM_UNIXTIME(拣货完成时间)>=1
and t.shipping_time < UNIX_TIMESTAMP('2011-01-01')
and t.order_status=1
and t.is_shiped!=1
and t.is_check=1
and t.pay_status=1
and t.depot_id in (5,4,6)
and t.is_shiped in (3,5,6)
and t.is_problems_order in(0,2)
into outfile '$DATA_DIR/24h_noshipping_detail.csv'
fields terminated by ','
optionally enclosed by '≌'
lines terminated by '|@@|\n'
;
