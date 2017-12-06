


WITH 
t1 AS
(select s.pay_date,s.pay_duration,s.depot,
  s.order_id,s.order_sn,s.site_id,
  s.goods_num order_goods_number,
  s.pay_time,s.no_problems_order_uptime,s.shipping_time,s.is_shiped,
 s1.goods_id,s1.sku_id,s1.goods_number,FROM_UNIXTIME(s2.last_modified_time) last_modified_time,
 s2.org_num,
 s2.num    allo_need_num,
 s2.oos_num allo_lock_oos,
 FROM_UNIXTIME(s3.gmt_created)  allocate_demand_start_time,
 s6.allocate_order_sn,s6.from_depot_id,s6.to_depot_id,s6.status  allocate_order_status,
 --1-待拣货,2-已取消,3-拣货中,4-拣货完成,5-打包完成,6-已发货,7-部分收货,8-全部收货
 FROM_UNIXTIME(s6.gmt_created) allo_info_time,
 FROM_UNIXTIME(s6.out_time) allocate_order_outdepot,
 FROM_UNIXTIME(s6.arrive_time) allocate_order_arriveindepot,--到货时间:(到货签收时间或不签收时的第一件商品质检时间)
 s7.check_status deliver_check_status,--验收状态: 0-未验收,1-已验收,2-部分验收
   FROM_UNIXTIME(s7.gmt_created) allo_deliver_time,
   FROM_UNIXTIME(s7.first_check_time)  allo_first_check_time ,
  FROM_UNIXTIME(s7.finish_check_time)  allo_finish_check_time ,
 FROM_UNIXTIME(s10.gmt_created)  pur_goods_onself_time 
 from zybiro.yf_mysql_pay_10orders_3a s
 inner join default.who_order_goods s1 on s.order_id=s1.order_id 
 inner  join default.who_wms_goods_need_lock_detail s2  on  s.order_id=s2.order_id  and cast(s1.sku_id as int)=s2.sku_id
 left  join  default.jolly_spm_allocate_goods_demand s3  on s.order_id=s3.order_id and cast(s1.sku_id as int)=s3.sku_id                                                                                   
 inner join   default.jolly_spm_allocate_demand_goods_relation  s4 on   s4.allocate_goods_demand_rec_id = s3.rec_id
 left  join  default.jolly_spm_allocate_order_goods  s5 on s4.allocate_order_goods_rec_id= s5.rec_id
 left  join default.jolly_spm_allocate_order_info s6  on s5.allocate_order_id=s6.allocate_order_id
 left join  default.who_wms_delivered_order_info  s7 on s6.allocate_order_sn=s7.delivered_order_sn  
 left  join
 ( 
  select s9.delivered_order_sn ,max(s10.gmt_created) gmt_created 
    from  default.who_wms_on_shelf_info s10 
     left join default.who_wms_on_shelf_goods_price s9  on s9.on_shelf_id=s10.on_shelf_id 
    group by s9.delivered_order_sn
    ) s10         on  s7.delivered_order_sn=s10.delivered_order_sn  
 where 1=1
 )


select *
from t1
limit 10;