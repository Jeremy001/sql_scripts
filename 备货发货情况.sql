  create table  if not exists zybiro.yf_spring_supp_detail
  as
 select     to_date(t.create_time) create_date, t.depot_id,
 sum(t.supp_num) create_supp_num,
 sum(case when t.check_time >'2017-01-01 00:00:00' then t.supp_num  else 0 end ) check_supp_num,
 sum(t.send_num)  send_num,
 sum(case when t.check_time >'2017-01-01 00:00:00' then t.oos_num  else 0 end) check_oos_num,
 sum(case when t.reciept_time >'2017-01-01 00:00:00' then t.supp_num -oos_num  else 0 end ) reciept_num
 from 
( 
select  row_number () over(partition by p3.rec_id order by p10.gmt_created desc) row_num ,
 p3.depot_id, p3.gmt_created create_time ,p3.check_time ,p3.supp_num, p3.oos_num,p3.pur_type,p3.review_status,p3.send_num,
 p9.supp_name, p5.pur_order_sn, from_unixtime(p5.gmt_created)  pur_send,
 p8.tracking_no,
 from_unixtime(p9.gmt_created)  reciept_time,
 p10.gmt_created   deliver_on    
                                                           
from 
 (
    SELECT  p1.rec_id ,p1.pur_type, P1.supp_num,p1.oos_num,p1.depot_id,p1.send_num,p1.review_status,
            from_unixtime(p1.gmt_created) gmt_created , ---按需求生成时间
            from_unixtime(p1.check_time) check_time , ---按需求审核时间
           p2.pur_order_goods_rec_id 
     FROM jolly_spm.jolly_spm_pur_goods_demand  p1            
     LEFT JOIN jolly_spm.jolly_spm_pur_goods_demand_relation   p2    on  p2.demand_rec_id = p1.rec_id      
   where  1=1
   and p1.pur_type = 11
   and p1.gmt_created  >= unix_timestamp(concat(to_date(date_sub(from_unixtime(unix_timestamp(current_timestamp(),'yyyyMMdd')),10)),' 00:00:00'))
  )p3                                                       
 
 Left Join   jolly_spm.jolly_spm_pur_order_goods          p4 on p3.pur_order_goods_rec_id= p4.rec_id 
 left join  jolly_spm.jolly_spm_pur_order_info            p5 on p4.pur_order_id= p5.pur_order_id 
 left join  jolly.who_wms_delivered_order_info            p7 on p5.pur_order_sn=p7.delivered_order_sn  --来源类型:1-批发订单作业单,2-采购单,3-调拨入库单
 left join  jolly.who_wms_delivered_order_tracking_info   p8 on p8.delivered_order_id =p7.delivered_order_id   --采购单物流信息表
 left join jolly.who_wms_delivered_receipt_info           p9 on trim(p9.tracking_no) =trim(p8.tracking_no)     --到货签收单
 left  join
 ( 
  select s9.delivered_order_sn ,max(s10.gmt_created) gmt_created 
    from  jolly.who_wms_on_shelf_info s10 
     left join jolly.who_wms_on_shelf_goods_price s9  on s9.on_shelf_id=s10.on_shelf_id 
    group by s9.delivered_order_sn
 ) p10         on  p7.delivered_order_sn=p10.delivered_order_sn   
) t
    where row_num=1
    group by  to_date(t.create_time) , t.depot_id
;