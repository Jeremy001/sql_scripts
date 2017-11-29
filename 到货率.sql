/*
内容：采购48小时按需到货率和96小时滚动到货率
 */

select 
sum(send_num)  all_pur_num,
sum(case when pur_type in (1,2,7) then send_num else 0 end ) need_pur_num,
sum(case when pur_type in (3,6,10) then send_num else 0 end ) stock_pur_num,
sum(case when pur_type in (1,2,7) and (qianshou_time-push_time)/3600<48 then send_num else 0 end ) need_pur_num_ok,
sum(case when pur_type in (3,6,10) and (qianshou_time-push_time)/3600<96 then send_num else 0 end ) stock_pur_num_ok
from 
(  select 
 p0.rec_id as demand_id,
 row_number() over(partition by p0.rec_id order by p7.gmt_created desc) row_num ,--按需求发出时间和形成采购单时间
 p0.pur_type, 
    p0.goods_id, p0.sku_id,
    p0.review_status,  --0 未审核 1 已审核 2不用审核
    p0.send_num,
    case when p0.review_status=1 then p0.check_time else p1.gmt_created end as push_time,
 p1.rec_id,
    p1.pur_order_id,
    p1.supp_num,p1.check_num,p1.exp_num, 
 p5.receipt_time,
 p7.deliver_id,
 p7.gmt_created as deliver_time,
 case when ifnull(p5.receipt_time,4102416000)-ifnull(p7.gmt_created,4102416000)>0 then p7.gmt_created else p5.receipt_time end qianshou_time
    from 
     ( SELECT  p1.rec_id, p1.review_status,p1.pur_type,
               p1.goods_id, p1.sku_id, p1.send_num,
               p1.check_time,
               if (p1.pur_order_goods_id IS NOT NULL,p1.pur_order_goods_id, p2.demand_rec_id) as pur_order_goods_rec_id          
        FROM jolly.who_wms_pur_goods_demand p1            
        LEFT JOIN jolly.who_wms_demand_goods_relation p2 ON p1.rec_id=p2.pur_order_goods_rec_id  
        where p1.review_status in (1,2)
      ) p0  
     join jolly.WHO_WMS_PUR_ORDER_GOODS p1 on p0.pur_order_goods_rec_id = p1.rec_id 
     left join ( select t.pur_order_id, max(p5.gmt_created) receipt_time   
     from jolly.who_wms_pur_order_tracking_info t    
     Left Join zydb.ods_wms_pur_deliver_receipt p5 On trim(t.Tracking_No)=trim(P5.Tracking_No)                                                   
     group by t.pur_order_id
      ) p5 on p1.pur_order_id=p5.pur_order_id  
     left join jolly.who_wms_pur_deliver_goods p7 on p0.pur_order_goods_rec_id = p7.pur_order_goods_rec_id 
     where 1=1 
     and case when p0.review_status=1 then p0.check_time else p1.gmt_created end>= unix_timestamp('2017-07-01') 
     and case when p0.review_status=1 then p0.check_time else p1.gmt_created end< unix_timestamp('2017-07-02')
)t
where row_num=1 -- 去重
and qianshou_time-push_time>0
;
