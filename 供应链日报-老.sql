

insert overwrite table zydb.rpt_scm_order_tmp   
Select
p1.site_id, 
p1.add_time, 
p1.pay_time, 
p1.user_id,  
p1.pay_id, 
p1.shipping_id, 
'' , --p1.session_id
p1.order_id, 
p1.order_sn, 
p1.goods_amount, 
p1.order_total_amount, 
p1.pay_status, 
'' , --p1.search_keyword
 0 ,--p1.goods_number 
p1.result_pay_time, 
p1.is_shiped, 
p1.order_source, 
p1.shipping_time, 
p1.no_problems_order_uptime, 
p1.order_pack_time, 
p1.depod_id, 
p1.is_cod_vip,
p2.order_status ,
p2.is_check 
From zydb.dw_order_sub_order_fact p1
Inner join 
(
 select a.order_id,a.order_status,is_check,a.add_time,
        (case
             when a.language_site=11 then 601
             when order_source in (5) then 900
             when order_source in (3,4) then 600
             when order_source in (6)   then 700
             when b.email in ('jollyalie365@gmail.com','jollycorp2016@gmail.com','jollychicam@gmail.com',
                          'jollycorp20130613@gmail.com','jollycorp.asos@gmail.com','jollycorpuk@gmail.com',
                          'jollychicc@hotmail.com','meetu2016@hotmail.com','meetu2015@hotmail.com'
                          )  then 800
             else 400
             end
       ) site_id
 from 
 (
     select * from jolly.who_order_info
     union all 
     select * from jolly.who_order_info_history
 ) a
 left join 
 (
     select * from secure.who_order_user_info 
     union all 
     select * from secure.who_order_user_info_history
 )b
  on a.order_id=b.order_id
)p2
on p1.order_id=p2.order_id
and p1.site_id=p2.site_id


Where P1.Site_Id In(400,600,700,800,900)
and p1.pop_id=0
and p2.add_time>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),180),'yyyy-MM-dd')
and p2.add_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
and p1.add_time>=date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),180)
 and p1.add_time<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
;


 

insert overwrite table  zydb.rpt_supply_tmp2 
Select 
cast(p1.order_id as bigint),
P2.SKU_ID,
cast(p1.pay_id as bigint),
p1.pay_time,
p1.result_pay_time,
from_unixtime(p2.check_time),
from_unixtime(p2.last_modified_time) ,
from_unixtime(p3.gmt_created) push_time, --推送时间
from_unixtime(p4.gmt_created) pur_time, --采购单时间,
from_unixtime(p5.gmt_created) recepit_time, --到货签收时间,
from_unixtime(P6.gmt_created) deliver_time, --到货质检完成时间,
--p7.gmt_created 上架时间
from_unixtime(p7.finish_time) onself_time--上架时间
,p3.pur_order_goods_rec_id
,p2.org_num
,p2.oos_num
,p3.source_rec_id
,review_status
From zydb.rpt_scm_order_tmp p1
Inner Join jolly.who_wms_goods_need_lock_detail  p2
On P1.Order_Id=P2.Order_Id
Left join (
     SELECT 
          p1.rec_id ,
          p1.review_status,
          p1.pur_order_goods_id,
          p1.check_time gmt_created , --推送时间
          p2.demand_rec_id,
          source_rec_id,
          CASE
              WHEN p1.pur_order_goods_id IS NOT NULL --供应商门户
                 THEN p1.pur_order_goods_id
              ELSE p2.demand_rec_id  --线下采购单
          END pur_order_goods_rec_id
   FROM  jolly.who_wms_pur_goods_demand  p1
   LEFT JOIN  jolly.who_wms_demand_goods_relation  p2 
   ON p1.rec_id=p2.pur_order_goods_rec_id
   where to_date(from_unixtime(p1.check_time))<>'1970-01-01'
   )p3 on p2.source_rec_id =p3.rec_id
Left Join  jolly.who_wms_pur_order_goods   p4
   on p3.pur_order_goods_rec_id=p4.rec_id
   --And p4.site_id=400
   and to_date(from_unixtime(p4.gmt_created))<>'1970-01-01'
 
left join (select t.pur_order_id,max(p5.gmt_created)  gmt_created   
from jolly.who_wms_pur_order_tracking_info  t    
Left Join jolly.who_wms_pur_deliver_receipt  p5
   On trim(t.Tracking_No)=trim(P5.Tracking_No)
   and to_date(from_unixtime(p5.gmt_created))<>'1970-01-01'
   where t.gmt_created<=unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
 group by t.pur_order_id
)p5
on p4.pur_order_id=p5.pur_order_id
   and to_date(from_unixtime(p5.gmt_created))<>'1970-01-01'
LEFT JOIN jolly.who_wms_pur_deliver_goods  p6
   ON p3.pur_order_goods_rec_id=p6.pur_order_goods_rec_id  --有疑问 p4.rec_id
   and to_date(from_unixtime(p6.gmt_created))<>'1970-01-01'
left join jolly.who_wms_pur_deliver_info  p7
  on p6.deliver_id=p7.DELIVER_ID
  and to_date(from_unixtime(p7.finish_time))<>'1970-01-01'
Where  to_date(p1.shipping_time)=to_Date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
  And p1.is_shiped=1
ANd P2.org_num-P2.oos_num>0 
And source_type=1 
;




 

insert overwrite table zydb.rpt_supply_tmp3 
select 
    order_id
    ,sku_id
    ,pay_id
    ,pay_time
    ,result_pay_time
    ,check_time
    ,last_modified_time
    ,push_time
    ,buy_time
    ,receipt_time
    ,quarly_check_time
    ,onself_time
    ,pur_order_goods_rec_id
    ,org_num
    ,oos_num
    ,source_rec_id
    ,review_status
    ,row_number()over(partition by t.source_rec_id order by t.onself_time desc) row_num 
from rpt_supply_tmp2 t 
where t.pur_order_goods_rec_id is not null ;



 

insert overwrite  table zydb.rpt_supply_chain_table
select
    to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))) data_date
    ,ship_paid_order_num
    ,shipping_total_time
    ,0 --cod_receipt_order_num
    ,0 --cod_receipt_total_time
    ,paid_order_num_last
    ,goods_num_last
    ,out_stock_orders_num_last
    ,out_stock_goods_num_last
    ,abnormal_order
    ,pay_push_duration
    ,push_receipt_duration
    ,receipt_quality_duration
    ,quality_onshelf_duration
    ,tc_purchase_goods_rate
    ,nagao_purchase_goods_rate
    ,nagao_ros_goods_rate
    ,nagao_pks_order_rate
    ,onshelf_outstock_duration
    ,picking_duration
    ,package_duration
    ,shipping_duration
    ,lag_review_order_rate
    ,nagao_review_order_rate

from
(Select
      '${data_date}' data_date
      ,count(distinct Case When to_date(P1.Shipping_Time)=to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))) Then p1.order_id Else null end)  ship_paid_order_num 
      ,sum(case when (case when to_date(p1.shipping_time)=to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))) then 
      (unix_timestamp(p1.shipping_time) - 
      unix_timestamp((Case  When P1.Pay_Id=41 and to_date(p1.pay_time)<>'1970-01-01'
      then p1.pay_time 
        else 
          case when to_date(p1.pay_time)<>'1970-01-01' then 
           p1.result_pay_time 
           end
        end )))/3600/24
      else 0
        end)>0 then (case when to_date(p1.shipping_time)=to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))) then 
      (unix_timestamp(p1.shipping_time) - 
      unix_timestamp((Case  When P1.Pay_Id=41 and to_date(p1.pay_time)<>'1970-01-01'
      then p1.pay_time 
        else 
          case when to_date(p1.pay_time)<>'1970-01-01' then 
           p1.result_pay_time 
           end
        end )))/3600/24
      else 0
        end) else 0 end ) shipping_total_time
      From zydb.rpt_scm_order_tmp p1
      Where to_date(shipping_time)=to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
) p1

inner join
(
        select '${data_date}' data_date,count(distinct a.order_id) paid_order_num_last,
      sum(b.goods_number)+sum(nvl(c.oos_num,0)) goods_num_last,
      count(distinct c.order_id) out_stock_orders_num_last,
      sum(nvl(c.oos_num,0))   out_stock_goods_num_last
      
      from zydb.rpt_scm_order_tmp a
    inner join 
    (
     select c.*,
            (case
                 when a.language_site=11 then 601
                 when order_source in (5) then 900
                 when order_source in (3,4) then 600
                 when order_source in (6)   then 700
                 when b.email in ('jollyalie365@gmail.com','jollycorp2016@gmail.com','jollychicam@gmail.com',
                              'jollycorp20130613@gmail.com','jollycorp.asos@gmail.com','jollycorpuk@gmail.com',
                              'jollychicc@hotmail.com','meetu2016@hotmail.com','meetu2015@hotmail.com'
                              )  then 800
                 else 400
                 end
           ) site_id
     from jolly.who_order_info a
     left join secure.who_order_user_info b
      on a.order_id=b.order_id
     inner join jolly.who_order_goods  c
      on a.order_id=c.order_id
    ) b 
    on a.order_id=b.order_id
    and a.site_id=b.site_id
    and b.gmt_created>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),180),'yyyy-MM-dd')  
    and b.gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')  
    left join  jolly.who_wms_order_oos_log  c 
    on b.rec_id=c.order_goods_rec_id
    where to_date(a.pay_time)=date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),7) 
    and a.pay_status in (1,3)
    
)p3

on p1.data_date=p3.data_date
inner join (
    Select
      '${data_date}' data_date
      ,Count(Distinct p1.order_id) abnormal_order --发货异常订单数
    From zydb.rpt_scm_order_tmp p1
    Inner Join  jolly.who_wms_returned_order_info  p2
    on p1.order_id=p2.returned_order_id
    --And p2.returned_order_status  In(3,6,7,8) --modified by hanshizhong 20170308 WMS调整退货标识 
    And p2.return_reason In(24)
    Where p1.is_shiped=1
    And to_Date(p1.Shipping_Time)=to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
)p4 
on p1.data_date=p4.data_date
inner join (
    select '${data_date}' data_date
    ,sum(((unix_timestamp(push_time)-unix_timestamp(pay_time))/3600/24)*(t.org_num-t.oos_num))/sum(t.org_num-t.oos_num) 
    pay_push_duration,
      sum(((unix_timestamp(receipt_time)-unix_timestamp(push_time))/3600/24)*(t.org_num-t.oos_num))/sum(t.org_num-t.oos_num) 
      push_receipt_duration,
    sum(((unix_timestamp(deleve_time)-unix_timestamp(receipt_time))/3600/24)*(t.org_num-t.oos_num))/sum(t.org_num-t.oos_num) 
    receipt_quality_duration,
    sum(((unix_timestamp(onself_time)-unix_timestamp(deleve_time))/3600/24)*(t.org_num-t.oos_num))/sum(t.org_num-t.oos_num) 
    quality_onshelf_duration,
    sum(case when (unix_timestamp(receipt_time)-unix_timestamp(pay_time))/3600/24<=1.8 then (t.org_num-t.oos_num) else 0 end)/sum(t.org_num-t.oos_num) tc_purchase_goods_rate,
    sum(case when (unix_timestamp(receipt_time)-unix_timestamp(pay_time))/3600/24>3 then (t.org_num-t.oos_num) else 0 end)/sum(t.org_num-t.oos_num) nagao_purchase_goods_rate,
    sum(case when (unix_timestamp(onself_time)-unix_timestamp(receipt_time))/3600/24>0.5 then (t.org_num-t.oos_num) else 0 end)/sum(t.org_num-t.oos_num) nagao_ros_goods_rate

     from 
    (

    select t.org_num,t.oos_num,
      case when t.pay_id=41 then t.pay_time else t.result_pay_time end pay_time,
       push_time,
        t.receipt_time,t.deleve_time,t.onself_time
     from 
     (
         select order_id, 
              sku_id, 
              pay_id, 
              pay_time, 
              result_pay_time, 
              check_time, 
              last_modified_time, 
              --推送时间, 
              buy_time , 
              deleve_time , 
              onself_time , 
              pur_order_goods_rec_id, 
              org_num, 
              oos_num, 
              source_rec_id, 
              review_status, 
              row_num
              ,case when (unix_timestamp(receipt_time)-unix_timestamp(deleve_time))/3600/24>0 then deleve_time else 
                receipt_time end receipt_time
              ,case when t.review_status=2 then t.buy_time else t.push_time end  push_time 
         from zydb.rpt_supply_tmp3 t
     )t
    where t.row_num=1
    and t.push_time>'2012-01-01'
    and (unix_timestamp(t.receipt_time)-unix_timestamp(push_time))>0
    and (unix_timestamp(deleve_time)-unix_timestamp(t.receipt_time))>0
    and (unix_timestamp(onself_time)-unix_timestamp(deleve_time))>0
    and (unix_timestamp(push_time)-unix_timestamp(pay_time))>0

 )t
)p5
on p1.data_date=p5.data_date
inner join (
    Select
        '${data_date}' data_date  
        ,sum(nagao_review_orders)/count(1) nagao_review_order_rate --审核时长>3.5d订单
        ,sum(nagao_pks_orders)/count(1) nagao_pks_order_rate --可拣货至发运时长>1d订单
        ,sum(onshelf_outstock_duration)/count(case when onshelf_outstock_duration*24*60*60>20 then 1 end) onshelf_outstock_duration --订单审核等待时长
        ,sum(picking_duration)/count(case when picking_duration>0 then 1 end) picking_duration --拣货时长
        ,sum(package_duration)/count(case when package_duration>0 then 1 end) package_duration --发运时长
        ,sum(shipping_duration)/count(case when shipping_duration>0 then 1 end) shipping_duration --发运时长
        ,sum(lag_review_orders)/count(1) lag_review_order_rate --滞后审核订单
    From (
      Select
          p1.order_sn
          ,case when (unix_timestamp(no_problems_order_uptime)-unix_timestamp((Case When pay_id=41 Then P1.pay_time Else P1.Result_Pay_Time
          end)))/3600/24>3.5  Then 1 Else 0 
          end nagao_review_orders 
          ,P2.last_modified_time 
          ,Case when unix_timestamp(P1.no_problems_order_uptime)>P2.last_modified_time Then 1 Else 0
          end lag_review_orders
          ,p3.Gmt_Created  
          ,p4.gmt_created  gmt_created1
          ,p1.order_pack_time 
          ,p1.Shipping_Time 
          ,case when (unix_timestamp(Shipping_Time)-p3.Gmt_Created)/3600/24 >1 then 1 else 0 end nagao_pks_orders --可拣货至发运时长
          ,(p3.Gmt_Created -P2.last_modified_time)/3600/24  onshelf_outstock_duration --订单审核等待时长
          ,(p4.gmt_created - p3.Gmt_Created)/3600/24  picking_duration --拣货时长
          ,(unix_timestamp(p1.order_pack_time)-p4.gmt_created)/3600/24 package_duration --打包时长
          ,(unix_timestamp(p1.Shipping_Time)-unix_timestamp(p1.order_pack_time))/3600/24  shipping_duration --发运时长
      From 
      (
          Select P1.order_sn,is_shiped,p1.pay_id
              ,max(p1.no_problems_order_uptime) no_problems_order_uptime --订单标非时间
              ,max(P1.Shipping_Time) Shipping_Time --发运时间
              ,max(p1.order_pack_time ) order_pack_time --打包时间
              ,max(p1.Result_Pay_Time) Result_Pay_Time 
              ,max(P1.Pay_Time) Pay_Time
          From zydb.rpt_scm_order_tmp p1
          Where p1.pay_status In (1,3)
          and to_date(order_pack_time)<>'1970-01-01'
          Group By order_sn,is_shiped,pay_id
      )P1
      Left Join 
      (
          Select
            p2.order_sn
            ,max(p2.last_modified_time) last_modified_time --配货完成时间
          From  jolly.who_wms_goods_need_lock_detail  p2
          WHERE P2.last_modified_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
          and  to_date(from_unixtime((last_modified_time)))<>'1970-01-01'
          Group by order_sn
      )P2
      On P1.order_sn=P2.order_sn
      Left join (
          Select
          P3.order_sn,max(P3.Gmt_Created) Gmt_Created --定时任务出库时间
          From  jolly.who_wms_outing_stock_detail  P3
          WHERE P3.GMT_CREATED<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
          and to_date(from_unixtime(Gmt_Created))<>'1970-01-01'
          Group By order_sn
      )P3
      On P1.order_sn=P3.order_sn
      Left Join(
          Select
          P2.order_sn,max(p1.finish_time )gmt_created --拣货完成时间
          From jolly.who_wms_picking_info   P1
          Inner Join jolly.who_wms_picking_goods_detail  p2
          On p1.picking_id=p2.picking_id  
          WHERE P1.finish_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
          and to_date(from_unixtime(p1.finish_time))<>'1970-01-01'
          Group By order_sn
      )P4
      On p1.order_sn=p4.order_sn
      Where  to_date(p1.shipping_time)=to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
      and to_date(no_problems_order_uptime)<>'1970-01-01'
        And p1.is_shiped=1
    )a
)p6
on p1.data_date=p6.data_date

;


 
insert overwrite  table  zydb.rpt_supply_backlog_temp_arrival_goods_num
 Select
        to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))) data_date
        ,sum(deliver_num) arrival_goods_num 
        From  jolly.who_wms_pur_deliver_goods  P1
        Where p1.gmt_created>=unix_timestamp(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),'yyyy-MM-dd')
        And p1.gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
        Group By from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
;


insert overwrite table  zydb.rpt_supply_backlog_temp_arrival_onshelf_goods_num 
select  to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))) data_date,
                sum(case when f.gmt_created>=unix_timestamp(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),'yyyy-MM-dd') 
                and f.gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
                then f.deliver_num else 0 end) onshelf_goods_num,
                sum(case when  f.gmt_created<=unix_timestamp(concat(to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))),' 20:00:00'),'yyyy-MM-dd HH:mm:ss')
                  and f.gmt_created>=unix_timestamp(concat(to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)),' 20:00:00'),'yyyy-MM-dd HH:mm:ss')
                  and (onshelf_time is null or (onshelf_time-gmt_created)/3600/24>=10/24 )
                then f.deliver_num else 0 end)    backlog_onshelf_goods  
        from (select 
                f.rec_id
                ,f.deliver_id
                ,f.pur_order_sn
                ,f.pur_order_id,
                f.pur_order_goods_rec_id
                ,f.deliver_num
                --,j.shelf_num
                ,f.gmt_created
                --,j.gmt_created onshelf_time  --modified by hanshizhong 20161228
                ,j.finish_time onshelf_time
        from  ( 
                select * from 
                (
                    select 
                    *,row_number() over(partition by rec_id order by  gmt_created )  rn 
                      from 
                    jolly.who_wms_pur_deliver_goods a
                )a
                where rn=1
        ) f
        --left join zybi.ods_who_wms_on_shelf_stockgood j
        left join jolly.who_wms_pur_deliver_info j 
         on f.deliver_id=j.deliver_id
        where f.gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd') 
         and f.gmt_created>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),15),'yyyy-MM-dd') 
) f;



 
insert overwrite  table zydb.rpt_supply_backlog_temp_1 
select e.tracking_no,e.inspector_admin_id,e.deliver_id,e.should_num,
from_unixtime(e.gmt_created),from_unixtime(d.inspector_time),from_unixtime(d.add_time),from_unixtime(t.inspector_time) inspector_time_1,
from_unixtime(allo_order_time)   from 
(
  select 
   e.tracking_no,e.inspector_admin_id,0 deliver_id,
   e.should_num,e.gmt_created,e.pur_order_sn
   from  zydb.ods_wms_pur_deliver_receipt   e
  where e.gmt_created>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),15),'yyyy-MM-dd')
  and  e.gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')  
) e 
left join (
 
    select 
      trim(x.tracking_no) tracking_no
      ,min(t.gmt_created) inspector_time
      ,min(add_time) add_time 
    from  jolly.who_wms_pur_order_tracking_info  x
    left join jolly.who_wms_pur_order_goods   d 
     on x.pur_order_id=d.pur_order_id
    left join jolly.who_wms_pur_deliver_goods   t 
     on d.rec_id=t.pur_order_goods_rec_id 
    and t.gmt_created>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),15),'yyyy-MM-dd')
    and t.gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')  
    left join (
        --select   f.pur_delivery_goods_rec_id
        --,min(f.add_time) add_time 
        --from jolly.who_wms_pur_exception_goods  f 
        -- group by f.pur_delivery_goods_rec_id
 		select b.rec_id    pur_delivery_goods_rec_id
        ,min(f.add_time) add_time 
        from jolly.who_wms_pur_exception_goods  f 
        left join 
        jolly.jolly_spm_pur_order_info a 
        on a.pur_order_sn=f.pur_delivery_sn
        left join 
        jolly.who_wms_pur_order_goods_spm b
        on a.pur_order_id=b.pur_order_id  
   		group by  b.rec_id
     ) f
    on d.rec_id=f.pur_delivery_goods_rec_id
    where   d.gmt_created>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),15),'yyyy-MM-dd')
        and d.gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')  
    group by trim(x.tracking_no)  

) d 
on lower(trim(d.tracking_no))=lower(trim(e.tracking_no))
left join (
    select t.pur_order_sn,min(t.gmt_created) inspector_time 
     from  jolly.who_wms_pur_deliver_goods   t 
    where t.gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')  
     and t.gmt_created>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),15),'yyyy-MM-dd')
    group by t.pur_order_sn  
) t
--on trim(e.pur_order_sn)like concat('%',t.pur_order_sn,'%')
on trim(e.pur_order_sn) = t.pur_order_sn
left join (select  c.tracking_no,min(d.gmt_created) allo_order_time
 from jolly.who_wms_allocate_order_info   c
left join  jolly.who_wms_pur_deliver_goods  d
on c.allocate_order_id=d.pur_order_id
where c.tracking_no is not null
group by c.tracking_no) c
on lower(trim(c.tracking_no))=lower(trim(e.tracking_no))

;



insert overwrite table  zydb.rpt_daily_supply_stockage_temp_1 
select to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))) data_date, d.*
  from (select d.rec_id,
               c.pur_order_id,
               c.pur_order_sn,
               c.type,
               d.org_supp_num,
               d.relieve_num,
               d.cancel_num,
               from_unixtime(d.gmt_created)  pur_time,--采购单生成时间,
               from_unixtime(d.gmt_created)  recepit_time,--签收时间,
               from_unixtime(t.gmt_created)  deliver_time,--到货签收时间,
               from_unixtime(f.add_time)     --异常添加时间
          from (select d.pur_order_id,
                       d.rec_id,
                       min(d.org_supp_num ) org_supp_num,
                       min(d.relieve_num) relieve_num,
                       min(d.cancel_num)cancel_num,
                       min(e.gmt_created) gmt_created
                  from  jolly.who_wms_pur_order_goods  d
                  left join  jolly.who_wms_pur_order_tracking_info  t
                    on d.pur_order_id = t.pur_order_id
					and t.is_new=1	
                    and t.gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
                  left join (select trim(e.tracking_no) tracking_no,
                                   min(e.gmt_created) gmt_created
                              from  jolly.who_wms_pur_deliver_receipt  e
                             WHERE e.gmt_created < unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
							 and e.is_new =1
                             group by trim(e.tracking_no)) e
                    on trim(e.tracking_no) = trim(t.tracking_no)
                 where d.gmt_created >= unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),15),'yyyy-MM-dd')
                   and d.gmt_created <  unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
				   and d.is_new=1
                 group by d.pur_order_id, d.rec_id) d
         inner join jolly.who_wms_pur_order_info  c
            on d.pur_order_id = c.pur_order_id
			and c.is_new=1
          left join (select t.pur_order_goods_rec_id,
                           min(t.gmt_created) gmt_created
                      from  jolly.who_wms_pur_deliver_goods  t
                     WHERE gmt_created < unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
					 and t.is_new=1
                     group by t.pur_order_goods_rec_id) t
            on d.rec_id = t.pur_order_goods_rec_id
          left join (select f.pur_delivery_goods_rec_id,
                           min(f.add_time) add_time
                      from jolly.who_wms_pur_exception_goods  f
                     WHERE F.ADD_TIME < unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
                     group by f.pur_delivery_goods_rec_id) f
            on d.rec_id = f.pur_delivery_goods_rec_id) d
 where d.org_supp_num - d.relieve_num - d.cancel_num > 0
;





insert overwrite  table zydb.rpt_daily_supply_stockage_temp_2 
 
     select b.rec_id,g.rec_id pur_rec_id ,b.supp_num,b.oos_num,b.oos_status,b.review_status,b.pur_type,
          from_unixtime(b.gmt_created) ,from_unixtime(b.check_time) ,g.pur_order_sn,
          g.buy_time,g.receipt_time,g.arrive_time,g.except_time,
          g.org_supp_num,g.relieve_num,g.cancel_num
        from 
     (
        select b.*,c.demand_rec_id,case when b.pur_order_goods_id is not null 
         then b.pur_order_goods_id else c.demand_rec_id end pur_order_goods_rec_id 
            from jolly.who_wms_pur_goods_demand  b
        left join jolly.who_wms_demand_goods_relation  c
        on b.rec_id=c.pur_order_goods_rec_id
      ) b
    left join zydb.rpt_daily_supply_stockage_temp_1 g 
    
    on g.rec_id=b.pur_order_goods_rec_id
    where b.gmt_created>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),30),'yyyy-MM-dd')
    and b.gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')  
;  








insert overwrite  table  zydb.rpt_daily_supply_temp_1 
select
    to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
    ,goods_num--商品数量
    ,cast(goods_number as bigint)--商品数
    ,order_num --订单数
from 
(
    select  
    '${data_date}' data_date,
    sum(T.SUPP_NUM-T.OOS_NUM) goods_num--商品数量 
    from zydb.rpt_daily_supply_stockage_temp_2 t
    where nvl(t.receipt_time,nvl(t.arrive_time,t.except_time))>=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
    and nvl(t.receipt_time,nvl(t.arrive_time,t.except_time))<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)  
    and t.pur_type=1
)o1 left join (
  select
    '${data_date}' data_date,sum(t.org_num-t.oos_num) goods_number--商品数
   from zydb.rpt_supply_tmp3 t
  where t.row_num=1
  and t.push_time>'2012-01-01'
  and (unix_timestamp(t.receipt_time)-unix_timestamp(push_time))/3600/24>0
  and (unix_timestamp(deleve_time)-unix_timestamp(t.receipt_time))/3600/24>0
  and (unix_timestamp(onself_time)-unix_timestamp(deleve_time))/3600/24>0
  and (unix_timestamp(push_time)-unix_timestamp(pay_time))/3600/24>0
)o2 on 
o1.data_date=o2.data_date
left join (
    Select
    '${data_date}' data_date, count(distinct P1.ORDER_SN)  order_num--订单数
    From zydb.rpt_scm_order_tmp p1
    Where p1.pay_status In (1,3)
    and to_date(order_pack_time)<>'1970-01-01'
)o3 
on o1.data_date=o3.data_date 
;











 
insert overwrite table  zydb.rpt_supply_chain_final_table   
select
      to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))) data_date,
      ship_paid_order_num, 
      shipping_total_time, 
      cod_receipt_order_num, 
      cod_receipt_total_time, 
      paid_order_num_last, 
      goods_num_last, 
      out_stock_orders_num_last, 
      out_stock_goods_num_last, 
      abnormal_order, 
      pay_push_duration, 
      push_receipt_duration, 
      receipt_quality_duration, 
      quality_onshelf_duration, 
      tc_purchase_goods_rate, 
      nagao_purchase_goods_rate, 
      nagao_ros_goods_rate, 
      nagao_pks_order_rate, 
      onshelf_outstock_duration, 
      picking_duration, 
      package_duration, 
      shipping_duration, 
      lag_review_order_rate, 
      nagao_review_order_rate,
      add_shipped_order_num,
      add_packaged_orders_num,
      add_picking_order_num,
      picking_abnormal_order_num,
      backlog_pick_shipped_order_num
      ,backlog_picking_order_num
      ,backlog_packaged_order_num
      ,backlog_shipped_order_num
      ,stocking_goods_num 
      ,paid_goods_num 
      ,stocking_order_num 
      ,paid_order_num 
      ,arrival_goods_num
      ,abnormal_arrival_goods_num
      ,out_stock_goods_num
      ,arrival_goods_avg_duration
      ,nagao_purchase_goods_num
      ,p12.onshelf_goods_num add_onshelf_goods_num
      ,p12.backlog_onshelf_goods  
      ,add_qc_goods_num
      ,backlog_qc_goods_number
      ,purchase_goods_num
      ,p16.goods_num  arrival_goods_avg_duration_num
      ,p16.good_count pay_receipt_duration_num
      ,p16.order_count  ONSHELF_OUTSTO_DUR_ORDER_NUM
      ,0 deliver_num --p17.deliver_num
      ,0 deliver_time --p17.deliver_time
      ,0 arrive_country_num --p17.arrive_country_num
      ,0 trans_time--p17.trans_time
From zydb.rpt_supply_chain_table P1
 
left Join (
     --拣货异常订单数
     Select
     to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))) data_date,
     Count(Distinct P1.returned_Order_Id) picking_abnormal_order_num
     From jolly.who_wms_returned_order_info  P1
     Where p1.returned_time>=unix_timestamp('${data_date}','yyyyMMdd')
     And P1.returned_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
 
     and return_reason in (24) 
     Group By to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
 
)p5
On p1.data_date=p5.data_date
left join(
        select to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))) data_date,
         count(case when a.is_status in (1,2,3) then a.order_sn else null end) backlog_pick_shipped_order_num,
         count(case when a.is_status in (1) then a.order_sn else null end) backlog_picking_order_num,
         count(case when a.is_status in (2) then a.order_sn else null end) backlog_packaged_order_num,
         count(case when a.is_status in (3) then a.order_sn else null end) backlog_shipped_order_num,
         count(case when pick_time  >unix_timestamp(concat(to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)),' 18:00:00'),'yyyy-MM-dd HH:mm:ss') and 
                pick_time  <=unix_timestamp(concat(to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))),' 18:00:00'),'yyyy-MM-dd HH:mm:ss')
                    then a.order_sn else null end) add_picking_order_num, 
         count(case when pick_finish_time  >=unix_timestamp('${data_date}','yyyyMMdd')
                and pick_finish_time  <unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
                    then a.order_sn else null end ) add_packaged_orders_num,
         count(case when order_pack_time>=to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
                and order_pack_time<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
                    then a.order_sn else null end )add_shipped_order_num
        from 
         (
            select to_date(a.shipping_time) data_date,a.order_sn,
                   case when a.pay_id=41 then a.pay_time 
                   else a.result_pay_time 
                     end pay_time,
                   a.shipping_time,a.no_problems_order_uptime,a.order_pack_time,last_modified_time last_modified_time ,job_time ,pick_finish_time  ,
                   job_time    pick_time  ,
                   a.is_shiped,a.is_check,a.order_status,
                   case when job_time <=unix_timestamp(concat(to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))),' 18:00:00'),'yyyy-MM-dd HH:mm:ss')
                     and (a.is_shiped!=1 or a.shipping_time>=concat(to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))),' 23:00:00'))
                     and (a.is_shiped in (7,8) or pick_finish_time  >=unix_timestamp(concat(to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))),' 23:00:00'),'yyyy-MM-dd HH:mm:ss'))  
                     then 1
                        when  job_time <=unix_timestamp(concat(to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))),' 18:00:00'),'yyyy-MM-dd HH:mm:ss')
                            and (a.is_shiped!=1 or a.shipping_time>=concat(to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))),' 23:00:00'))
                            and (a.is_shiped in (6) or (a.is_shiped in(1,3)
                                and a.order_pack_time>=concat(to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))),' 23:00:00'))) then 2 
                        when job_time <=unix_timestamp(concat(to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))),' 18:00:00'),'yyyy-MM-dd HH:mm:ss')
                            and (a.is_shiped!=1 or a.shipping_time>=concat(to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))),' 23:00:00'))
                            and (a.is_shiped in (3) or (a.is_shiped in(1) 
                                and a.shipping_time>=concat(to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))),' 23:00:00'))) then 3 
                           else 0 end  is_status
                from zydb.rpt_scm_order_tmp a
                 left join (select n.order_sn,max(n.last_modified_time) last_modified_time from jolly.who_wms_goods_need_lock_detail  n group by n.order_sn) n
                 on a.order_sn=n.order_sn
                 left join (select k.order_sn,max(k.gmt_created) job_time  
                  from  jolly.who_wms_outing_stock_detail  k 
                    where  gmt_created <unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
                 group by k.order_sn) k
                on a.order_sn=k.order_sn
                left join (select m.order_sn,max(l.finish_time) pick_finish_time     
                from jolly.who_wms_picking_info   l
                inner join jolly.who_wms_picking_goods_detail   m
                on l.picking_id=m.picking_id
                group by m.order_sn) m
                on a.order_sn=m.order_sn
                where   
                                a.pay_status in (1,3)
                               and a.is_check=1
                               and a.order_status=1
                               and a.is_shiped in (1,3,6,7,8)
        ) a


)p6
On p1.data_date=p6.data_date
left Join (
        --stocking_goods_num  备货命中商品数
        --paid_goods_num  待配货商品数
       -- stocking_order_num  备货命中订单数
        --paid_order_num  待配货订单数
        select to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))) data_date,sum(original_goods_number)-sum(nvl(supp_num,0)) stocking_goods_num,
               sum(original_goods_number) paid_goods_num,
               count(distinct order_id)-count (distinct(case when a.supp_num is not null then order_id else null end)) stocking_order_num,
               count(distinct order_id) paid_order_num
        from 
        (select case when b.pay_id=41 then b.pay_time 
        else b.result_pay_time end pay_time,a.order_id,a.goods_number,a.original_goods_number,a.sku_id,a.goods_id,
        c.supp_num 
        from zydb.dw_order_goods_fact  a
        inner join zydb.rpt_scm_order_tmp b
        on a.site_id=b.site_id
        and a.order_id=b.order_id
        left join  
        (
            select order_sn,is_check from jolly.who_order_info
            union all
            select order_sn,is_check from jolly.who_order_info_history
        )  d
        on b.order_sn=d.order_sn
        left join jolly.who_wms_pur_goods_demand c
        on a.order_id=c.order_id
        and a.goods_id=c.goods_id
        and cast(a.sku_id as int)=c.sku_id
        where case when b.pay_id=41 then b.pay_time 
            else b.result_pay_time end >=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
            and case when b.pay_id=41 then b.pay_time 
            else b.result_pay_time end <date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
            and b.pay_status in (1,3)
            and d.is_check=1) a
)p7
On p1.data_date=p7.data_date
left Join
        --arrival_goods_num 到货商品件数
--        Select
--        to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))) data_date
--        ,sum(deliver_num) arrival_goods_num 
--        From  jolly.who_wms_pur_deliver_goods  P1
--        Where p1.gmt_created>=unix_timestamp(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),'yyyy-MM-dd')
--        And p1.gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
--        Group By from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))

--)p8
zydb.rpt_supply_backlog_temp_arrival_goods_num p8
on p1.data_date=p8.data_date
left Join(
        --abnormal_arrival_goods_num  到货异常件数
        Select
            to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))) data_date
            ,sum(exp_num) abnormal_arrival_goods_num
        From  jolly.who_wms_pur_exception_goods  P1
        where add_time>=unix_timestamp(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),'yyyy-MM-dd')
        And add_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
        And exp_type=1 
        and op_type!=8
        Group By from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
)p9
On p1.data_date=p9.data_date
left Join (

    -- out_stock_goods_num  缺货商品件数
    Select
        to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))) data_date
        ,sum(oos_num )out_stock_goods_num
    From  jolly.who_wms_order_oos_log  P1
    WHere p1.create_time>=unix_timestamp(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),'yyyy-MM-dd')
    And p1.create_time <unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
    Group By from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')) 

)p10
On p1.data_date=p10.data_date
left Join( 
        select to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))  data_date
            ,sum(( unix_timestamp(nvl(t.receipt_time,nvl(t.arrive_time,t.except_time)))
            - unix_timestamp(case when t.review_status=2 then t.buy_time 
            else t.need_push_time end))/3600/24
            *(T.SUPP_NUM-T.OOS_NUM))/sum(T.SUPP_NUM-T.OOS_NUM) arrival_goods_avg_duration 
        from zydb.rpt_daily_supply_stockage_temp_2 t
        where nvl(t.receipt_time,nvl(t.arrive_time,t.except_time))>=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
            and nvl(t.receipt_time,nvl(t.arrive_time,t.except_time))<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
            and t.pur_type=1
)p11
On p1.data_date=p11.data_date
left join 
--(
--        select  to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))) data_date,
--                sum(case when f.gmt_created>=unix_timestamp(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),'yyyy-MM-dd') 
--                and f.gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
--                then f.deliver_num else 0 end) onshelf_goods_num,
--                sum(case when  f.gmt_created<=unix_timestamp(concat(to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))),' 20:00:00'),'yyyy-MM-dd HH:mm:ss')
--                 and f.gmt_created>=unix_timestamp(concat(to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)),' 20:00:00'),'yyyy-MM-dd HH:mm:ss')
--                  and (onshelf_time is null or (onshelf_time-gmt_created)/3600/24>=10/24 )
--                then f.deliver_num else 0 end)    backlog_onshelf_goods  
--        from (select 
--                f.rec_id
--                ,f.deliver_id
--                ,f.pur_order_sn
--                ,f.pur_order_id,
--                f.pur_order_goods_rec_id
--                ,f.deliver_num
--                --,j.shelf_num
--                ,f.gmt_created
--                --,j.gmt_created onshelf_time  --modified by hanshizhong 20161228
--                ,j.finish_time onshelf_time
--        from  jolly.who_wms_pur_deliver_goods  f
--        --left join zybi.ods_who_wms_on_shelf_stockgood j
--        left join jolly.who_wms_pur_deliver_info j 
--         on f.deliver_id=j.deliver_id
--        where f.gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
--         and f.gmt_created>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),15),'yyyy-MM-dd')) f
--)p12
zydb.rpt_supply_backlog_temp_arrival_onshelf_goods_num  p12 
on p1.data_date=p12.data_date
left join (

    select to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))) data_date,
           sum(e.should_num) add_qc_goods_num,
           sum(case
                 when  (unix_timestamp(coalesce(inspector_time_1,e.inspector_time, add_Time,e.allo_order_time)) -
                      unix_timestamp(e.gmt_created))/3600/24 >= 1 or   coalesce(inspector_time_1,e.inspector_time, add_Time,e.allo_order_time) is null then
                  e.should_num
                 else
                  0
               end) backlog_qc_goods_number
      from (select e.tracking_no,
                   e.inspector_admin_id,
                   e.deliver_id,
                   e.should_num,
                   e.gmt_created,
                   e.inspector_time,
                   min(e.inspector_time_1) inspector_time_1,
                   min(e.allo_order_time) allo_order_time,
                   min(add_Time) add_Time
              from zydb.rpt_supply_backlog_temp_1 e
             where e.should_num > 0
             group by e.tracking_no,
                      e.inspector_admin_id,
                      e.deliver_id,
                      e.should_num,
                      e.gmt_created,
                      e.inspector_time) e
            where
            to_Date(e.gmt_created)=to_Date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1))
            
)p13
on p1.data_date=p13.data_date
left join (
    select  to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))) data_date,
            sum(t.supp_num)  purchase_goods_num 
    from zydb.rpt_daily_supply_stockage_temp_2 t
    where  (case when t.review_status=2 then t.buy_time else t.need_push_time end)>=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
    and  (case when t.review_status=2 then t.buy_time else t.need_push_time end)<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
    and t.pur_type=1
    
)p14
On p1.data_date=p14.data_date
left join (
    select  to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))  data_date,
            sum(t.supp_num) nagao_purchase_goods_num 
    from zydb.rpt_daily_supply_stockage_temp_2 t
    where   (case when t.review_status=2 then t.buy_time else t.need_push_time end)<=date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),3)
    and nvl(t.receipt_time,nvl(t.arrive_time,t.except_time))>from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
    and  nvl(t.receipt_time,nvl(t.arrive_time,t.except_time))<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
    and t.pur_type=1
)p15
on p1.data_date=p15.data_date
left join zydb.rpt_daily_supply_temp_1 p16
on p1.data_date=p16.data_date
--left join zydb.rpt_trans_delivery_tmp_1 p17
--on p1.data_date=p17.data_date
where p1.data_date=to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))



union all


select * from zydb.rpt_supply_chain_final_table
where data_Date<>to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))

;
