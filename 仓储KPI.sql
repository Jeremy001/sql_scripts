/*
内容：仓储kpi底层数据表sql
作者：Steven王世鑫
*/

insert overwrite rpt_depot_order_tmp_kpi
Select
p1.site_id,
p1.add_time,
p1.pay_time,
p1.user_id,
p1.pay_id,
p1.shipping_id,
'',--p1.session_id
p1.order_id,
p1.order_sn,
p1.goods_amount,
p1.order_total_amount,
p1.pay_status,
'' ,-- p1.search_keyword
0 , --p1.goods_number
p1.result_pay_time,
p1.is_shiped,
p1.order_source,
p1.shipping_time,
p1.no_problems_order_uptime,
p1.order_pack_time,
p1.depod_id depot_id,
p1.is_cod_vip,
p2.order_status ,
p2.is_check
From zydb.dw_order_sub_order_fact p1
Inner join (
         select a.order_id,a.order_status,is_check,a.add_time,
                (case
                  when a.site_id=2 then 602 --addby hanshizhong 20170822
                  when a.site_id=1 then 601
                  when a.site_id=0 and order_source =5 then 900
                  when a.site_id=0 and order_source in (3,4) then 600
                  when a.site_id=0 and order_source in (6)   then 700
                  when a.site_id=0 and b.email in ('jollyalie365@gmail.com','jollycorp2016@gmail.com',
                    'jollychicam@gmail.com','jollycorp20130613@gmail.com','jollycorp.asos@gmail.com',
                    'jollycorpuk@gmail.com','jollychicc@hotmail.com','meetu2016@hotmail.com',
                    'meetu2015@hotmail.com')  then 800
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
) p2
on p1.order_id=p2.order_id
and p1.site_id=p2.site_id
Where P1.Site_Id In(400,600,700,800,900)
and p1.pop_id=0
and p1.add_time>=date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),180)
 and p1.add_time<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
;





insert overwrite zydb.rpt_wh_quality_tmp_kpi
select a.rec_id,
       a.pur_order_sn,
       a.pur_order_goods_rec_id,
       a.deliver_num,
       a.depot_id,
       from_unixtime(a.gmt_created) gmt_created,
       from_unixtime(t1.gmt_created)           arrive1,
       from_unixtime(t2.gmt_created)           arrive2
  from  jolly.who_wms_pur_deliver_goods  a
  Left join (SELECT p1.rec_id,
                    p1.review_status,
                    p1.pur_order_goods_id,
                    p1.check_time gmt_created, --推送时间
                    p2.demand_rec_id,
                    source_rec_id,
                    CASE
                      WHEN p1.pur_order_goods_id IS NOT NULL THEN
                       p1.pur_order_goods_id
                      ELSE
                       p2.demand_rec_id
                    END pur_order_goods_rec_id
               FROM jolly.who_wms_pur_goods_demand  p1 --------2
               LEFT JOIN jolly.who_wms_demand_goods_relation  p2 ---------3
                 ON p1.rec_id = p2.pur_order_goods_rec_id
              where to_date(from_unixtime(p1.check_time)) > to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),9))  --从采购推送倒推10天
             ) p3
    on a.pur_order_goods_rec_id = p3.pur_order_goods_rec_id
  left join jolly.who_wms_pur_order_goods  p4
    on p3.pur_order_goods_rec_id = p4.rec_id
   and p4.gmt_created >unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),4))
  left join jolly.who_wms_pur_deliver_receipt  t1
    on trim(P4.Tracking_No) = trim(t1.Tracking_No) -----根据物流单号匹配
   and t1.gmt_created >unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),2))
  left join (select
                 pur_order_sn,
                 min(gmt_created) gmt_created

                from  zydb.ods_wms_pur_deliver_receipt
                 where gmt_created >= unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),9))
                group by pur_order_sn
            ) t2
    on t2.pur_order_sn = a.pur_order_sn
 where a.gmt_created >=unix_timestamp(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
   and a.gmt_created < unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1))
 ;








insert overwrite  zydb.rpt_depot_quality_duration_tmp_kpi

select q.depot_id,
    sum(((unix_timestamp(q.gmt_created)-unix_timestamp(nvl(q.arrive1,q.arrive2)))/3600/24)*q.deliver_num)/sum(q.deliver_num)*24 receipt_quality_duration
    from (
     select q.*,row_number()over(partition by q.rec_id order by q.gmt_created desc) row_num
         from zydb.rpt_wh_quality_tmp_kpi q
         )q
    where nvl(q.arrive1,q.arrive2)is not null
    and ((unix_timestamp(q.gmt_created)-unix_timestamp(nvl(q.arrive1,q.arrive2)))/3600/24)>0
    and to_date(nvl(q.arrive1,q.arrive2))<>'1970-01-01'
    and q.row_num=1
    group by q.depot_id
;



insert overwrite  zydb.rpt_depot_onshelf_duration_tmp_kpi

select
p2.depot_id,sum(p2.deliver_num) onshelf_goods,
 sum(case when p1.finish_time>p2.gmt_created
               and to_date(from_unixtime(receive_time))<>'1970-01-01'
             then ((p1.finish_time-p2.gmt_created)/3600/24)*p2.deliver_num end )
    /sum(case when p1.finish_time>p2.gmt_created
                   and to_date(from_unixtime(p2.gmt_created)) <>'1970-01-01'
             then p2.deliver_num end) *24 quality_onshelf_duration
from jolly.who_wms_pur_deliver_info  p1
left join jolly.who_wms_pur_deliver_goods  p2
on p1.deliver_id=p2.deliver_id
where to_Date(from_unixtime(p1.finish_time))=to_Date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
and
to_Date(from_unixtime(p2.gmt_created))>=to_Date(from_unixtime(unix_timestamp('20170901','yyyyMMdd')))
group by p2.depot_id
;



insert overwrite  zydb.rpt_depot_picking_duration_tmp_kpi

select p4.depot_id,count(distinct p4.order_sn) picking_order_num,-- 拣货订单数,
    avg(case when p4.gmt_created>p3.gmt_created and to_date(from_unixtime(p3.gmt_created))<>'1970-01-01'
    then (p4.gmt_created-p3.gmt_created)/3600/24 end)*24  picking_duration --拣货时长
    from
        (Select
            p2.depot_id,P2.Order_Sn,max(p1.finish_time)gmt_created --拣货完成时间
        From jolly.who_wms_picking_info  P1
        Inner Join jolly.who_wms_picking_goods_detail  p2
        On p1.picking_id=p2.picking_id
        left join zydb.rpt_depot_order_tmp_kpi o on p2.order_id=o.order_id
         and o.is_shiped in (1,3,6)--?
        where
        to_Date(from_unixtime(p1.finish_time))>to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),19))
        Group By p2.depot_id,P2.Order_Sn
        having max(p1.finish_time)>=unix_timestamp(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
        and max(p1.finish_time)<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1))
  ) P4
    left join
        (Select
           P3.Order_Sn,max(P3.Gmt_Created) Gmt_Created --可拣货时间
        From jolly.who_wms_outing_stock_detail  P3
        WHERE P3.GMT_CREATED<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1))
        and to_date(from_unixtime(Gmt_Created))>to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),19))
        Group By Order_Sn
        )  p3 on p3.order_sn=p4.order_sn
    group by p4.depot_id
;






insert overwrite  zydb.rpt_depot_pack_ship_tmp_kpi

select
p1.depot_id,count( distinct
case when to_date(p1.ORDER_PACK_TIME)=to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))    then p1.order_id
  else null end ) packaged_order_num
,count(
case when to_date(p1.shipping_time)=to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))   then p1.order_id
  else null end ) shipped_order_num
,avg(case when  to_date(p1.shipping_time)=to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))   and p1.shipping_time>p1.order_pack_time and to_Date(p1.order_pack_time)<>'1970-01-01'
    then (unix_timestamp(p1.shipping_time)-unix_timestamp(p1.order_pack_time))/3600/24 end)*24 shipping_duration --发运时长
, avg(case when to_date(p1.order_pack_time)=to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
and p1.order_pack_time>from_unixtime(p2.gmt_created)
and to_date(from_unixtime(p2.gmt_created))<>'1970-01-01'
    then  (unix_timestamp(p1.order_pack_time)-p2.gmt_created)/3600/24 end)*24 package_duration --打包时长

from zydb.rpt_depot_order_tmp_kpi p1
 left join
        (Select
        p2.depot_id,P2.Order_id,max(p1.finish_time)gmt_created --拣货完成时间
        From jolly.who_wms_picking_info  P1
        Inner Join jolly.who_wms_picking_goods_detail  p2
         On p1.picking_id=p2.picking_id
        left join zydb.rpt_depot_order_tmp_kpi o
        on p2.order_id=o.order_id

        Group By p2.depot_id,P2.Order_id)p2
    on p1.order_id=p2.order_id
group by p1.depot_id
;


insert overwrite  zydb.rpt_depot_quality_tmp_kpi
select
p1.depot_id,sum(p1.deliver_num) qc_goods_number
from  jolly.who_wms_pur_deliver_goods   p1
where p1.gmt_created>=unix_timestamp(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
and p1.gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1))
group by p1.depot_id
;


insert into ZYDB.rpt_warehousing_depart_kpi partition(data_date='${data_date}')

select
receipt_quality_duration+quality_onshelf_duration+picking_duration+shipping_duration+PACKAGE_DURATION warehouse_total_time
,oos_orders
,paid_orders
,complaint_orders
,shipped_orders
,change_num
,deliver_num
,allocate_time
from zydb.rpt_kpi_paied_order  p1
left join (
select
'${data_date}' data_date,
sum(qc_goods_number*receipt_quality_duration)/sum(qc_goods_number)  receipt_quality_duration,
sum(onshelf_goods*quality_onshelf_duration)/sum(onshelf_goods) quality_onshelf_duration,
sum(picking_order_num*picking_duration)/sum(picking_order_num) picking_duration,
sum(packaged_order_num*package_duration)/sum(packaged_order_num) shipping_duration,
sum(shipped_order_num*shipping_duration)/sum(shipped_order_num)  PACKAGE_DURATION
from zydb.dim_dw_depot p1
left join zydb.rpt_depot_quality_tmp_kpi p2
on p1.depot_id=p2.depot_id
left join zydb.rpt_depot_quality_duration_tmp_kpi p3
on p1.depot_id=p3.depot_id
left join zydb.rpt_depot_onshelf_duration_tmp_kpi p4
on p1.depot_id=p4.depot_id
left join zydb.rpt_depot_picking_duration_tmp_kpi p5
on p1.depot_id=p5.depot_id
left join zydb.rpt_depot_pack_ship_tmp_kpi p6
on p1.depot_id=p6.depot_id
where p1.depot_id in (4,5,6)

)p2
on p1.data_date=p2.data_date
left join (
WITH t1 AS
(SELECT p1.returned_order_id
        ,p1.returned_rec_id
        ,p3.order_sn
        ,p2.depot_id
        ,p6.reason_name_cn AS customer_return_reason--客户退货原因
        ,p4.reason_name_cn AS audit_first_reason--审核一级原因
        ,p5.reason_name_cn AS audit_second_reason --审核二级原因
        ,DECODE(p1.duty_depart, 0, '其他', 1, '品控', 2, '后端供应链', 3, '仓库',  4, '文案翻译') AS unconfirm_duty_depart--未确认前责任部门
        ,DECODE(p7.depart_duty, 0, '其他', 1, '品控', 2, '后端供应链', 3, '仓库',  4, '文案翻译') AS confirm_duty_depart--确认责任部门
        ,DECODE(p7.push_status, 1, '未确认', 2, '已确认') AS  push_status---推送状态
        ,FROM_UNIXTIME(p2.apply_time) AS apply_time --申请时间
        ,FROM_UNIXTIME(p1.gmt_created) AS  gmt_created--添加时间
        ,FROM_UNIXTIME(p2.audit_time) AS cs_audit_time--客服审核时间
        ,FROM_UNIXTIME(p7.gmt_push) AS    push_time --推送时间
        ,FROM_UNIXTIME(p7.gmt_confirm) AS   confirm_time  -- 确认时间
  FROM jolly.who_wms_returned_order_goods p1
  LEFT JOIN jolly.who_wms_returned_order_info p2
             ON p1.returned_rec_id = p2.returned_rec_id
 LEFT JOIN jolly.who_order_info p3
             ON p1.returned_order_id = p3.order_id
LEFT JOIN jolly.who_wms_returned_goods_reason p4
             ON p1.check_return_reason = p4.reason_id
LEFT JOIN jolly.who_wms_returned_goods_reason p5
             ON p1.second_check_return_reason = p5.reason_id
LEFT JOIN jolly.who_wms_returned_goods_reason p6
             ON p1.return_reason = p6.reason_id
LEFT JOIN jolly.who_cs_duty_push p7
             ON p2.returned_rec_id = p7.returned_rec_id
WHERE (p1.check_return_reason IN (43, 44, 49) OR p1.second_check_return_reason IN (3, 4))       -- 可能归属到仓库责任的原因，会推送给仓库的只有49
     AND p2.return_status IN (1, 3, 4, 5, 6, 8)        -- 会推送到仓库的退货退款单的状态
     AND p1.duty_depart >= 1
     AND regexp_replace(substr(FROM_UNIXTIME(p7.gmt_push),1,10),'-','')='${data_date}'
GROUP BY p1.returned_order_id
        ,p1.returned_rec_id
        ,p3.order_sn
        ,p2.depot_id
        ,p6.reason_name_cn
        ,p4.reason_name_cn
        ,p5.reason_name_cn
        ,DECODE(p1.duty_depart, 0, '其他', 1, '品控', 2, '后端供应链', 3, '仓库',  4, '文案翻译')
        ,DECODE(p7.depart_duty, 0, '其他', 1, '品控', 2, '后端供应链', 3, '仓库',  4, '文案翻译')
        ,DECODE(p7.push_status, 1, '未确认', 2, '已确认')
        ,FROM_UNIXTIME(p2.apply_time)
        ,FROM_UNIXTIME(p1.gmt_created)
        ,FROM_UNIXTIME(p2.audit_time)
        ,FROM_UNIXTIME(p7.gmt_push)
        ,FROM_UNIXTIME(p7.gmt_confirm)
)

SELECT regexp_replace(substr(t1.push_time,1,10),'-','') data_date,count(distinct concat(cast(returned_order_id as string),cast(returned_rec_id as string)) ) complaint_orders
FROM t1
WHERE confirm_duty_depart = '仓库'
     AND push_status = '已确认'
     group by regexp_replace(substr(t1.push_time,1,10),'-','')

)p3
on p1.data_date=p3.data_date
left join (
SELECT '${data_date}' data_date,
       count(DISTINCT order_id) oos_orders --缺货订单数
FROM
  (SELECT DISTINCT a.order_id,
                   a.create_time,
                   a.oos_num,
                   a.type --取消类型: 1-采购单取消,2-供应商门户确认缺货,3-处理到货异常,4-审核超过7天未配货的商品标记缺货,5-拣货异常,6-调拨取消,7-盘亏

   FROM jolly.who_wms_order_oos_log a
   WHERE a.type IN (5,
                    6,
                    7)--5-拣货异常+ 7-盘亏  2017年2月开始 6也是仓库造成

     AND regexp_replace(substr(from_unixtime(a.create_time),1,10),'-','') = '${data_date}' ) a
)p4
on p1.data_date=p4.data_date
left join (
select '${data_date}' data_date
, sum(abs(change_num)) change_num
  from ( ---先统计一个月内 每个sku的盘亏盘盈总计，外层再统计每个sku绝对值之和
        select
                goods_id,
                sku_id,

                sum(change_num)  change_num
          from jolly.who_wms_check_stock_detail_log
         where  regexp_replace(substr(from_unixtime(op_time),1,10),'-','') = '${data_date}'
                group by goods_id,
                   sku_id) a
)p5
on p1.data_date=p5.data_date
left join (

select '${data_date}' data_date,
       sum(case
             when p7.finish_time is not null then
              p6.deliver_num
             else
              0
           end) deliver_num

  from jolly.who_wms_pur_deliver_info p7
  left join jolly.who_wms_pur_deliver_goods p6
    on p6.deliver_id = p7.deliver_id
 where regexp_replace(substr(from_unixtime(finish_time),1,10),'-','')='${data_date}'
)p6
on p1.data_date=p5.data_date
left join (
select
'${data_date}' data_date
,round(avg((arrive_time -allo_start_time)/3600),2) allocate_time -- 平均到货用时
from
(
select
pur_order_sn,
min(b.gmt_created) allo_start_time,
max(t.finish_time) arrive_time
from  jolly.who_wms_allocate_order_info  b   --主键调拨单表
inner join jolly.who_wms_delivered_order_info c
on b.allocate_order_sn = c.delivered_order_sn
inner join
    (
      select
    t.deliver_id,t.deliver_sn,
    s.pur_order_id,s.pur_order_sn,
    s.pur_order_goods_rec_id ,
    t.gmt_created gmt_created,
    t.finish_time finish_time,
    t.status,t.total_num,t.depot_id,t.from_type
    from jolly.who_wms_pur_deliver_info t
    inner join jolly.who_wms_pur_deliver_goods   s on t.deliver_id=s.deliver_id
    where  t.from_type=3
    and regexp_replace(substr(from_unixtime(t.finish_time),1,10),'-','')= '${data_date}'
    and s.is_new=1
    and t.is_new=1
    ) t
    ON c.delivered_order_id=t.pur_order_id
where regexp_replace(substr(from_unixtime(t.finish_time),1,10),'-','')   = '${data_date}'
group by pur_order_sn

) a
)p7
on p1.data_date=p7.data_date
;


-- 一些明细数据的提取 ===========================================================

-- 1.异常和缺货订单明细
-- jolly.who_wms_order_oos_log，订单缺货日志表
-- type:取消类型: 1-采购单取消,2-供应商门户确认缺货,3-处理到货异常,4-审核超过7天未配货的商品标记缺货,5-拣货异常,6-调拨取消,7-盘亏
-- 其中属于仓库责任的取消类型是5、6、7这三个
WITH
t1 AS
(SELECT p1.order_id
        ,FROM_UNIXTIME(p1.create_time) AS create_time
        ,p1.oos_num
        ,(CASE WHEN p1.type = 5 THEN '5-拣货异常'
               WHEN p1.type = 6 THEN '6-调拨取消'
               WHEN p1.type = 7 THEN '7-盘亏'
               ELSE '其他'
          END) AS oos_type
FROM jolly.who_wms_order_oos_log AS p1
WHERE p1.type IN (5, 6, 7)
  AND p1.create_time >= UNIX_TIMESTAMP('2017-12-01', 'yyyy-MM-dd')
  AND p1.create_time <  UNIX_TIMESTAMP('2018-01-01', 'yyyy-MM-dd')
GROUP BY p1.order_id
        ,FROM_UNIXTIME(p1.create_time)
        ,p1.oos_num
        ,(CASE WHEN p1.type = 5 THEN '5-拣货异常'
               WHEN p1.type = 6 THEN '6-调拨取消'
               WHEN p1.type = 7 THEN '7-盘亏'
               ELSE '其他'
          END)
)
SELECT COUNT(*)
FROM t1
;

SELECT *
FROM t1
ORDER BY create_time
LIMIT 10
;


-- 2017年12月kpi数据

SELECT *
FROM zydb.rpt_warehousing_depart_kpi
WHERE data_date >= '20171201'
  AND data_date <= '20171231'
;
















