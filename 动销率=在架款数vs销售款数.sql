在架款数
select count(distinct b.goods_id) on_sale_num
from zydb.dw_goods_on_sale t 
inner join
zydb.dim_jc_goods b
on t.goods_id=b.goods_id
where t.site_id=600
and t.ds>='${month_fisrt_day_yyyymmdd}'
and t.ds<='${month_end_day_yyyymmdd}'
and t.is_on_sale=1



----------------
售出款数
select
count(distinct b.goods_id) sold_num
from zydb.dw_order_fact a
inner join zydb.dw_order_goods_fact b on a.order_id=b.order_id
inner join zydb.dim_jc_goods c on b.goods_id=c.goods_id
left join 
(select *
   from (select t.medium,
                t.source,
                t.campaign,
                t.os,
                t.siteid,
                t.isnewuser,
                t.orderid,
                t.ip_country,
                t.servertime,
                t.cookieid,
                row_number() over(partition by t.siteid, t.orderid order by t.clienttime desc) row_num
                from zydb.ods_event_order_log t
                where  t.eventkey = 'key_checkout_result'
                AND nvl(t.source, '(unknown)') not like '%_incent%'
                AND (nvl(t.campaign, '(unknown)') not like '%_incent%' AND nvl(t.campaign, '(unknown)') not like '%_burst%')) t
              where row_num = 1) ws3
              on a.order_id=ws3.orderid and a.site_id=ws3.siteid
where a.site_id in(400,600,700,800,900)
and a.pay_status in(1,3)
and (case when a.pay_id=41 then a.pay_time else a.result_pay_time end )>='${month_fisrt_day}'
and (case when a.pay_id=41 then a.pay_time else a.result_pay_time end )<'${sys_date}'



