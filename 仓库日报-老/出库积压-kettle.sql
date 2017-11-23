
with t as
(select to_date(a.shipping_time) data_date,
                  a.order_sn,
                  a.depot_id,
                  (case
                    when a.pay_id = 41 then
                     a.pay_time
                    else
                     a.result_pay_time
                  end) AS pay_time,
                  a.no_problems_order_uptime,
                  from_unixtime(定时任务时间) AS 可拣货时间,
                  from_unixtime(拣货完成时间) AS 拣货完成时间,
                  a.order_pack_time,
                  a.shipping_time,
                  a.is_shiped,
                  a.is_check,
                  a.order_status,
                  case
                    when 定时任务时间 <=
                    unix_timestamp(concat(to_date(from_unixtime(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 18:00:00'))
                         and
                         (   a.is_shiped != 1 or
                             a.shipping_time >=
                             concat(to_date(from_unixtime(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 23:59:59')
                          ) and
                         (
                             (a.is_shiped in (7, 8) and 拣货完成时间 is null) or
                             拣货完成时间 >=
                             unix_timestamp(concat(to_date(from_unixtime(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 23:59:59'))
                         ) then
                     1
                    when 定时任务时间 <=
                        unix_timestamp(concat(to_date(from_unixtime(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 18:00:00')) --拣货完成时间小于当天18：00
                           and
                         (   a.is_shiped != 1 or
                             a.shipping_time >=
                             concat(to_date(from_unixtime(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 23:59:59')
                         ) and
                         (a.is_shiped in (6) 
                            or
                            (  a.is_shiped in (1, 3) 
                                and
                            a.order_pack_time >= concat(to_date(from_unixtime(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 23:59:59')
                            )
                         ) then
                     2
                    when 定时任务时间 <=
                        unix_timestamp(concat(to_date(from_unixtime(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 18:00:00'))
                          and
                         (a.is_shiped != 1 or
                         a.shipping_time >=
                         concat(to_date(from_unixtime(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 23:59:59')) and
                         (a.is_shiped in (3) or
                         (a.is_shiped in (1) and
                         a.shipping_time >=
                         concat(to_date(from_unixtime(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 23:59:59'))) then
                     3
                    else
                     0
                  end is_status
             from zydb.rpt_depot_order_tmp a
                    left join (select k.order_sn, max(k.gmt_created) 定时任务时间
                         from jolly.who_wms_outing_stock_detail   k
                        where gmt_created > unix_timestamp(to_date(date_sub(from_unixtime(unix_timestamp('$[&data_date]','yyyyMMdd')),29)))  
                          and gmt_created < unix_timestamp(to_date(date_add(from_unixtime(unix_timestamp('$[&data_date]','yyyyMMdd')),1)))  
                        group by k.order_sn) k
               on a.order_sn = k.order_sn
             left join (select m.order_sn, max(l.finish_time) 拣货完成时间
                         from jolly.who_wms_picking_info  l
                        inner join  jolly.who_wms_picking_goods_detail   m
                           on l.picking_id = m.picking_id
                           where unix_timestamp(to_date(from_unixtime(l.finish_time)))<=unix_timestamp(to_date(from_unixtime(unix_timestamp('$[&data_date]','yyyyMMdd')))) 
                        group by m.order_sn) m
               on a.order_sn = m.order_sn
            where a.pay_status in (1, 3)
              and a.is_check = 1
              and a.order_status = 1
              and a.is_shiped in (1, 3, 6, 7, 8)
)

select * 
from t 
where is_status >= 1;


