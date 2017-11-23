
-- 老仓库日报
-- 出库积压-hive

-- hue =========================================================================================
WITH 
-- outing_stock_time
k AS
(select k.order_sn 
        ,max(k.gmt_created) outing_stock_time
FROM jolly.who_wms_outing_stock_detail   k
where gmt_created > unix_timestamp(to_date(date_sub(FROM_UNIXTIME(unix_timestamp('${data_date}','yyyyMMdd')),29)), 'yyyy-MM-dd')  
    and gmt_created < unix_timestamp(to_date(date_add(FROM_UNIXTIME(unix_timestamp('${data_date}','yyyyMMdd')),1)), 'yyyy-MM-dd')  
group by k.order_sn
), 
-- picking_finish_time
m AS
(select m.order_sn
        ,max(l.finish_time) picking_finish_time
FROM jolly.who_wms_picking_info  l
inner join  jolly.who_wms_picking_goods_detail   m
on l.picking_id = m.picking_id
where unix_timestamp(to_date(FROM_UNIXTIME(l.finish_time)))<=unix_timestamp(to_date(FROM_UNIXTIME(unix_timestamp('${data_date}','yyyyMMdd'))), 'yyyy-MM-dd') 
group by m.order_sn
), 
t as
(select to_date(a.shipping_time) data_date,
                  a.order_sn,
                  a.depot_id,
                  (case when a.pay_id = 41 then a.pay_time else a.result_pay_time end) AS pay_time,
                  a.no_problems_order_uptime,
                  FROM_UNIXTIME(outing_stock_time) AS outing_stock_time,
                  FROM_UNIXTIME(picking_finish_time) AS picking_finish_time,
                  a.order_pack_time,
                  a.shipping_time,
                  a.is_shiped,
                  a.is_check,
                  a.order_status,
                  (case
                    when outing_stock_time <=
                    unix_timestamp(concat(to_date(FROM_UNIXTIME(unix_timestamp('${data_date}','yyyyMMdd'))),' 18:00:00'))
                         and
                         (   a.is_shiped != 1 or
                             a.shipping_time >=
                             concat(to_date(FROM_UNIXTIME(unix_timestamp('${data_date}','yyyyMMdd'))),' 23:59:59')
                          ) and
                         (
                             (a.is_shiped in (7, 8) and picking_finish_time is null) or
                             picking_finish_time >=
                             unix_timestamp(concat(to_date(FROM_UNIXTIME(unix_timestamp('${data_date}','yyyyMMdd'))),' 23:59:59'))
                         ) then
                     1
                    when outing_stock_time <=
                        unix_timestamp(concat(to_date(FROM_UNIXTIME(unix_timestamp('${data_date}','yyyyMMdd'))),' 18:00:00')) --picking_finish_time小于当天18：00
                           and
                         (   a.is_shiped != 1 or
                             a.shipping_time >=
                             concat(to_date(FROM_UNIXTIME(unix_timestamp('${data_date}','yyyyMMdd'))),' 23:59:59')
                         ) and
                         (a.is_shiped in (6) 
                            or
                            (  a.is_shiped in (1, 3) 
                                and
                            a.order_pack_time >= concat(to_date(FROM_UNIXTIME(unix_timestamp('${data_date}','yyyyMMdd'))),' 23:59:59')
                            )
                         ) then
                     2
                    when outing_stock_time <=
                        unix_timestamp(concat(to_date(FROM_UNIXTIME(unix_timestamp('${data_date}','yyyyMMdd'))),' 18:00:00'))
                          and
                         (a.is_shiped != 1 or
                         a.shipping_time >=
                         concat(to_date(FROM_UNIXTIME(unix_timestamp('${data_date}','yyyyMMdd'))),' 23:59:59')) and
                         (a.is_shiped in (3) or
                         (a.is_shiped in (1) and
                         a.shipping_time >=
                         concat(to_date(FROM_UNIXTIME(unix_timestamp('${data_date}','yyyyMMdd'))),' 23:59:59'))) then
                     3
                    else
                     0
                  end) AS is_status
FROM zydb.rpt_depot_order_tmp a
left join  k on a.order_sn = k.order_sn
left join  m on a.order_sn = m.order_sn
where a.pay_status in (1, 3)
and a.is_check = 1
and a.order_status = 1
and a.is_shiped in (1, 3, 6, 7, 8)
)

select * 
FROM t 
where is_status >= 1;



-- 客户端 ===================================================================================
WITH 
-- outing_stock_time
k AS
(select k.order_sn 
        ,max(k.gmt_created) outing_stock_time
FROM jolly.who_wms_outing_stock_detail   k
where gmt_created > unix_timestamp(to_date(date_sub(FROM_UNIXTIME(unix_timestamp('$[&data_date]','yyyyMMdd')),29)), 'yyyy-MM-dd')  
    and gmt_created < unix_timestamp(to_date(date_add(FROM_UNIXTIME(unix_timestamp('$[&data_date]','yyyyMMdd')),1)), 'yyyy-MM-dd')  
group by k.order_sn
), 
-- picking_finish_time
m AS
(select m.order_sn
        ,max(l.finish_time) picking_finish_time
FROM jolly.who_wms_picking_info  l
inner join  jolly.who_wms_picking_goods_detail   m
on l.picking_id = m.picking_id
where unix_timestamp(to_date(FROM_UNIXTIME(l.finish_time)))<=unix_timestamp(to_date(FROM_UNIXTIME(unix_timestamp('$[&data_date]','yyyyMMdd'))), 'yyyy-MM-dd') 
group by m.order_sn
), 
t as
(select to_date(a.shipping_time) data_date,
                  a.order_sn,
                  a.depot_id,
                  (case when a.pay_id = 41 then a.pay_time else a.result_pay_time end) AS pay_time,
                  a.no_problems_order_uptime,
                  FROM_UNIXTIME(outing_stock_time) AS outing_stock_time,
                  FROM_UNIXTIME(picking_finish_time) AS picking_finish_time,
                  a.order_pack_time,
                  a.shipping_time,
                  a.is_shiped,
                  a.is_check,
                  a.order_status,
                  (case
                    when outing_stock_time <=
                    unix_timestamp(concat(to_date(FROM_UNIXTIME(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 18:00:00'))
                         and
                         (   a.is_shiped != 1 or
                             a.shipping_time >=
                             concat(to_date(FROM_UNIXTIME(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 23:59:59')
                          ) and
                         (
                             (a.is_shiped in (7, 8) and picking_finish_time is null) or
                             picking_finish_time >=
                             unix_timestamp(concat(to_date(FROM_UNIXTIME(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 23:59:59'))
                         ) then
                     1
                    when outing_stock_time <=
                        unix_timestamp(concat(to_date(FROM_UNIXTIME(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 18:00:00')) --picking_finish_time小于当天18：00
                           and
                         (   a.is_shiped != 1 or
                             a.shipping_time >=
                             concat(to_date(FROM_UNIXTIME(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 23:59:59')
                         ) and
                         (a.is_shiped in (6) 
                            or
                            (  a.is_shiped in (1, 3) 
                                and
                            a.order_pack_time >= concat(to_date(FROM_UNIXTIME(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 23:59:59')
                            )
                         ) then
                     2
                    when outing_stock_time <=
                        unix_timestamp(concat(to_date(FROM_UNIXTIME(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 18:00:00'))
                          and
                         (a.is_shiped != 1 or
                         a.shipping_time >=
                         concat(to_date(FROM_UNIXTIME(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 23:59:59')) and
                         (a.is_shiped in (3) or
                         (a.is_shiped in (1) and
                         a.shipping_time >=
                         concat(to_date(FROM_UNIXTIME(unix_timestamp('$[&data_date]','yyyyMMdd'))),' 23:59:59'))) then
                     3
                    else
                     0
                  end) AS is_status
FROM zydb.rpt_depot_order_tmp a
left join  k on a.order_sn = k.order_sn
left join  m on a.order_sn = m.order_sn
where a.pay_status in (1, 3)
and a.is_check = 1
and a.order_status = 1
and a.is_shiped in (1, 3, 6, 7, 8)
)

select * 
FROM t 
where is_status >= 1;