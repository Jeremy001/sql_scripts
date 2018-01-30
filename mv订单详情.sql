--mv订单详情
select to_date(a.pay_time) pay_date,
        a.order_id,a.order_sn, c.oos_num ,c.create_time,
        d.user_id,d.shipping_id, d.shipping_name,d.goods_amount,d.order_total_amount,
        a.order_status, a.depot_id,a.depot_name,a.is_cod,a.is_problems_order,
        a.is_shiped,a.original_goods_number,a.goods_number,a.country_name,
      a.add_time, a.pay_time,a.order_check_time,
      a.lock_check_time,
      a.lock_last_modified_time,a.no_problems_order_uptime,a.problems_order_uptime,
      a.outing_stock_time,a.picking_time,a.picking_finish_time,
      a.order_pack_time,
     a.shipping_time,b.lading_time,
     b.leave_time,b.arrive_time,clear_time,deliver_time,b.receipt_time,
      b.refuse_time, b.return_time
FROM   zydb.dw_order_node_time a
left join zydb.dw_order_shipping_tracking_node b on a.order_id=b.order_id
inner join  zydb.dw_order_sub_order_fact  d  on a.order_id =d.order_id and d.site_id=602
left join
     (
     select c.order_id ,max(from_unixtime(c.create_time)) create_time, sum(c.oos_num) oos_num
     from jolly.who_wms_order_oos_log c
     inner join zydb.dw_order_node_time a on a.order_id=c.order_id
     where a.site_id=602
     group by c.order_id
     ) c
     on a.order_id=c.order_id
where 1=1
and a.site_id=602
and a.depot_id=8
and a.pay_status in (1, 3)
