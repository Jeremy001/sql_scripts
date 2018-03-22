
set mapreduce.map.memory.mb=6120;
set mapreduce.reduce.memory.mb=12000;
set hive.exec.parallel=true;

--·ÖÅä¶©µ¥Á¿¡¢µ¥Á¿Õ¼±È
insert overwrite table zydb.rpt_depot_daily_report_new_tmp1
select
  t0.depot_id,
  orders_num,
  prop_orders,
  receipt_goods_num,
  check_goods_num,
  onshelf_goods,
  exp_check_goods_num,
  receipt_quality_duration,
  quality_onshelf_duration,
  nvl(receipt_quality_duration,0)+nvl(quality_onshelf_duration,0) as receipt_onself_duration,
  picked_order_num,
  picked_goods_num,
  picking_duration,
  packed_order_num,
  packed_goods_num,
  shipped_order_num,
  shipped_goods_num,
  package_duration,
  shipping_duration,
  picking_duration+package_duration+shipping_duration picking_shiping_duration,     --³ö¿âÊ±³¤
  pickup_orders,
  pickup_time
 from
zydb.dim_dw_depot t0
full join
(
	select order_count orders_num,depod_id depot_id,round(order_count/order_sum,3) prop_orders
	from
	(
		select order_count,depod_id ,sum(order_count) over() order_sum
		from
		(
			  select count(order_id) order_count,depod_id
			   from zydb.dw_order_sub_order_fact a
			  where
			  case when pay_id=41 then pay_time  else result_pay_time end >=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
			  and
			  case when pay_id=41 then pay_time else result_pay_time end <date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
			  and pay_status in(1,3)
			  group by depod_id

		)a
	)a
) t1
on t0.depot_id=t1.depot_id
full join
(
--²Ö¿âµ½»õÇ©ÊÕÉÌÆ·¼þÊý
  select    sum(should_num) receipt_goods_num,depot_id
  from jolly.who_wms_delivered_receipt_info a
  where gmt_created >=unix_timestamp('${data_date}','yyyyMMdd')
  and gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
  group by depot_id
) t2
on t1.depot_id=t2.depot_id
full join
(
--ÖÊ¼ìÍê³ÉÉÌÆ·¼þÊý  --ÉÏ¼ÜÉÌÆ·¼þÊý

	select a.depot_id,sum(on_shelf_num) onshelf_goods
	from
	  (
		  select on_shelf_id,depot_id,on_shelf_num,is_new from jolly.who_wms_on_shelf_goods a
		  union all
		  select on_shelf_id,depot_id,on_shelf_num,3 is_new from jolly_wms.who_wms_on_shelf_goods b
	  )a
	  left join
	  (
		  select on_shelf_id,on_shelf_finish_time,is_new from jolly.who_wms_on_shelf_info a
		  union all
		  select on_shelf_id,on_shelf_finish_time,3 is_new from jolly_wms.who_wms_on_shelf_info b
	  )b
	  on a.on_shelf_id=b.on_shelf_id
	  and a.is_new=b.is_new
    where  b.on_shelf_finish_time >=unix_timestamp('${data_date}','yyyyMMdd')
	  and b.on_shelf_finish_time < unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
	group by a.depot_id

) t3
on t1.depot_id=t3.depot_id
full join
(
	--ÖÊ¼ìÒì³£ÉÌÆ·¼þÊý
	select depot_id,sum(exp_num) exp_check_goods_num from
	jolly.who_wms_delivered_order_exp_goods a
	where a.gmt_created >=unix_timestamp('${data_date}','yyyyMMdd')
	and a.gmt_created < unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
	group by depot_id
)t4
on t1.depot_id=t4.depot_id
full join
(
	--ÖÊ¼ìÊ±³¤
	select depot_id,
       sum(((unix_timestamp(on_shelf_start_time)-unix_timestamp(start_receipt_time))/3600/24)*num)/sum(num)*24  as receipt_quality_duration
	from zydb.dw_delivered_receipt_onself a
	 where a.on_shelf_start_time >=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
	   and a.on_shelf_start_time < date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
	   and ((unix_timestamp(on_shelf_start_time)-unix_timestamp(start_receipt_time))/3600/24)>0
	 group by depot_id
)t5
on t1.depot_id=t5.depot_id
full join

(
	--ÉÏ¼ÜÊ±³¤
    	select depot_id,
    	  sum((unix_timestamp(on_shelf_finish_time)-unix_timestamp(on_shelf_start_time))*on_shelf_num)/sum(on_shelf_num)/3600 quality_onshelf_duration
    	from
    	(
  		    select a.depot_id,
              	 c.on_shelf_num,
              	 c.on_shelf_start_time,
              	 c.on_shelf_finish_time
      		from zydb.dw_order_node_time a
      		left join  zydb.dw_demand_pur b on a.order_id=b.order_id
      		left join  zydb.dw_delivered_receipt_onself c
      		  on b.pur_order_sn=c.delivered_order_sn
      		 and b.sku_id=c.sku_id
      		where  to_date(shipping_time)=to_Date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
      		and unix_timestamp(on_shelf_finish_time)>unix_timestamp(on_shelf_start_time)
      		and source_type=2
    	)a
    	group by depot_id
)t6
on t1.depot_id=t6.depot_id
full join

(
	--¼ð»õ¶©µ¥Êý (µ«²»Ò»¶¨Íê³É£¬ºóÃæÓÐÒ»¸öÒÑÍê³ÉµÄ¼ÆËã) --¼ð»õÉÌÆ·¼þÊý
	select a.depot_id ,count(distinct order_sn) picked_order_num
	from
	(
    	select a.depot_id,b.order_sn,picking_total_num
    	from
    	(
          	select  depot_id,picking_total_num,picking_id,gmt_created from  jolly.who_wms_picking_info
          	union all
          	select  depot_id,picking_total_num,picking_id,gmt_created from  jolly_wms.who_wms_picking_info
    	) a,
    	(
          	select  order_sn,picking_id from  jolly.who_wms_picking_goods_detail
          	union all
          	select  order_sn,picking_id from  jolly_wms.who_wms_picking_goods_detail
    	) b
    	where
    		a.picking_id=b.picking_id
    	and gmt_created>=unix_timestamp('${data_date}','yyyyMMdd')
    	and gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
	)a
	group by a.depot_id
)t7
on t1.depot_id=t7.depot_id
full join

(
--¼ð»õÊ±³¤
    select depot_id,
    	sum(unix_timestamp(picking_finish_time) -unix_timestamp(outing_stock_time))/count(*)/3600 picking_duration
    	from  zydb.dw_order_node_time
    	where to_Date(picking_finish_time)=to_Date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
    group by depot_id
)t8
on t1.depot_id=t8.depot_id
full join

(
	--´ò°ü¶©µ¥Êý --´ò°üÉÌÆ·¼þÊý --·¢»õ¶©µ¥Êý  --·¢»õÉÌÆ·¼þÊý
	select  depod_id depot_id,
			sum(case when order_pack_time>=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')) and order_pack_time<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1) then 1 end) packed_order_num,
			sum(case when order_pack_time>=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')) and order_pack_time<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1) then original_goods_number  end) packed_goods_num,
			sum(case when shipping_time>=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')) 	and shipping_time<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1) then 1 end) shipped_order_num,
			sum(case when shipping_time>=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')) 	and shipping_time<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1) then original_goods_number  end) shipped_goods_num
	from
	zydb.dw_order_sub_order_fact
	group by depod_id
)t9
on t1.depot_id=t9.depot_id
full join

(
	--´ò°üÊ±³¤
   	select depot_id,
    	sum(unix_timestamp(order_pack_time) -unix_timestamp(picking_finish_time))/count(*)/3600 package_duration
    	from  zydb.dw_order_node_time
    	where to_Date(order_pack_time)=to_Date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
      group by depot_id

)t10
on t1.depot_id=t10.depot_id
full join

(
--·¢»õÊ±³¤
    	select depot_id,
    	sum(unix_timestamp(shipping_time) -unix_timestamp(order_pack_time))/count(*)/3600 shipping_duration
    	from  zydb.dw_order_node_time
    	where to_Date(shipping_time)=to_Date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
        group by depot_id
)t11
on t1.depot_id=t11.depot_id
full join



(
--Ìá»õ¶©µ¥Êý
	select depod_id depot_id,count(*) pickup_orders from
	zydb.dw_order_node_time a
	left join
	zydb.dw_order_sub_order_fact b
	on a.order_id=b.order_id
	where lading_time>=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
	and lading_time<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
	group by depod_id
)t12
on t1.depot_id=t12.depot_id
full join

(
	--Ìá»õÊ±³¤
	select depod_id depot_id,sum(unix_timestamp(b.lading_time)-unix_timestamp(a.shipping_time))/3600/count(*) pickup_time
	 from
	zydb.dw_order_sub_order_fact a
	left join
	zydb.dw_order_node_time  b
	on a.order_id=b.order_id
	where lading_time>=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
	and lading_time<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
	group by depod_id
)t13
on t1.depot_id=t13.depot_id
full join

(
	--¼ñ»õÉÌÆ·Êý
	select  depot_id,sum(picking_total_num) picked_goods_num
    from
    (
          	select  depot_id,picking_total_num,picking_id,gmt_created from  jolly.who_wms_picking_info
          	union all
          	select  depot_id,picking_total_num,picking_id,gmt_created from  jolly_wms.who_wms_picking_info
    	) a
    	where  gmt_created>=unix_timestamp('${data_date}','yyyyMMdd')
    	and gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
	group by depot_id
)t14
on t1.depot_id=t14.depot_id
full join
(
--ÖÊ¼ìÉÌÆ·Êý
	select a.depot_id,sum(checked_num) check_goods_num
	from
	(
		select depot_id,checked_num,gmt_created from  jolly.who_wms_on_shelf_goods a
		union all
		select depot_id,checked_num,gmt_created from  jolly_wms.who_wms_on_shelf_goods a
	)a
   where a.gmt_created >=unix_timestamp('${data_date}','yyyyMMdd')
  and a.gmt_created < unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
  	group by depot_id

)t15
on t1.depot_id=t15.depot_id

;


set mapreduce.map.memory.mb=6120;
set mapreduce.reduce.memory.mb=12000;
set hive.exec.parallel=true;

---------------½øÏú´æ

insert overwrite table zydb.rpt_depot_daily_report_new_tmp2
select
  t0.depot_id,
  pur_onself_goods_num,
  allocate_onself_goods_num,
  return_onself_goods_num,
  return_onself_order_num,
  other_onself_goods_num,
  other_out_goods_num,
  inven_profit_goods_num,
  inven_loss_goods_num,
  sale_out_goods_num,
  sale_out_order_num,
  allocate_out_goods_num,
  allocate_out_order_num,
  supp_return_out_goods_num,
  supp_return_num,
  total_stock,
  free_stock

 from
zydb.dim_dw_depot t0
full join
(
	--²É¹ºÈë¿âÊý  µ÷²¦Èë¿âÊý
	select  b.depot_id,
			nvl(sum(case when source_type in(1,2,4) then a.on_shelf_num end),0) pur_onself_goods_num,
			nvl(sum(case when source_type=3 then a.on_shelf_num end),0) allocate_onself_goods_num
	from jolly.who_wms_on_shelf_goods_price a
	left join
    jolly.who_wms_on_shelf_goods b
    on b.rec_id=a.on_shelf_goods_rec_id
    and a.is_new=b.is_new
	left join
	jolly.who_wms_on_shelf_info c
	on b.on_shelf_id=c.on_shelf_id
	and b.is_new=c.is_new
	where
	a.gmt_created>=unix_timestamp('${data_date}','yyyyMMdd')
	and a.gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd' )
	group by b.depot_id
)t1
on t0.depot_id=t1.depot_id
full join

(
--ÏúÊÛÍË»õÈë¿âÊý   ÏúÊÛÍË»õÈë¿â¶©µ¥Êý

	select depot_id,sum(nvl(change_num,0))  return_onself_goods_num,
		   0 return_onself_order_num
	from jolly.who_wms_goods_stock_detail_log
	where change_type=3
	 and  change_time>=unix_timestamp('${data_date}','yyyyMMdd')
	 and  change_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
	group by  depot_id

)t2
on t0.depot_id=t2.depot_id
full join
(
	--ÆäËûÈë¿âÊý(ÅÌÓ®¡¢ÊÖ¹¤)
	--ÆäËû³ö¿âÊý
	--ÅÌÓ¯\ÅÌ¿÷	ÉÌÆ·¼þÊý
	select  a.depot_id depot_id,
			nvl(sum(case when change_type in(4,9) then change_num end ),0)  other_onself_goods_num,
			nvl(sum(case when change_type in(6,10) then change_num end ),0)  other_out_goods_num,
			sum(case when change_type=4 then change_num end) inven_profit_goods_num,   --ÅÌÓ¯
			sum(case when change_type=6 then change_num end) inven_loss_goods_num  --ÅÌ¿÷
	from
	 (
		 select depot_id,change_type,change_num,change_time from  jolly.who_wms_goods_stock_detail_log a
		 union all
		 select depot_id,change_type,change_num,change_time from  jolly_wms.who_wms_goods_stock_detail_log a
	 )a
	where  change_time>=unix_timestamp('${data_date}','yyyyMMdd')
	and change_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
	group by a.depot_id
)t3
on t0.depot_id=t3.depot_id
full join

(
	--ÏúÊÛ³ö¿âÉÌÆ·Êý
	select depod_id depot_id,nvl(sum(goods_send_num),0)  sale_out_goods_num
	from zydb.dw_order_sub_order_fact a
	full join
	jolly.who_order_goods b
	on a.order_id=b.order_id
	where
	shipping_time >=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
	and
	shipping_time <date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
	group by depod_id
)t4
on t0.depot_id=t4.depot_id
full join

(
	--ÏúÊÛ³ö¿âµ¥Êý
	select depod_id depot_id,count(*)  sale_out_order_num
	from zydb.dw_order_sub_order_fact a
	where
	shipping_time >=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
	and
	shipping_time <date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
	group by depod_id
)t5
on t0.depot_id=t5.depot_id
full join

(
	--µ÷²¦³ö¿âÊý
	select  a.from_depot_id depot_id,
				nvl(sum(total_num),0) allocate_out_goods_num
	from jolly.who_wms_allocate_out_info a
	where out_time>=unix_timestamp('${data_date}','yyyyMMdd')
	and out_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
	group by a.from_depot_id
)t6
on t0.depot_id=t6.depot_id
full join
(
	--µ÷²¦³ö¿âµ¥Êý
	select  a.from_depot_id depot_id,
				count(distinct allocate_out_id)  allocate_out_order_num
	from jolly.who_wms_allocate_out_info a
	where out_time>=unix_timestamp('${data_date}','yyyyMMdd')
	and out_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
	group by a.from_depot_id
)t7
on t0.depot_id=t7.depot_id
full join

(
	--¹©Ó¦ÉÌÍË»õ³ö¿âÉÌÆ·Êý
	select  a.depot_id,
				nvl(sum(returned_num),0)  supp_return_out_goods_num
	from  jolly.who_wms_pur_returned_info a
	full join
	jolly.who_wms_pur_returned_goods b
	on a.returned_order_id=b.returned_order_id
	where a.returned_time>=unix_timestamp('${data_date}','yyyyMMdd')
	and a.returned_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')

	group by a.depot_id
)t8
on t0.depot_id=t8.depot_id
full join
(
	--×Ü¿â´æÊý
	select  a.depot_id,
				sum(stock_num) total_stock
	from  zydb.ods_who_wms_goods_stock_detail a
	where data_date='${data_date}'
	group by a.depot_id
)t9
on t0.depot_id=t9.depot_id
full join

(
--×ÔÓÉ¿â´æÊý
select a.depot_id,
      sum(  a.total_stock_num-a.total_order_lock_num-a.total_allocate_lock_num-a.total_return_lock_num)    free_stock
  from
zydb.ods_who_wms_goods_stock_total_detail  a
    where   data_date='${data_date}'
group by a.depot_id
)t10
on t0.depot_id=t10.depot_id

full join
(
	--¹©Ó¦ÉÌÍË»õ³ö¿âÊý
	select
		depot_id,
		nvl(count(distinct returned_order_id),0) supp_return_num
	from
	jolly.who_wms_purchase_returned_info a
	where a.returned_time>=unix_timestamp('${data_date}','yyyyMMdd')
	and a.returned_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
	group by depot_id
)t11
on t0.depot_id=t11.depot_id
;



set mapreduce.map.memory.mb=6120;
set mapreduce.reduce.memory.mb=12000;
set hive.exec.parallel=true;
---------------´ý´¦ÀíÁ¿

insert overwrite table zydb.rpt_depot_daily_report_new_tmp3
select
   t0.depot_id,
   unrec_goods_num_before2days,
   unrec_goods_num_1days,
   unrec_goods_num_days,
   send_goods_num_1days,
   send_goods_num_days,
   unprepare_order_num_before3days,
   unprepare_order_num_3days,
   unprepare_order_num_2days,
   unprepare_order_num_1days,
   pay_order_num_3days,
   pay_order_num_2days,
   pay_order_num_1days,
   unshipping_prepare_order_num,
   prepare_order_num_3days,
   prepare_order_num_2days,
   prepare_order_num_1days,
   prepare_goods_num_3days,
   prepare_goods_num_2days,
   prepare_goods_num_1days,
   unship_prepare_order_num_3days,
   unship_prepare_order_num_2days,
   unship_prepare_order_num_1days,
   unship_prepare_goods_num_3days,
   unship_prepare_goods_num_2days,
   unship_prepare_goods_num_1days
 from
zydb.dim_dw_depot t0
full join
(
--´óÓÚ2Ìì¹©Ó¦ÉÌ·¢»õÎ´µ½»õ
--£¨1ÌìÇ°¹©Ó¦ÉÌ·¢»õÎ´µ½»õ£©Î´µ½»õÉÌÆ·Êý
--£¨µ±Ìì¹©Ó¦ÉÌ·¢»õÎ´µ½»õ£©Î´µ½»õÉÌÆ·Êý
 select a.depot_id,
        sum(case when a.gmt_created<unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),2),'yyyy-MM-dd')
		and a.gmt_created>=unix_timestamp('2017-10-01','yyyy-MM-dd') then supp_num end ) unrec_goods_num_before2days,

		sum(case when a.gmt_created>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd') and a.gmt_created<unix_timestamp('${data_date}','yyyyMMdd')  then supp_num end )unrec_goods_num_1days,

		sum(case when a.gmt_created>=unix_timestamp('${data_date}','yyyyMMdd') and a.gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd') then supp_num end ) unrec_goods_num_days
 from
 jolly_spm.jolly_spm_pur_order_info  a
 full join
 jolly_spm.jolly_spm_pur_order_goods b
 on a.pur_order_id=b.pur_order_id
 full join
 zydb.dw_delivered_order_info c
 on a.pur_order_sn=c.delivered_order_sn

 where nvl(c.end_receipt_time,start_onself_time) is null
 group by  a.depot_id

) t1
on t0.depot_id=t1.depot_id
full join

(
	--£¨1ÌìÇ°¹©Ó¦ÉÌ·¢»õÎ´µ½»õ£©¹©Ó¦ÉÌ·¢»õÉÌÆ·Êý
	--£¨µ±Ìì¹©Ó¦ÉÌ·¢»õÎ´µ½»õ£©¹©Ó¦ÉÌ·¢»õÉÌÆ·Êý
	 select depot_id,
			sum(case when a.gmt_created>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd') and a.gmt_created<unix_timestamp('${data_date}','yyyyMMdd')  then supp_num end ) send_goods_num_1days,

			sum(case when a.gmt_created>=unix_timestamp('${data_date}','yyyyMMdd') and a.gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd') then supp_num end )  send_goods_num_days
	from
	 jolly_spm.jolly_spm_pur_order_info a
	  full join
	 jolly_spm.jolly_spm_pur_order_goods b
	 on a.pur_order_id=b.pur_order_id
	 group by depot_id
)t2
on t0.depot_id=t2.depot_id
full join

(
	--´óÓÚ3Ìì¸¶¿î¶©µ¥Î´Åä»õÍê³É¶©µ¥Êý
	--£¨3ÌìÇ°¸¶¿î¶©µ¥²Ö¿âÎ´Åä»õÍê³É£©Î´Åä»õÍê³É¸¶¿î¶©µ¥Êý
	--£¨2ÌìÇ°¸¶¿î¶©µ¥²Ö¿âÎ´Åä»õÍê³É£©Î´Åä»õÍê³É¸¶¿î¶©µ¥Êý
	--£¨1ÌìÇ°¸¶¿î¶©µ¥²Ö¿âÎ´Åä»õÍê³É£©Î´Åä»õÍê³É¸¶¿î¶©µ¥Êý
	select  a.depod_id depot_id,
			count(distinct case when case when pay_id=41 then to_date(pay_time) else to_date(result_pay_time) end <date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),3)   then   a.order_id end) unprepare_order_num_before3days,

			count(distinct case when case when pay_id=41 then to_date(pay_time) else to_date(result_pay_time) end =to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),3))  then   a.order_id end) unprepare_order_num_3days,

		    count(distinct case when case when pay_id=41 then to_date(pay_time) else to_date(result_pay_time) end =to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),2))  then   a.order_id end) unprepare_order_num_2days,

		    count(distinct case when case when pay_id=41 then to_date(pay_time) else to_date(result_pay_time) end =to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1))  then   a.order_id end) unprepare_order_num_1days
	from
	zydb.dw_order_sub_order_fact a
	left join
	(
	  select order_id,rec_id from jolly.who_wms_outing_stock_detail
	  union all
	  select order_id,rec_id from jolly.who_wms_outing_stock_detail_history
	  union all
	  select order_id,rec_id from jolly_wms.who_wms_outing_stock_detail
	) b
	on a.order_id=b.order_id
	where a.pay_status in (1,3)
	and a.is_problems_order != 1
	and b.rec_id is null
	and a.add_time>'2017-08-01'
	group by a.depod_id
)t3
on t0.depot_id=t3.depot_id
full join

(
	--£¨3ÌìÇ°¸¶¿î¶©µ¥²Ö¿âÎ´Åä»õÍê³É£©¸¶¿î¶©µ¥Êý
	--£¨2ÌìÇ°¸¶¿î¶©µ¥²Ö¿âÎ´Åä»õÍê³É£©¸¶¿î¶©µ¥Êý
	--£¨1ÌìÇ°¸¶¿î¶©µ¥²Ö¿âÎ´Åä»õÍê³É£©¸¶¿î¶©µ¥Êý
	select a.depod_id depot_id,
		   count( case when case when pay_id=41 then to_date(pay_time) else to_date(result_pay_time) end =to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),3))  then   a.order_id end) pay_order_num_3days,
		   count( case when case when pay_id=41 then to_date(pay_time) else to_date(result_pay_time) end =to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),2))  then   a.order_id end) pay_order_num_2days,
		   count( case when case when pay_id=41 then to_date(pay_time) else to_date(result_pay_time) end =to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1))  then   a.order_id end) pay_order_num_1days
	from  zydb.dw_order_sub_order_fact a
	where pay_status in (1,3)
	group by a.depod_id
)t4
on t0.depot_id=t4.depot_id
full join

(
	-- ´óÓÚ3ÌìÅä»õÍê³É²Ö¿âÎ´·¢»õ¶©µ¥Êý
		select a.depod_id depot_id,count(distinct a.order_id)  unshipping_prepare_order_num
	from zydb.dw_order_sub_order_fact a
	left join
	(
    	select order_id,max(gmt_created) outing_stock_time
      from
      (
    	  select order_id,gmt_created from  jolly.who_wms_outing_stock_detail
    	  union all
    	  select order_id,gmt_created from  jolly_wms.who_wms_outing_stock_detail --É³ÌØ
      )a
      group by order_id
	)b
	on a.order_id=b.order_id
	where is_shiped<>1
	and order_status<>2
	and outing_stock_time<unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),3),'yyyy-MM-dd')
	and outing_stock_time>=unix_timestamp('2017-10-01' ,'yyyy-MM-dd')
	and outing_stock_time>0
	group by a.depod_id

)t5
on t0.depot_id=t5.depot_id
full join

(
	--£¨3ÌìÇ°Åä»õÍê³É²Ö¿âÎ´·¢»õ£©Åä»õÍê³É¶©µ¥Êý
	--£¨2ÌìÇ°Åä»õÍê³É²Ö¿âÎ´·¢»õ£©Åä»õÍê³É¶©µ¥Êý
	--£¨1ÌìÇ°Åä»õÍê³É²Ö¿âÎ´·¢»õ£©Åä»õÍê³É¶©µ¥Êý
	select depot_id,
		   count(distinct case when
		   gmt_created>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),3),'yyyy-MM-dd') and
		   gmt_created<unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),2),'yyyy-MM-dd') then a.order_id end) prepare_order_num_3days,

		   count(distinct case when
		   gmt_created>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),2),'yyyy-MM-dd') and
		   gmt_created<unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd') then a.order_id end) prepare_order_num_2days,

		   count(distinct case when
		   gmt_created>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd') and
		   gmt_created<unix_timestamp('${data_date}','yyyyMMdd') then a.order_id end)
		   prepare_order_num_1days
	 from
	(
	select * from jolly.who_wms_outing_stock_detail
	union all
	select * from jolly_wms.who_wms_outing_stock_detail
	)a
	group by depot_id
)t6
on t0.depot_id=t6.depot_id
full join

(
	--£¨3ÌìÇ°Åä»õÍê³É²Ö¿âÎ´·¢»õ£©Åä»õÍê³ÉÉÌÆ·Êý
	--£¨2ÌìÇ°Åä»õÍê³É²Ö¿âÎ´·¢»õ£©Åä»õÍê³ÉÉÌÆ·Êý
	--£¨1ÌìÇ°Åä»õÍê³É²Ö¿âÎ´·¢»õ£©Åä»õÍê³ÉÉÌÆ·Êý
	select depot_id,
		   sum( case when
		   last_modified_time>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),3),'yyyy-MM-dd') and
		   last_modified_time<unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),2),'yyyy-MM-dd') then org_num end) prepare_goods_num_3days,

		   sum( case when
		   last_modified_time>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),2),'yyyy-MM-dd') and
		   last_modified_time<unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd') then org_num end) prepare_goods_num_2days,

		   sum( case when
		   last_modified_time>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd') and
		   last_modified_time<unix_timestamp('${data_date}','yyyyMMdd') then org_num end) prepare_goods_num_1days
	from
	jolly_oms.who_wms_goods_need_lock_detail a
	where num=0 and oos_num=0
	group by depot_id
)t7
on t0.depot_id=t7.depot_id
full join

(
	--£¨3ÌìÇ°Åä»õÍê³É²Ö¿âÎ´·¢»õ£©Î´·¢»õ¶©µ¥Êý
	--£¨2ÌìÇ°Åä»õÍê³É²Ö¿âÎ´·¢»õ£©Î´·¢»õ¶©µ¥Êý
	--£¨1ÌìÇ°Åä»õÍê³É²Ö¿âÎ´·¢»õ£©Î´·¢»õ¶©µ¥Êý
	select a.depod_id depot_id,
		   count(distinct case when
		   gmt_created>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),3),'yyyy-MM-dd') and
		   gmt_created<unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),2),'yyyy-MM-dd') then a.order_id end) unship_prepare_order_num_3days,

		   count(distinct case when
		   gmt_created>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),2),'yyyy-MM-dd') and
		   gmt_created<unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd') then a.order_id end) unship_prepare_order_num_2days,

		   count(distinct case when
		   gmt_created>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd') and
		   gmt_created<unix_timestamp('${data_date}','yyyyMMdd') then a.order_id end) unship_prepare_order_num_1days
	from
	zydb.dw_order_sub_order_fact  a
	inner join
	(
	select distinct order_id,gmt_created from jolly.who_wms_outing_stock_detail
	union all
	select distinct order_id,gmt_created from jolly_wms.who_wms_outing_stock_detail
	)b
	on a.order_id=b.order_id
	where is_shiped<>1
	and order_status in(1,4)
	and is_problems_order=2
	group by a.depod_id
) t8
on t0.depot_id=t8.depot_id
full join

(
	--£¨3ÌìÇ°Åä»õÍê³É²Ö¿âÎ´·¢»õ£©Î´·¢»õÉÌÆ·Êý
	--£¨2ÌìÇ°Åä»õÍê³É²Ö¿âÎ´·¢»õ£©Î´·¢»õÉÌÆ·Êý
	--£¨1ÌìÇ°Åä»õÍê³É²Ö¿âÎ´·¢»õ£©Î´·¢»õÉÌÆ·Êý
	select a.depod_id depot_id,
		   sum( case when
		   gmt_created>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),3),'yyyy-MM-dd') and
		   gmt_created<unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),2),'yyyy-MM-dd') then goods_number end) unship_prepare_goods_num_3days,

		   sum( case when
		   gmt_created>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),2),'yyyy-MM-dd') and
		   gmt_created<unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd') then goods_number end) unship_prepare_goods_num_2days,

		   sum( case when
		   gmt_created>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd') and
		   gmt_created<unix_timestamp('${data_date}','yyyyMMdd') then goods_number end) unship_prepare_goods_num_1days

	from zydb.dw_order_sub_order_fact  a
	inner join
	(
  	select distinct order_id,gmt_created from jolly.who_wms_outing_stock_detail
  	union all
  	select distinct order_id,gmt_created from jolly_wms.who_wms_outing_stock_detail
	) b
	on a.order_id=b.order_id
	where is_shiped<>1
	and order_status in(1,4)
	and is_problems_order=2
	group by a.depod_id

)t9
on t0.depot_id=t9.depot_id

;


set mapreduce.map.memory.mb=6120;
set mapreduce.reduce.memory.mb=12000;
set hive.exec.parallel=true;
----------------------»ýÑ¹Òì³£

insert overwrite table zydb.rpt_depot_daily_report_new_tmp4
select
	t0.depot_id,
    exp_goods_num,
    in_overstock_goods_num,
    pick_exp_orders_num,
    picked_orders,
    out_overstock_orders_num

 from
zydb.dim_dw_depot t0
full join
(
--µ½»õÒì³£ÉÌÆ·¼þÊý
select depot_id,
       sum(exp_num)  exp_goods_num
 from
 (
      select depot_id,exp_num,gmt_created from jolly.who_wms_delivered_order_exp_goods
      union all
      select depot_id,exp_num,gmt_created from jolly_wms.who_wms_delivered_order_exp_goods
  )a
where gmt_created>=unix_timestamp('${data_date}','yyyyMMdd')
  and gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')

group by depot_id
)t1
on t0.depot_id=t1.depot_id
full join

(
	--Èë¿â»ýÑ¹ÉÌÆ·¼þÊý	--µ±Ìì00:00µ½24:00²Ö¿âÇ©ÊÕÎïÁ÷°ü¹üÖÐÖÁ´ÎÈÕ24:00ÈÔÎ´ÖÊ¼ìµÄÉÌÆ·¼þÊý+µ±Ìì00:00µ½20:00ÖÊ¼ìÍê³ÉÉÌÆ·ÖÐÖÁ´ÎÈÕ6:00ÈÔÎ´Íê³ÉÉÏ¼ÜµÄÉÌÆ·¼þÊý£¨ÖÊ¼ì»ýÑ¹+ÉÏ¼Ü»ýÑ¹£©
	select nvl(a.depot_id,b.depot_id) depot_id,nvl(no_check_num,0)+nvl(no_onself,0)  in_overstock_goods_num
	from
	(
		--ÖÊ¼ì»ýÑ¹
			select a.depot_id,sum(a.delivered_num) no_check_num
			from
			(
				select distinct a.depot_id,delivered_num,checked_num,delivered_order_sn,sku_id
				from zydb.dw_delivered_receipt_onself a
				where start_receipt_time>=date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
				and end_receipt_time <from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
				and (on_shelf_start_time>=date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
					or on_shelf_start_time is null
					or on_shelf_start_time='1970-01-01 08:00:00')
				and a.exp_num=0
			)a
			left join
			jolly.who_wms_delivered_order_exp_goods b
			on a.delivered_order_sn=b.delivered_order_sn
			where b.delivered_order_sn is null
			group by a.depot_id
	)a
	full join
	(
		--ÉÏ¼Ü»ýÑ¹
		select a.depot_id,sum(num-on_shelf_num) no_onself
		from zydb.dw_delivered_receipt_onself  a
		left join
		jolly.who_wms_delivered_order_exp_goods b
		on a.delivered_order_sn=b.delivered_order_sn
		where on_shelf_start_time>=date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
		and on_shelf_start_time <concat(to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)),' 20:00:00')
		and (on_shelf_finish_time>=concat(to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))),' 06:00:00')
			  or on_shelf_finish_time is null
			  or on_shelf_finish_time='1970-01-01 08:00:00')
		and a.exp_num=0
		and b.delivered_order_sn is null
		group by a.depot_id
	)b
	on a.depot_id=b.depot_id
)t2
on t0.depot_id=t2.depot_id
full join

(
	--¼ð»õÒì³£¶©µ¥Êý
	select depot_id,count(distinct order_id) pick_exp_orders_num
	from jolly.who_wms_picking_exception_detail
	where gmt_created >= unix_timestamp('${data_date}','yyyyMMdd')
	and gmt_created < unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
	group by depot_id
/*   select p1.depot_id,
     count(distinct P1.returned_Order_Id) pick_exp_orders_num --¼ð»õÒì³£¶©µ¥
     From jolly.who_wms_returned_order_info  P1
	 left join
	 zydb.dw_order_sub_order_fact b
	 on P1.returned_order_id = b.order_id
	 where b.is_problems_order =2
     and p1.returned_time>=unix_timestamp(to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))),'yyyy-MM-dd')
     And P1.returned_time<unix_timestamp(to_date(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)),'yyyy-MM-dd')
     and return_reason = 24
     Group By p1.depot_id
*/
)t3
on t0.depot_id=t3.depot_id
full join

(
	--¼ð»õÍê³É¶©µ¥Êý µ±Ìì00:00ÖÁ24:00¼ð»õÍê³ÉµÄ¶©µ¥Êý
	select a.depot_id,count(distinct a.order_sn)  picked_orders
	from
	(
	select b.depot_id,b.order_sn,max(a.finish_time) finish_time
	from
	(
		select depot_id,picking_id,finish_time from jolly.who_wms_picking_info a
		union all
		select depot_id,picking_id,finish_time from jolly_wms.who_wms_picking_info a
	)a
	,
	(
		select * from jolly.who_wms_picking_goods_detail b
		union all
		select * from jolly_wms.who_wms_picking_goods_detail a
	)b

	where
		a.picking_id=b.picking_id
	and finish_time>=unix_timestamp('${data_date}','yyyyMMdd')
	group by b.depot_id,b.order_sn
	having max(a.finish_time)>=unix_timestamp('${data_date}','yyyyMMdd')
	and  max(a.finish_time)<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1) ,'yyyy-MM-dd')
	)a
	group by a.depot_id
)t4
on t0.depot_id=t4.depot_id
full join

(
	--³ö¿â»ýÑ¹¶©µ¥Êý
	--µ±Ìì00:00µ½18:00¿É¼ð»õ¶©µ¥ÖÐÖÁµ±Ìì24:00²Ö¿âÈÔÎ´·¢»õµÄ¶©µ¥Êý£¨¼ð»õ»ýÑ¹¶©µ¥Êý+´ò°ü»ýÑ¹¶©µ¥Êý+·¢»õ»ýÑ¹¶©µ¥Êý

	select depot_id,count(order_id) out_overstock_orders_num
	 from zydb.dw_order_node_time
	where  outing_stock_time>=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
	and outing_stock_time<from_unixtime(unix_timestamp(concat(to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))),' 18:00:00'),'yyyy-MM-dd HH:mm:ss'))
	and is_problems_order != 1
	and order_status=1
	and (shipping_time>=date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1) or shipping_time is null)
	group by depot_id

)t5
on t0.depot_id=t5.depot_id






---²Ö¿âÈÕ±¨
insert overwrite table zydb.rpt_depot_daily_report_new
select
  ${data_date} data_date,
  coalesce(t1.depot_id,t2.depot_id,t3.depot_id,t4.depot_id) depot_id,
  orders_num,
  prop_orders,
  receipt_goods_num,
  check_goods_num,
  onshelf_goods,
  exp_check_goods_num,
  receipt_quality_duration,
  quality_onshelf_duration,
  receipt_onself_duration,
  picked_order_num,
  picked_goods_num,
  picking_duration,
  packed_order_num,
  packed_goods_num,
  shipped_order_num,
  shipped_goods_num,
  package_duration,
  shipping_duration,
  picking_shiping_duration,
  pickup_orders,
  pickup_time,
  pur_onself_goods_num,
  allocate_onself_goods_num,
  return_onself_goods_num,
  return_onself_order_num,
  other_onself_goods_num,
  other_out_goods_num,
  inven_profit_goods_num,
  inven_loss_goods_num,
  sale_out_goods_num,
  sale_out_order_num,
  allocate_out_goods_num,
  allocate_out_order_num,
  supp_return_out_goods_num,
  supp_return_num,
  total_stock,
  free_stock,
  unrec_goods_num_before2days,
  unrec_goods_num_1days,
  unrec_goods_num_days,
   send_goods_num_1days,
   send_goods_num_days,
   unprepare_order_num_before3days,
   unprepare_order_num_3days,
   unprepare_order_num_2days,
   unprepare_order_num_1days,
   pay_order_num_3days,
   pay_order_num_2days,
   pay_order_num_1days,
   unshipping_prepare_order_num,
   prepare_order_num_3days,
   prepare_order_num_2days,
   prepare_order_num_1days,
   prepare_goods_num_3days,
   prepare_goods_num_2days,
   prepare_goods_num_1days,
   unship_prepare_order_num_3days,
   unship_prepare_order_num_2days,
   unship_prepare_order_num_1days,
   unship_prepare_goods_num_3days,
   unship_prepare_goods_num_2days,
   unship_prepare_goods_num_1days,
   exp_goods_num,
   in_overstock_goods_num,
   pick_exp_orders_num,
   picked_orders,
   out_overstock_orders_num

from
zydb.rpt_depot_daily_report_new_tmp1 t1
full join
zydb.rpt_depot_daily_report_new_tmp2 t2
on t1.depot_id=t2.depot_id
full join
zydb.rpt_depot_daily_report_new_tmp3 t3
on t1.depot_id=t3.depot_id
full join
zydb.rpt_depot_daily_report_new_tmp4 t4
on t1.depot_id=t4.depot_id
left join zydb.dim_dw_depot t5 on t1.depot_id=t5.depot_id
where
t5.status=1

union all

select * from zydb.rpt_depot_daily_report_new
where data_date<>${data_date}



























