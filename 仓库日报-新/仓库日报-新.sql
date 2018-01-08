
set mapreduce.map.memory.mb=6120;
set mapreduce.reduce.memory.mb=12000;
set hive.exec.parallel=true;

--分配订单量、单量占比
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
  picking_duration+package_duration+shipping_duration picking_shiping_duration,     --出库时长
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
			   from zydb.dw_order_sub_order_fact 
			  where 
			  depod_id in(4,5,6,7,14)
			  and case when pay_id=41 then pay_time  else result_pay_time end >=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
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
--仓库到货签收商品件数
  select    sum(should_num) receipt_goods_num,depot_id  
  from jolly.who_wms_delivered_receipt_info
  where gmt_created >=unix_timestamp('${data_date}','yyyyMMdd')
  and gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
  group by depot_id
) t2  
on t1.depot_id=t2.depot_id
full join 
(
--质检完成商品件数  --上架商品件数
	
	select a.depot_id,sum(on_shelf_num) onshelf_goods
	from  
	  (
		  select on_shelf_id,depot_id,on_shelf_num from jolly.who_wms_on_shelf_goods a 
		  union all
		  select on_shelf_id,depot_id,on_shelf_num from jolly_wms.who_wms_on_shelf_goods b
	  )a
	  left join
	  (
		  select on_shelf_id,on_shelf_finish_time from jolly.who_wms_on_shelf_info a
		  union all
		  select on_shelf_id,on_shelf_finish_time from jolly_wms.who_wms_on_shelf_info b
	  )b
	  on a.on_shelf_id=b.on_shelf_id
		 where  b.on_shelf_finish_time >=unix_timestamp('${data_date}','yyyyMMdd')
	  and b.on_shelf_finish_time < unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
	group by a.depot_id
	
) t3
on t1.depot_id=t3.depot_id
full join 
(
	--质检异常商品件数
	select depot_id,sum(exp_num) exp_check_goods_num from  
	jolly.who_wms_delivered_order_exp_goods a 
	where a.gmt_created >=unix_timestamp('${data_date}','yyyyMMdd')
	and a.gmt_created < unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
	group by depot_id
)t4
on t1.depot_id=t4.depot_id
full join 
(
	--质检时长
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
	--上架时长 
	select depot_id,
       sum(((unix_timestamp(on_shelf_finish_time)-unix_timestamp(on_shelf_start_time))/3600/24)*on_shelf_num)/sum(on_shelf_num)*24  as quality_onshelf_duration
	from zydb.dw_delivered_receipt_onself a
	 where a.on_shelf_finish_time >=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
	   and a.on_shelf_finish_time < date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
	   and a.on_shelf_start_time >=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
	   and a.on_shelf_start_time < date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
	 group by depot_id 
)t6
on t1.depot_id=t6.depot_id
full join 
 
(
	--拣货订单数 (但不一定完成，后面有一个已完成的计算) --拣货商品件数
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
--拣货时长
	select depod_id depot_id,sum(finish_time-gmt_created)/count(order_id)/3600 picking_duration
	from 
	(
		select   a.depod_id,a.order_id,c.finish_time ,b.gmt_created 
		from zydb.dw_order_sub_order_fact a
		left join  
		--可捡货
		(select order_id,max(gmt_created) gmt_created from jolly.who_wms_outing_stock_detail group by order_id
		  union all 
		 select order_id,max(gmt_created) gmt_created from jolly_wms.who_wms_outing_stock_detail group by order_id
		) b
		on a.order_id=b.order_id
		left join 
		(
		---捡货完成
			select order_id,max(finish_time) finish_time
			from
			jolly.who_wms_picking_goods_detail a
			left join  
			jolly.who_wms_picking_info b
			on a.picking_id=b.picking_id
			group by order_id
			
			union all 
			
			select order_id,max(finish_time) finish_time
			from
			jolly_wms.who_wms_picking_goods_detail a
			left join  
			jolly_wms.who_wms_picking_info b
			on a.picking_id=b.picking_id
			group by order_id
		)c
		on a.order_id=c.order_id
		
		where shipping_time>=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
		and shipping_time<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1) 
	)a
	group by depod_id
)t8
on t1.depot_id=t8.depot_id
full join  

(
	--打包订单数 --打包商品件数 --发货订单数  --发货商品件数
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

	--打包时长
	select a.depod_id depot_id,sum((unix_timestamp(a.order_pack_time)-c.finish_time)*b.real_picking_num)/sum(b.real_picking_num)/3600 package_duration
	from 
	zydb.dw_order_sub_order_fact  a
	left join 
	(
	 select order_id,picking_id,real_picking_num from jolly.who_wms_picking_goods_detail 
	 union all
	 select order_id,picking_id,real_picking_num from jolly_wms.who_wms_picking_goods_detail 
	) b
	on a.order_id=b.order_id
	left join  
	(
	 select picking_id,finish_time from jolly.who_wms_picking_info
	 union all
	 select picking_id,finish_time from jolly_wms.who_wms_picking_info
	)c
	on b.picking_id=c.picking_id
	where shipping_time>=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
	and shipping_time<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
	group by a.depod_id
)t10
on t1.depot_id=t10.depot_id
full join  

(
--发货时长
select depod_id depot_id,avg(unix_timestamp(shipping_time)-unix_timestamp(order_pack_time))/3600 shipping_duration
from 
zydb.dw_order_sub_order_fact  
where shipping_time>=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
and shipping_time<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
group by depod_id
)t11
on t1.depot_id=t11.depot_id
full join  



(
--提货订单数
	select depod_id depot_id,count(*) pickup_orders from 
	zydb.dw_order_shipping_tracking_node a
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
	--提货时长 
	select depod_id depot_id,sum(unix_timestamp(b.lading_time)-unix_timestamp(a.shipping_time))/3600/count(*) pickup_time
	 from
	zydb.dw_order_sub_order_fact a
	left join  
	zydb.dw_order_shipping_tracking_node  b
	on a.order_id=b.order_id
	where lading_time>=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
	and lading_time<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
	group by depod_id
)t13
on t1.depot_id=t13.depot_id
full join  

(
	--捡货商品数 
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
--质检商品数
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

---------------进销存

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
	--采购入库数  调拨入库数
	select  b.depot_id,
			nvl(sum(case when source_type in(1,2,4) then a.on_shelf_num end),0) pur_onself_goods_num,
			nvl(sum(case when source_type=3 then a.on_shelf_num end),0) allocate_onself_goods_num
	from jolly.who_wms_on_shelf_goods_price a
	left join
  jolly.who_wms_on_shelf_goods b
  on b.rec_id=a.on_shelf_goods_rec_id
	left join  
	jolly.who_wms_on_shelf_info c
	on b.on_shelf_id=c.on_shelf_id
	where 
	a.gmt_created>=unix_timestamp('${data_date}','yyyyMMdd')
	and a.gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd' ) 
	group by b.depot_id
)t1
on t0.depot_id=t1.depot_id
full join  

(
--销售退货入库数   销售退货入库订单数
	
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
	--其他入库数(盘赢、手工)
	--其他出库数
	--盘盈\盘亏	商品件数
	select  a.depot_id depot_id,
			nvl(sum(case when change_type in(4,9) then change_num end ),0)  other_onself_goods_num,
			nvl(sum(case when change_type in(6,10) then change_num end ),0)  other_out_goods_num,
			sum(case when change_type=4 then change_num end) inven_profit_goods_num,   --盘盈
			sum(case when change_type=6 then change_num end) inven_loss_goods_num  --盘亏	
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
	--销售出库商品数
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
	--销售出库单数
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
	--调拨出库数
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
	--调拨出库单数
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
	--供应商退货出库商品数
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
	--总库存数
	select  a.depot_id,
				sum(stock_num) total_stock
	from  zydb.ods_who_wms_goods_stock_detail a
	where data_date='${data_date}'
	group by a.depot_id
)t9
on t0.depot_id=t9.depot_id
full join  

(
--自由库存数
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
	--供应商退货出库数
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
---------------待处理量

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
--大于2天供应商发货未到货
--（1天前供应商发货未到货）未到货商品数
--（当天供应商发货未到货）未到货商品数
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
 
 where c.end_receipt_time is null 
group by  a.depot_id
) t1
on t0.depot_id=t1.depot_id
full join   
 
(
	--（1天前供应商发货未到货）供应商发货商品数
	--（当天供应商发货未到货）供应商发货商品数
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
	--大于3天付款订单未配货完成订单数
	--（3天前付款订单仓库未配货完成）未配货完成付款订单数
	--（2天前付款订单仓库未配货完成）未配货完成付款订单数
	--（1天前付款订单仓库未配货完成）未配货完成付款订单数
	select  a.depod_id depot_id,
			count(distinct case when case when pay_id=41 then to_date(pay_time) else to_date(result_pay_time) end <date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),3)   then   a.order_id end) unprepare_order_num_before3days,
			
			count(distinct case when case when pay_id=41 then to_date(pay_time) else to_date(result_pay_time) end =to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),3))  then   a.order_id end) unprepare_order_num_3days,
		   
		    count(distinct case when case when pay_id=41 then to_date(pay_time) else to_date(result_pay_time) end =to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),2))  then   a.order_id end) unprepare_order_num_2days,
		   
		    count(distinct case when case when pay_id=41 then to_date(pay_time) else to_date(result_pay_time) end =to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1))  then   a.order_id end) unprepare_order_num_1days
	from 
	zydb.dw_order_sub_order_fact  a
	left join  
	(
	  select order_id,rec_id from jolly.who_wms_outing_stock_detail 
	  union all
	  select order_id,rec_id from jolly.who_wms_outing_stock_detail_history
	  union all
	  select order_id,rec_id from jolly_wms.who_wms_outing_stock_detail
	) b
	on a.order_id=b.order_id
	where pay_status in (1,3)
	and  is_problems_order != 1
	and b.rec_id is null
	and a.add_time>'2017-08-01'
	group by a.depod_id 
)t3
on t0.depot_id=t3.depot_id
full join   

(
	--（3天前付款订单仓库未配货完成）付款订单数
	--（2天前付款订单仓库未配货完成）付款订单数
	--（1天前付款订单仓库未配货完成）付款订单数
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
	-- 大于3天配货完成仓库未发货订单数
		select a.depod_id depot_id,count(distinct a.order_id)  unshipping_prepare_order_num
	from zydb.dw_order_sub_order_fact a
	left join 
	(
    	select order_id,max(gmt_created) outing_stock_time
      from 
      (
    	  select order_id,gmt_created from  jolly.who_wms_outing_stock_detail
    	  union all 
    	  select order_id,gmt_created from  jolly_wms.who_wms_outing_stock_detail --沙特
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
	--（3天前配货完成仓库未发货）配货完成订单数
	--（2天前配货完成仓库未发货）配货完成订单数  
	--（1天前配货完成仓库未发货）配货完成订单数 
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
	--（3天前配货完成仓库未发货）配货完成商品数
	--（2天前配货完成仓库未发货）配货完成商品数
	--（1天前配货完成仓库未发货）配货完成商品数
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
	--（3天前配货完成仓库未发货）未发货订单数
	--（2天前配货完成仓库未发货）未发货订单数
	--（1天前配货完成仓库未发货）未发货订单数
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
	--（3天前配货完成仓库未发货）未发货商品数
	--（2天前配货完成仓库未发货）未发货商品数
	--（1天前配货完成仓库未发货）未发货商品数
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
----------------------积压异常

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
--到货异常商品件数
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
	--入库积压商品件数	--当天00:00到24:00仓库签收物流包裹中至次日24:00仍未质检的商品件数+当天00:00到20:00质检完成商品中至次日6:00仍未完成上架的商品件数（质检积压+上架积压）
	select nvl(a.depot_id,b.depot_id) depot_id,nvl(no_check_num,0)+nvl(no_onself,0)  in_overstock_goods_num
	from 
	(
		--质检积压
			select a.depot_id,sum(a.real_num-a.inspect_num) no_check_num 
			from
			(
				select distinct a.depot_id,tracking_no,real_num,inspect_num,delivered_order_sn 
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
		--上架积压
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
	--拣货异常订单数 
--	select depot_id,count(distinct order_id) pick_exp_orders_num
--	from jolly.who_wms_picking_exception_detail
--	where gmt_created>=unix_timestamp('${data_date}','yyyyMMdd')
--	and gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd') 
--	group by depot_id
     select p1.depot_id,
     count(distinct P1.returned_Order_Id) pick_exp_orders_num --拣货异常订单
     From jolly.who_wms_returned_order_info  P1   
	 left join 
	 zydb.dw_order_sub_order_fact b
	 on P1.returned_order_id=b.order_id
	 where b.is_problems_order =2
     and p1.returned_time>=unix_timestamp(to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))),'yyyy-MM-dd') 
     And P1.returned_time<unix_timestamp(to_date(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)),'yyyy-MM-dd')  
     and return_reason =24
     Group By p1.depot_id 
)t3
on t0.depot_id=t3.depot_id
full join 

(
	--拣货完成订单数 当天00:00至24:00拣货完成的订单数
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
	--出库积压订单数
	--当天00:00到18:00可拣货订单中至当天24:00仓库仍未发货的订单数（拣货积压订单数+打包积压订单数+发货积压订单数
	
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
 





---仓库日报
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
where 
coalesce(t1.depot_id,t2.depot_id,t3.depot_id,t4.depot_id) in(4,5,6,7)

union all

select * from zydb.rpt_depot_daily_report_new
where data_date<>${data_date}



























