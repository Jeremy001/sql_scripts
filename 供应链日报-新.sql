
set mapreduce.map.memory.mb=6120;
set mapreduce.reduce.memory.mb=12000;

set hive.exec.parallel=true;

--create table zydb.dpr_supply_chain_final_table_new1 as 
insert overwrite table zydb.dpr_supply_chain_final_table_new1
select t1.depot_area
      ,lt04
      ,lt01
      ,lt02
      ,lt34
      ,pay_order_num_7day
      ,pay_order_num_oos_7day
      ,pay_goods_num_7day
      ,pay_goods_num_oos_7day
      ,depot_mtd
      ,out_depot_mtd

from 
(
    --客户服务时长(LT04)
    select  depot_area,  
            sum(b.update_time - unix_timestamp(case when pay_id=41 then pay_time else result_pay_time end))/count(a.order_id)/3600/24   lt04  
     from zydb.dw_order_sub_order_fact a
    inner join
    (
        select order_id,update_time from (
          select order_id,update_time,row_number() over(partition by order_id order by id desc) rn from jolly.who_order_shipping_tracking a
          where update_time>=unix_timestamp('${data_date}','yyyyMMdd') 
          and update_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd') 
          and shipping_state =3
        )a 
        where rn=1
    )b
    on a.order_id=b.order_id
    inner join 
    zydb.dim_dw_depot c
    on a.depod_id=c.depot_id
    where c.depot_area in('cn','sa')
    group by depot_area

    union all

    select   'all_depot' depot_area,sum(b.update_time - unix_timestamp(case when pay_id=41 then pay_time else result_pay_time end))/count(a.order_id)/3600/24   lt04  
     from zydb.dw_order_sub_order_fact a
    inner join
    (
        select order_id,update_time from (
          select order_id,update_time,row_number() over(partition by order_id order by id desc) rn from jolly.who_order_shipping_tracking a
          where update_time>=unix_timestamp('${data_date}','yyyyMMdd') 
          and update_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd') 
          and shipping_state =3
        )a 
        where rn=1
    )b
    on a.order_id=b.order_id
    inner join 
    zydb.dim_dw_depot c
    on a.depod_id=c.depot_id
    where c.depot_area<>'' 
)t1
left join 
(
--采购时长(LT01)
--国内、本地仓
select  depot_area,sum(
				 (
					   unix_timestamp(least(nvl(start_receipt_time,on_shelf_start_time),on_shelf_start_time))-unix_timestamp(pay_time)
				 )
				*(lock_org_num-lock_oos_num))/sum(lock_org_num-lock_oos_num)/3600 LT01
	from (
		  select  b.rec_id,lock_org_num,lock_oos_num,case when a.pay_id=41 then a.pay_time else result_pay_time end   pay_time,d.depot_area,min(c.start_receipt_time) start_receipt_time,min(c.on_shelf_start_time) on_shelf_start_time
		  from 
		  zydb.dw_order_sub_order_fact a
		  left join
		  zydb.dw_demand_pur    b
		  on a.order_id=b.order_id
		  left join 
		  zydb.dw_delivered_receipt_onself   c
		  on b.pur_order_sn=c.delivered_order_sn
		  and b.sku_id=c.sku_id
		  inner join 
      zydb.dim_dw_depot d
      on a.depod_id=d.depot_id
		  where 
		  least(start_receipt_time,on_shelf_start_time)>=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
		  and least(start_receipt_time,on_shelf_start_time)<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1) 
		  and b.demand_type=1
		  and d.depot_area in('cn','sa')
		  group by b.rec_id,lock_org_num,lock_oos_num,case when a.pay_id=41 then a.pay_time else result_pay_time end ,d.depot_area
	)a 
group by depot_area

union all
--全球仓
  select  'all_depot' depot_area,sum(
				 (
					   unix_timestamp(least(nvl(start_receipt_time,on_shelf_start_time),on_shelf_start_time))-unix_timestamp(pay_time)
				 )
				*(lock_org_num-lock_oos_num))/sum(lock_org_num-lock_oos_num)/3600 LT01
	from (
		  select  b.rec_id,lock_org_num,lock_oos_num,case when a.pay_id=41 then a.pay_time else result_pay_time end  pay_time,d.depot_area,min(c.start_receipt_time) start_receipt_time,min(c.on_shelf_start_time) on_shelf_start_time
		  from 
		  zydb.dw_order_sub_order_fact a
		  left join
		  zydb.dw_demand_pur    b
		  on a.order_id=b.order_id
		  left join 
		  zydb.dw_delivered_receipt_onself   c
		  on b.pur_order_sn=c.delivered_order_sn
		  and b.sku_id=c.sku_id
		  inner join 
      zydb.dim_dw_depot d
      on a.depod_id=d.depot_id
		  where 
		  least(start_receipt_time,on_shelf_start_time)>=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
		  and least(start_receipt_time,on_shelf_start_time)<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1) 
		  and b.demand_type=1
		  and d.depot_area<>''
		  group by b.rec_id,lock_org_num,lock_oos_num,case when a.pay_id=41 then a.pay_time else result_pay_time end ,d.depot_area
	)a 
)t2
on t1.depot_area=t2.depot_area
left join 
(
--订单发货时长(LT02)
	select  b.depot_area,
	        sum(unix_timestamp(shipping_time)-unix_timestamp(case when pay_id=41 then pay_time else result_pay_time end))/count(*) /3600 lt02
	from zydb.dw_order_sub_order_fact a
	left join 
	zydb.dim_dw_depot b
	on a.depod_id=b.depot_id
	where to_date(shipping_time)=to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
		  and b.depot_area in('cn','sa')
	group by b.depot_area

	union all

	select  'all_depot' depot_area,
	        sum(unix_timestamp(shipping_time)-unix_timestamp(case when pay_id=41 then pay_time else result_pay_time end))/count(*) /3600 lt02
	from zydb.dw_order_sub_order_fact a
	left join 
	zydb.dim_dw_depot b
	on a.depod_id=b.depot_id
	where to_date(shipping_time)=to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
		  and b.depot_area<>''
)t3
on t1.depot_area=t3.depot_area
left join
(
--物流时长(LT34)
	select c.depot_area,
		  sum(b.update_time - unix_timestamp(shipping_time))/count(a.order_id)/3600/24 lt34
	 from zydb.dw_order_sub_order_fact a
	inner join
	(
		select order_id,update_time from (
		  select order_id,update_time,row_number() over(partition by order_id order by id desc) rn from jolly.who_order_shipping_tracking a
		  where update_time>=unix_timestamp('${data_date}','yyyyMMdd') 
		  and update_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd') 
		  and shipping_state =3
		)a 
		where rn=1
	)b
	on a.order_id=b.order_id
	left join 
	zydb.dim_dw_depot c
	on a.depod_id=c.depot_id
	where c.depot_area in('cn','sa')
	group by c.depot_area

	union all 

	select 'all_depot' depot_area,
		  sum(b.update_time - unix_timestamp(shipping_time))/count(a.order_id)/3600/24 lt34
	 from zydb.dw_order_sub_order_fact a
	inner join
	(
		select order_id,update_time from (
		  select order_id,update_time,row_number() over(partition by order_id order by id desc) rn from jolly.who_order_shipping_tracking a
		  where update_time>=unix_timestamp('${data_date}','yyyyMMdd') 
		  and update_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd') 
		  and shipping_state =3
		)a 
		where rn=1
	)b
	on a.order_id=b.order_id
	left join 
	zydb.dim_dw_depot c
	on a.depod_id=c.depot_id
	where c.depot_area<>'' 
)t4
on t1.depot_area=t4.depot_area
left join 
(
--7天前付款订单数
select depot_area,count(*) pay_order_num_7day
from zydb.dw_order_sub_order_fact a
left join 
zydb.dim_dw_depot b
on a.depod_id=b.depot_id
where 
to_date(case when pay_id=41 then pay_time else result_pay_time end)=to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),6))
and b.depot_area in('cn','sa')
and a.pay_status in (1,3) 
group by depot_area

union all

select 'all_depot' depot_area,count(*) pay_order_num_7day
from zydb.dw_order_sub_order_fact a
left join 
zydb.dim_dw_depot b
on a.depod_id=b.depot_id
where 
to_date(case when pay_id=41 then pay_time else result_pay_time end)=to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),6))
and b.depot_area<>'' 
and a.pay_status in (1,3)
)t5
on t1.depot_area=t5.depot_area
left join 
(
--7天前付款订单缺货数
select depot_area,count(*) pay_order_num_oos_7day
 from zydb.dw_order_sub_order_fact a
inner join 
(select distinct order_id from jolly.who_wms_order_oos_log ) b
on a.order_id=b.order_id  
left join 
zydb.dim_dw_depot c
on a.depod_id=c.depot_id
where 
to_date(case when pay_id=41 then pay_time else result_pay_time end)=to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),6))
and c.depot_area in('cn','sa')
and a.pay_status in (1,3)
group by depot_area

union all

select 'all_depot' depot_area,count(*) pay_order_num_oos_7day
 from zydb.dw_order_sub_order_fact a
inner join 
(select distinct order_id from jolly.who_wms_order_oos_log ) b
on a.order_id=b.order_id  
left join 
zydb.dim_dw_depot c
on a.depod_id=c.depot_id
where 
to_date(case when pay_id=41 then pay_time else result_pay_time end)=to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),6))
and c.depot_area<>'' 
and a.pay_status in (1,3)
)t6
on t1.depot_area=t6.depot_area
left join
(
--7天前付款订单商品数
select depot_area,sum(b.original_goods_number) pay_goods_num_7day
from zydb.dw_order_sub_order_fact a
left join zydb.dw_order_goods_fact b
on a.order_id=b.order_id
left join zydb.dim_dw_depot c
on a.depod_id=c.depot_id 
where 
to_date(case when a.pay_id=41 then a.pay_time else a.result_pay_time end)=to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),6))
and c.depot_area in('cn','sa')
and a.pay_status in (1,3)
group by depot_area

union all

select 'all_depot' depot_area,sum(b.original_goods_number) pay_goods_num_7day
from zydb.dw_order_sub_order_fact a
left join zydb.dw_order_goods_fact b
on a.order_id=b.order_id
left join zydb.dim_dw_depot c
on a.depod_id=c.depot_id
where 
to_date(case when a.pay_id=41 then a.pay_time else a.result_pay_time end)=to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),6))
and c.depot_area<>'' 
and a.pay_status in (1,3)
)t7
on t1.depot_area=t7.depot_area
left join
(
--7天前付款订单商品缺货数
select depot_area,sum(b.oos_num)  pay_goods_num_oos_7day
 from zydb.dw_order_sub_order_fact a
inner join 
(select  order_id,sum(oos_num) oos_num from jolly.who_wms_order_oos_log group by order_id) b
on a.order_id=b.order_id  
left join 
zydb.dim_dw_depot d
on a.depod_id=d.depot_id
where 
to_date(case when a.pay_id=41 then a.pay_time else a.result_pay_time end)=to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),6))
and depot_area in('cn','sa')
and a.pay_status in (1,3)
group by depot_area

union all

select 'all_depot' depot_area,sum(b.oos_num)   pay_goods_num_oos_7day
 from zydb.dw_order_sub_order_fact a
inner join 
(select  order_id,sum(oos_num) oos_num from jolly.who_wms_order_oos_log group by order_id) b
on a.order_id=b.order_id  
left join 
zydb.dim_dw_depot d
on a.depod_id=d.depot_id
where 
to_date(case when a.pay_id=41 then a.pay_time else a.result_pay_time end)=to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),6))
and depot_area<>'' 
and a.pay_status in (1,3)
)t8
on t1.depot_area=t8.depot_area
left join
(
---MTD库存总和
select depot_area,sum(stock_num)  depot_mtd
from zydb.ods_who_wms_goods_stock_detail a
left join 
zydb.dim_dw_depot b
on a.depot_id=b.depot_id
where substr(data_date,1,6)=substr('${data_date}',1,6)
and depot_area in('cn','sa')
group by depot_area

union all

select 'all_depot' depot_area,sum(stock_num)  depot_mtd
from zydb.ods_who_wms_goods_stock_detail a
left join 
zydb.dim_dw_depot b
on a.depot_id=b.depot_id
where substr(data_date,1,6)=substr('${data_date}',1,6)
and depot_area<>'' 
)t9
on t1.depot_area=t9.depot_area
left join
(
--MTD销售出库数总和(仓内)
select depot_area,
       sum(b.original_goods_number) out_depot_mtd
 from zydb.dw_order_sub_order_fact a 
left join zydb.dw_order_goods_fact b
on a.order_id=b.order_id
left join 
zydb.dim_dw_depot c
on a.depod_id=c.depot_id
where a.is_shiped=1
and depot_area in('cn','sa')
and a.shipping_time>=to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),day(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))-1))
and a.shipping_time<to_date(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1))
group by depot_area
 
union all

select 'all_depot' depot_area,
       sum(b.original_goods_number) out_depot_mtd
 from zydb.dw_order_sub_order_fact a 
left join zydb.dw_order_goods_fact b
on a.order_id=b.order_id
left join 
zydb.dim_dw_depot c
on a.depod_id=c.depot_id
where a.is_shiped=1
and depot_area<>''
and a.shipping_time>=to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),day(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))-1))
and a.shipping_time<to_date(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1))
)t10
on t1.depot_area=t10.depot_area
;



set mapreduce.map.memory.mb=6120;
set mapreduce.reduce.memory.mb=12000;

set hive.exec.parallel=true;


--create table zydb.rpt_supply_chain_final_table_new2 as 

insert overwrite table zydb.rpt_supply_chain_final_table_new2
select t1.depot_area,nvl(pur_shiped_order_onway_num,0)+nvl(total_stock_num,0)+nvl(allocate_order_onway_num,0)+nvl(return_onway,0)+nvl(ship_onway,0) depot_link_mtd,receive_mtd
from
(
--MTD全链路库存总和
	--采购在途
select depot_area,sum(pur_shiped_order_onway_num) pur_shiped_order_onway_num
from zydb.ods_wms_goods_stock_onway_total a
inner join 
zydb.dim_dw_depot b
on a.depot_id=b.depot_id
and depot_area in('cn','sa')
and data_date=${data_date}
group by depot_area
  
union all

select 'all_depot' depot_area,sum(pur_shiped_order_onway_num) pur_shiped_order_onway_num
from zydb.ods_wms_goods_stock_onway_total a
inner join 
zydb.dim_dw_depot b
on a.depot_id=b.depot_id
and data_date=${data_date}
and depot_area<>''  
)t1
left join
(
---在仓库存
select depot_area,
       sum(nvl(a.total_stock_num, 0))  total_stock_num 
from zydb.ods_who_wms_goods_stock_total_detail a
inner join 
zydb.dim_dw_depot b
on a.depot_id=b.depot_id
and depot_area in('cn','sa')
and data_date='${data_date}'
group by depot_area

union all 

select 'all_depot' depot_area,
       sum(nvl(a.total_stock_num, 0))  total_stock_num 
from zydb.ods_who_wms_goods_stock_total_detail a
inner join 
zydb.dim_dw_depot b
on a.depot_id=b.depot_id
and depot_area<>''
and data_date='${data_date}'
)t2
on t1.depot_area=t2.depot_area
left join
(
---调拨在仓
select depot_area,sum(allocate_order_onway_num) allocate_order_onway_num
from zydb.ods_wms_goods_stock_onway_total a
inner join 
zydb.dim_dw_depot b
on a.depot_id=b.depot_id
and depot_area in('cn','sa')
and data_date=${data_date}
group by depot_area
  
union all

select 'all_depot' depot_area,sum(allocate_order_onway_num) allocate_order_onway_num
from zydb.ods_wms_goods_stock_onway_total a
inner join 
zydb.dim_dw_depot b
on a.depot_id=b.depot_id
and depot_area<>''  
and data_date=${data_date}
)t3
on t1.depot_area=t3.depot_area
left join
(
--退货在途 ，订单表已退货且不在退货表里
select depot_area,sum(original_goods_number) return_onway  from
(
--终结退回订单
    select a.order_id,a.depod_id ,c.original_goods_number
      from zydb.dw_order_sub_order_fact a
    inner join jolly.who_order_shipping_tracking b on a.order_id=b.order_id
    inner join zydb.dw_order_goods_fact c on a.order_id=c.order_id
    where shipping_state in(6,8)
		  and b.update_time >=unix_timestamp(to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),day(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))-1)),'yyyy-MM-dd')
          and b.update_time < unix_timestamp(to_date(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)),'yyyy-MM-dd')
         
)a
inner join 
zydb.dim_dw_depot b on a.depod_id=b.depot_id
                      and depot_area in('cn','sa')
where 
not exists
(
--退货入库
      select p1.returned_order_id
      from jolly.who_wms_returned_order_goods p1
      inner join jolly.who_wms_returned_order_info p2
                       on p1.returned_rec_id = p2.returned_rec_id
      where  p1.returned_stock_num > 0
           and p1.stock_end_time >0
           and p1.stock_end_time < UNIX_TIMESTAMP(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
           and p2.returned_order_status = 1
           and p1.returned_order_id=a.order_id
)
group by depot_area

union all

select 'all_depot' depot_area,sum(original_goods_number) return_onway  from
(
    select a.order_id,a.depod_id ,c.original_goods_number
      from zydb.dw_order_sub_order_fact a
    inner join jolly.who_order_shipping_tracking b on a.order_id=b.order_id
    inner join zydb.dw_order_goods_fact c on a.order_id=c.order_id
    where shipping_state in(6,8)
		  and b.update_time >=unix_timestamp(to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),day(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))-1)),'yyyy-MM-dd')
          and b.update_time < unix_timestamp(to_date(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)),'yyyy-MM-dd')
         
)a
inner join 
zydb.dim_dw_depot b
on a.depod_id=b.depot_id
and depot_area<>''
where 
not exists
(
      select p1.returned_order_id
      from jolly.who_wms_returned_order_goods p1
      inner join jolly.who_wms_returned_order_info p2
                       on p1.returned_rec_id = p2.returned_rec_id
      where  p1.returned_stock_num > 0
           and p1.stock_end_time > 0
           and p1.stock_end_time < UNIX_TIMESTAMP(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
           and p2.returned_order_status = 1
           and p1.returned_order_id=a.order_id
) 
)t4
on t1.depot_area=t4.depot_area
left join
(
--发货在途  ，已发货未签收
select depot_area,
       sum(b.original_goods_number) ship_onway 
from zydb.dw_order_sub_order_fact a
    left join zydb.dw_order_goods_fact  b on a.order_id=b.order_id 
    left join jolly.who_order_shipping_tracking c on a.order_id=c.order_id
    inner join  zydb.dim_dw_depot d 
       on a.depod_id=d.depot_id
          and depot_area in('cn','sa')
where shipping_state not in (3, 6, 8, 13)
      and a.is_shiped=1
	  and to_date(shipping_time) >=to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),day(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))-1))
	  and to_date(shipping_time)<to_date(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1))
group by depot_area

union all

select 'all_depot' depot_area,
      sum(b.original_goods_number) ship_onway
from zydb.dw_order_sub_order_fact a
    left join zydb.dw_order_goods_fact  b  on a.order_id=b.order_id
    left join jolly.who_order_shipping_tracking c on a.order_id=c.order_id
    inner join  zydb.dim_dw_depot d
          on a.depod_id=d.depot_id
          and depot_area<>''
where shipping_state not in (3, 6, 8, 13)
      and a.is_shiped=1
	  and to_date(shipping_time) >=to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),day(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))-1))
	  and to_date(shipping_time)<to_date(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1))
)t5
on t1.depot_area=t5.depot_area
left join   
(
--MTD签收数总和
    select  depot_area,
            sum(c.original_goods_number) receive_mtd
      from zydb.dw_order_sub_order_fact a
    inner join jolly.who_order_shipping_tracking b on a.order_id=b.order_id
    inner join zydb.dw_order_goods_fact c on a.order_id=c.order_id
    inner join  zydb.dim_dw_depot d on a.depod_id=d.depot_id
                                    and depot_area in('cn','sa')
    where shipping_state=3
	and b.update_time>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),day(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))-1),'yyyy-MM-dd')
	and b.update_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
    group by depot_area

   union all
   
     select 'all_depot' depot_area,
            sum(c.original_goods_number) receive_mtd
      from zydb.dw_order_sub_order_fact a
    inner join jolly.who_order_shipping_tracking b on a.order_id=b.order_id
    inner join zydb.dw_order_goods_fact c on a.order_id=c.order_id
    inner join  zydb.dim_dw_depot d on a.depod_id=d.depot_id
                                    and depot_area<>''
    where shipping_state=3 
	and b.update_time>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),day(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))-1),'yyyy-MM-dd')
	and b.update_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
)t6
on t1.depot_area=t6.depot_area
;



set mapreduce.map.memory.mb=6120;
set mapreduce.reduce.memory.mb=12000;
set hive.exec.parallel=true;


--create table zydb.rpt_supply_chain_final_table_new3 as 

insert overwrite table zydb.rpt_supply_chain_final_table_new3
select *
from
(
--按需采购反应时长(LT0)
	select 
	  sum((unix_timestamp(push_gmt_created,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(pay_time,'yyyy-MM-dd HH:mm:ss'))*(lock_org_num-lock_oos_num))/sum(lock_org_num-lock_oos_num)/3600 lt0
	from
	(
		select 
			  distinct  a.pay_time,
          			  b.rec_id,
          			  b.lock_org_num,
          			  b.lock_oos_num,
          			  b.push_gmt_created,
          			  b.order_id,
          			  b.sku_id
		from zydb.dw_order_node_time a
		left join  zydb.dw_demand_pur b on a.order_id=b.order_id
		left join  zydb.dw_delivered_receipt_onself c 
		  on b.pur_order_sn=c.delivered_order_sn
		 and b.sku_id=c.sku_id
		where  to_date(nvl(end_receipt_time,on_shelf_start_time))=to_Date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
		and b.push_gmt_created>a.pay_time
		and lock_org_num>lock_oos_num 
		and source_type=2
	)a
)t1,
(
--商品到货时长(LT1)
	select sum(goods_send_num) LT1
	 from   
	 (
              	select order_id,max(update_time) update_time
              	from(
                		    select order_id ,update_time
                        from jolly.who_order_shipping_tracking 
                        where shipping_state=3 --已签收
                        -----选择签收时间（15天前那周）
                         and update_time>=unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),15),'yyyy-MM-dd')
                         and update_time<unix_timestamp(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),14),'yyyy-MM-dd')  
              	) t 
              	group by order_id
    ) a
	 left join 
	 jolly.who_order_goods b
	 on a.order_id=b.order_id
)t2,
(
--仓库作业时长(LT2) 质检时长+上架时长+拣货时长+打包时长+发运时长
    select check_duration+onself_duration+pick_duration+pack_duration+shipping_duration LT2
    from
    (
    --质检时长   
      select 
       sum(((unix_timestamp(on_shelf_start_time)-unix_timestamp(start_receipt_time))/3600/24)*num)/sum(num)*24  as check_duration
	  from zydb.dw_delivered_receipt_onself a
	  where a.on_shelf_start_time >=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
	   and a.on_shelf_start_time < date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
	   and ((unix_timestamp(on_shelf_start_time)-unix_timestamp(start_receipt_time))/3600/24)>0
    )t1,
    (	
    	--上架时长
    	select 
    	  sum((unix_timestamp(on_shelf_finish_time)-unix_timestamp(on_shelf_start_time))*on_shelf_num)/sum(on_shelf_num)/3600 onself_duration
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
    	
    )t2,
    (	
    	--拣货时长
    	select 
    	sum(unix_timestamp(picking_finish_time) -unix_timestamp(outing_stock_time))/count(*)/3600 pick_duration
    	from  zydb.dw_order_node_time
    	where to_Date(picking_finish_time)=to_Date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
    )t3,
    (
    	--打包时长
    	select 
    	sum(unix_timestamp(order_pack_time) -unix_timestamp(picking_finish_time))/count(*)/3600 pack_duration
    	from  zydb.dw_order_node_time
    	where to_Date(order_pack_time)=to_Date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
    )t4,
    (
    	--发运时长
    	select 
    	sum(unix_timestamp(shipping_time) -unix_timestamp(order_pack_time))/count(*)/3600 shipping_duration
    	from  zydb.dw_order_node_time
    	where to_Date(shipping_time)=to_Date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
    )t5
)t3,
(
  --转运时长(LT3)
  select 
  sum(unix_timestamp(arrive_time) -unix_timestamp(shipping_time))/count(*)/3600/24 lt3
  from  zydb.dw_order_shipping_tracking_node
  where to_Date(arrive_time)=to_Date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
  and arrive_time is not null
)t4,
(
  --配送时长(LT4)
  select 
  sum(unix_timestamp(receipt_time) -unix_timestamp(arrive_time))/count(*)/3600/24 LT4
  from  zydb.dw_order_shipping_tracking_node
  where to_Date(receipt_time)=to_Date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
  and receipt_time is not null
)t5,
(
  --订单反应时长
	select 
	  sum((unix_timestamp(push_gmt_created,'yyyy-MM-dd HH:mm:ss')-unix_timestamp(pay_time,'yyyy-MM-dd HH:mm:ss'))*(lock_org_num-lock_oos_num))/sum(lock_org_num-lock_oos_num)/3600 pay_push_dur
	from
	(
		select 
			  distinct a.pay_time,b.rec_id,b.lock_org_num,b.lock_oos_num,b.push_gmt_created,b.order_id,b.sku_id
		from zydb.dw_order_node_time a
		left join 
		zydb.dw_demand_pur b
		on a.order_id=b.order_id
		left join
		zydb.dw_delivered_receipt_onself c
		on b.pur_order_sn=c.delivered_order_sn
		and b.sku_id=c.sku_id
		where 
		to_date(nvl(end_receipt_time,on_shelf_start_time))=to_Date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
		and b.push_gmt_created>a.pay_time
		and lock_org_num>lock_oos_num 
		and demand_type=1
	)a
)t6,
(
  --订单可拣货时长=max(配货、审单)
  select 
  sum(unix_timestamp(outing_stock_time) -unix_timestamp(order_check_time))/count(*)/3600  pick_time
  from  zydb.dw_order_node_time
  where to_Date(outing_stock_time)=to_Date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
  and outing_stock_time is not null
)t7,
(
--订单出库时长
  select 
  sum(unix_timestamp(shipping_time) -unix_timestamp(outing_stock_time))/count(*)/3600  out_time
  from  zydb.dw_order_node_time
  where to_Date(shipping_time)=to_Date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
  and shipping_time is not null
)t8,
(
  --订单签收时长
  select 
  sum(unix_timestamp(receipt_time) -unix_timestamp(shipping_time))/count(*)/3600/24  receive_time
  from  zydb.dw_order_shipping_tracking_node
  where to_Date(receipt_time)=to_Date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))
  and receipt_time is not null
)t9,
(
--当天自由库存数
  select sum(a.total_stock_num-a.total_order_lock_num-a.total_allocate_lock_num-a.total_return_lock_num)  free_goods_stock
  from  zydb.ods_who_wms_goods_stock_total_detail a
  where data_date='${data_date}'	
)t10 
;



set mapreduce.map.memory.mb=6120;
set mapreduce.reduce.memory.mb=12000;
set hive.exec.parallel=true;



--create table zydb.rpt_supply_chain_final_table_new4 as 

insert overwrite table zydb.rpt_supply_chain_final_table_new4
select * 
from
(
    --近7天商品平均销量	
    select 
        sum(case when 
          case when pay_id=41 then a.pay_time else a.result_pay_time end>=date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),6)
          and case when pay_id=41 then a.pay_time else a.result_pay_time end<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
        then  b.original_goods_number else 0 end)/7 goods_sale_7,
     --近15天商品平均销量
        sum(case when 
          case when pay_id=41 then a.pay_time else a.result_pay_time end>=date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),14)
          and case when pay_id=41 then a.pay_time else a.result_pay_time end<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
        then  b.original_goods_number else 0 end)/15 goods_sale_15,
    --近30天商品平均销量    
        sum(case when 
          case when pay_id=41 then a.pay_time else a.result_pay_time end>=date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),29)
          and case when pay_id=41 then a.pay_time else a.result_pay_time end<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
        then  b.original_goods_number else 0 end)/30  goods_sale_30
    
    from zydb.dw_order_sub_order_fact a
      left join zydb.dw_order_goods_fact b on a.order_id=b.order_id
)t1,
(
  --3天前按需采购商品件数、4天前按需采购商品件数
  select  sum(case when to_date(check_time)=to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),2))  then demand_supp_num else 0 end) goods_num_pur_3,
          sum(case when to_date(check_time)=to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),3))  then demand_supp_num else 0 end) goods_num_pur_4
  from
  (  
      select distinct rec_id,demand_supp_num,check_time
      from 
      zydb.dw_demand_pur a
      where demand_type in(1,2,7)
  )a
)t2,
(		
--48h按需采购到货商品件数、72h按需采购到货商品件数
	select 
	 sum(case when to_date(check_time)=to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),2))  
		        and least(start_receipt_time,on_shelf_start_time)<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)  
	          and (unix_timestamp(end_receipt_time) - unix_timestamp(check_time)) /3600<48 then demand_supp_num end) goods_num_pur_48h,
	          
	 sum(case when to_date(check_time)=to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),3))  
		        and least(start_receipt_time,on_shelf_start_time)<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)  
	          and (unix_timestamp(end_receipt_time) - unix_timestamp(check_time)) /3600<72 then demand_supp_num end) goods_num_pur_72h
	                   
	 from 
	(
		select  
			  a.rec_id,a.sku_id,a.check_time,a.demand_supp_num,max(b.end_receipt_time) end_receipt_time,min(b.start_receipt_time) start_receipt_time,min(on_shelf_start_time) on_shelf_start_time
		from 
		zydb.dw_demand_pur a
		left join 
		zydb.dw_delivered_receipt_onself b
		on a.pur_order_sn=b.delivered_order_sn
		and a.sku_id=b.sku_id
		where demand_type in(1,2,7)
		group by a.rec_id,a.sku_id,a.check_time,a.demand_supp_num

	)a
)t3,
(
-- 4天前滚动备货商品件数
  select  sum(demand_supp_num) goods_num_roll_4
  from
  (
      select distinct rec_id,demand_supp_num,demand_gmt_created
      from 
      zydb.dw_demand_pur a
      where demand_type in(3)
      and to_date(demand_gmt_created)=to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),3))
  )a
)t4,
(
--72小时滚动备货到货商品件数
	select 
	 sum(case when (unix_timestamp(end_receipt_time) - unix_timestamp(demand_gmt_created)) /3600<72 then demand_supp_num end) goods_num_roll_72h
	                   
	 from 
	(
		select  
			  a.rec_id,a.sku_id,a.demand_gmt_created,a.demand_supp_num,max(b.end_receipt_time) end_receipt_time,min(b.start_receipt_time) start_receipt_time,min(on_shelf_start_time) on_shelf_start_time
		from 
		zydb.dw_demand_pur a
		left join 
		zydb.dw_delivered_receipt_onself b
		on a.pur_order_sn=b.delivered_order_sn
		and a.sku_id=b.sku_id
		where demand_type in(3)
		and to_date(demand_gmt_created)=to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),3))  
		and least(start_receipt_time,on_shelf_start_time)<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1) 
		group by a.rec_id,a.sku_id,a.demand_gmt_created,a.demand_supp_num

	)a
)t5,
(
--7天前付款订单商品数
select sum(b.original_goods_number) pay_order_goods_num_7
from zydb.dw_order_sub_order_fact a
left join zydb.dw_order_goods_fact b on a.order_id=b.order_id
where to_date(case when a.pay_id=41 then a.pay_time else a.result_pay_time end) = to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),6))  
and a.pay_status in (1,3)
)t6,
(
--7天前付款订单缺货数
select sum(oos_num) pay_order_goods_num_oos_7
from zydb.dw_order_sub_order_fact a
left join jolly.who_wms_order_oos_log b on a.order_id=b.order_id
where to_date(case when pay_id=41 then pay_time else result_pay_time end) = to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),6))  
and b.create_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
and a.pay_status in (1,3)
)t7,
(
--商家缺货数
select sum(oos_num) supply_oos
from zydb.dw_order_sub_order_fact a
left join jolly.who_wms_order_oos_log b on a.order_id=b.order_id
where to_date(case when pay_id=41 then pay_time else result_pay_time end) = to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),6))  
and b.create_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
and type in(1,2,3,9)
)t8,
(
--仓库缺货数
select sum(oos_num) depot_oos
from zydb.dw_order_sub_order_fact a
left join jolly.who_wms_order_oos_log b on a.order_id=b.order_id
where to_date(case when pay_id=41 then pay_time else result_pay_time end) = to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),6))  
and b.create_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
and type in(5,6,7,10,13)
)t9
;




set mapreduce.map.memory.mb=6120;
set mapreduce.reduce.memory.mb=12000;
set hive.exec.parallel=true;



--create table zydb.rpt_supply_chain_final_table_new5 as 

insert overwrite table zydb.rpt_supply_chain_final_table_new5

select * 
from
(
--付款订单数、取消订单数、已发货订单数、国内仓子单付款订单数
select sum(case when pay_status in(1,3) then 1 else 0 end) pay_order_num,
       sum(case when order_status=2   then 1 else 0 end) cancel_order_num,
       sum(case when is_shiped=1 then 1 else 0 end) shipping_order_num,
	   sum(case when b.depot_area='cn' then 1 else 0 end)  pay_order_num_cn
from zydb.dw_order_sub_order_fact a
left join zydb.dim_dw_depot b on a.depod_id=b.depot_id
where to_date(case when pay_id=41 then pay_time else result_pay_time end) = to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))  
)t1,
(
-- 待发货订单数
select 
   count(*) pend_ship_order_num
from zydb.dw_order_sub_order_fact a
left join zydb.dw_order_node_time b on a.order_id=b.order_id
where to_date(outing_stock_time)=to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))  
and a.is_shiped<>1
)t2,
(
--超期订单数
select count(*) over_time_order_num 
from zydb.dw_order_sub_order_fact a
where to_date(case when pay_id=41 then pay_time else result_pay_time end) = to_date(date_sub(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),4))  
and a.is_shiped<>1
)t3,
(
--采购商品件数
  select sum(demand_supp_num) pur_goods_num
  from 
  (
      select distinct rec_id,demand_supp_num
      from 
      zydb.dw_demand_pur
      where to_date(demand_gmt_created)=to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))) 
		and demand_type in(1,2,7)
  )a
)t4,
(
  --缺货商品件数
  select sum(oos_num) oos_goods_num
  from jolly.who_wms_order_oos_log 
  where create_time>=unix_timestamp('${data_date}','yyyyMMdd') 
    and create_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')  
)t5,
(
--入库商品件数(进)
select sum(on_shelf_num) in_goods_num
from jolly.who_wms_on_shelf_goods_price 
where gmt_created>=unix_timestamp('${data_date}','yyyyMMdd') 
  and gmt_created<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')  
)t6,
(
--售出商品件数(销) 、国内仓子单付款商品数
select sum(b.original_goods_number) sale_goods_num,
	   sum(case when c.depot_area='cn' then b.original_goods_number else 0 end)  pay_order_goods_num_cn
from 
zydb.dw_order_sub_order_fact a
left join zydb.dw_order_goods_fact b on a.order_id=b.order_id
left join zydb.dim_dw_depot c on a.depod_id=c.depot_id
where to_date(case when pay_id=41 then a.pay_time else result_pay_time end) = to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))  
and a.pay_status in (1,3)
)t7,
(
--出库商品件数(销)
  select sum(change_num) out_goods_num
  from jolly.who_wms_goods_stock_detail_log
  where change_type in(5,6,13,17,19,20)
  and change_time>=unix_timestamp('${data_date}','yyyyMMdd') 
  and change_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')  
)t8,
(
--在仓商品件数(存)
  select sum(stock_num) depot_goods_num
  from zydb.ods_who_wms_goods_stock_detail
  where data_date='${data_date}'
)t9,
(
--命中订单数（all）
  select sum(case when pur_order_id is null then 1 else 0 end) hit_order_num
  from
  (
      select  a.order_id all_order_id,b.order_id pur_order_id
      from zydb.dw_order_sub_order_fact a
      left join 
      (
          select distinct rec_id,order_id
          from 
          zydb.dw_demand_pur 
      ) b
      on a.order_id=b.order_id
      where to_date(case when pay_id=41 then a.pay_time else result_pay_time end) = to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))  
  )a 
)t10,
(
--命中商品件数(all)
  select sum(case when pur_order_id is null then original_goods_number else 0 end) hit_goods_num
  from
  (
      select  a.order_id all_order_id,c.order_id pur_order_id,b.original_goods_number
      from zydb.dw_order_sub_order_fact a
      left join zydb.dw_order_goods_fact b on a.order_id=b.order_id
      left join 
      (
          select distinct rec_id,order_id,goods_id
          from 
          zydb.dw_demand_pur 
      ) c
      on b.order_id=c.order_id and b.goods_id=c.goods_id
      where to_date(case when pay_id=41 then a.pay_time else result_pay_time end) = to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))  
  )a 
)t11,
(
--订单命中数(CN)
  select sum(case when pur_order_id is null then 1 else 0 end) hit_rate_order
  from
  (
      select  a.order_id all_order_id,b.order_id pur_order_id,c.depot_area
      from zydb.dw_order_sub_order_fact a
      left join 
      (
          select distinct rec_id,order_id
          from 
          zydb.dw_demand_pur 
      ) b
      on a.order_id=b.order_id
  	left join zydb.dim_dw_depot c on a.depod_id=c.depot_id
      where to_date(case when pay_id=41 then a.pay_time else result_pay_time end) = to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))  
  	and c.depot_area='cn'
  )a 
)t12,
(
--库存命中数(CN)
  select sum(case when pur_order_id is null then original_goods_number else 0 end) hit_rate_depot
  from
  (
      select  a.order_id all_order_id,c.order_id pur_order_id,b.original_goods_number
      from zydb.dw_order_sub_order_fact a
      left join zydb.dw_order_goods_fact b on a.order_id=b.order_id
      left join 
      (
          select distinct rec_id,order_id,goods_id
          from 
          zydb.dw_demand_pur 
      ) c
      on b.order_id=c.order_id and b.goods_id=c.goods_id
  	left join zydb.dim_dw_depot d on a.depod_id=d.depot_id
      where to_date(case when pay_id=41 then a.pay_time else result_pay_time end) = to_date(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')))  
  		and d.depot_area='cn'
  )a 	
)t13
;	
	
	
	


--create table zydb.rpt_supply_chain_final_table_link_new as

insert overwrite table  zydb.rpt_supply_chain_final_table_link_new 
select 
      '${data_date}' data_date  
      ,a.depot_area
      ,lt04
      ,lt01
      ,lt02
      ,lt34
      ,pay_order_num_7day
      ,pay_order_num_oos_7day
      ,pay_goods_num_7day
      ,pay_goods_num_oos_7day
      ,depot_mtd
      ,out_depot_mtd
      ,depot_link_mtd
      ,receive_mtd

from 
zydb.rpt_supply_chain_final_table_new1 a
left join zydb.rpt_supply_chain_final_table_new2  b on a.depot_area=b.depot_area
union all 

select * from zydb.rpt_supply_chain_final_table_link_new where data_date  <>'${data_date}'
;



--create table zydb.rpt_supply_chain_final_table_new as

insert overwrite table  zydb.rpt_supply_chain_final_table_new
select  '${data_date}' data_date,*
from zydb.rpt_supply_chain_final_table_new3 a,
zydb.rpt_supply_chain_final_table_new4 b,
zydb.rpt_supply_chain_final_table_new5 c

union all 

select * from zydb.rpt_supply_chain_final_table_new where data_date  <>'${data_date}'
;

































