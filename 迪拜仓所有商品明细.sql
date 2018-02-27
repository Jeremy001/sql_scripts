


/*阿联酋迪拜仓库商品明细*/

insert overwrite table zybiro.blue_uae_goods_detail_part3
select
ws.goods_id
from
zydb.ods_who_wms_goods_stock_total_detail ws
where
ws.data_date=from_timestamp(date_sub(now(),1),'yyyyMMdd')
and
ws.depot_id=15
group by 
ws.goods_id;


insert overwrite table zybiro.blue_uae_goods_detail_part1
select
 ws.cate_level1_name
,ws.cate_level2_name
,ws.cate_level3_name
,ws.supp_name
,ws.goods_id
,ws.goods_sn
,ws.goods_name
,ws.season
,ws.is_auto_offsale
,ws.is_delete
,ws.list_not_show
,ws.last_sold_out_reason
,ws.is_forever_offsale
,ws.first_on_sale_time
,ws.off_sale_time
,ws.in_price
,ws.shop_price
,ws.promote_price
,ws.is_on_sale
,sum(case when ws.uae_unique=0 then 1 else 0 end) uae_unique_count
,sum(case when ws.can_sale=1   then 1 else 0 end) can_sale
,sum(case when ws.can_sale=1   then ws.free_num_all else 0 end) can_sale_num
,sum(ws.free_num_all) free_num
,sum(ws.free_num) free_num_dubai
,sum(case when ws.can_sale=1   then ws.free_num else 0 end) can_sale_num_dubai
from
(
	select
	a.*
	,case when d.status=0 and e.is_on_sale=1 then 1 else 0 end                                        can_sale
	,case when a.free_num>0 and c.free_num_china=0 and (b.is_stock=1 or b.status=1) then 1 else 0 end uae_unique
	,e.in_price
	,e.shop_price
	,e.promote_price
	,e.is_on_sale
	from
	(
		SELECT 
			ws1.cate_level1_name
			,ws1.cate_level2_name
			,ws1.cate_level3_name
			,ws1.supp_name
			,ws1.goods_sn
			,ws1.goods_id
			,ws1.goods_name
			,ws2.season
			,case when ws1.is_auto_offsale=1 then 'Yes' else 'No' end is_auto_offsale
			,case when ws3.is_delete =1 then 'Yes' ELSE 'No' end is_delete
			,case when ws3.list_not_show=1 then 'Yes' else 'No' end list_not_show
			,ws1.last_sold_out_reason
			,ws1.is_forever_offsale
			,ws1.first_on_sale_time
			,ws1.off_sale_time
			,ws.sku_id
			,sum(ws.total_stock_num-ws.total_allocate_lock_num-ws.total_order_lock_num-ws.total_return_lock_num) free_num_all
			,sum(case when ws.depot_id=15 then ws.total_stock_num-ws.total_allocate_lock_num-ws.total_order_lock_num-ws.total_return_lock_num else 0 end) free_num
		FROM 
		zydb.ods_who_wms_goods_stock_total_detail ws
		left join 
		zydb.dim_goods ws1
		on ws.goods_id=ws1.goods_id
		left join 
		zybiro.blue_basic_season_info ws2
		on ws1.goods_season=ws2.goods_season
		left join 
		jolly.who_goods ws3
		on ws1.goods_id=ws3.goods_id
		where data_date=from_timestamp(date_sub(now(),1),'yyyyMMdd')
		group by
		ws1.cate_level1_name
		,ws1.cate_level2_name
		,ws1.cate_level3_name
		,ws1.supp_name
		,ws1.goods_sn
		,ws1.goods_id
		,ws1.goods_name
		,ws2.season
		,case when ws1.is_auto_offsale=1 then 'Yes' else 'No' end 
		,case when ws3.is_delete =1 then 'Yes' ELSE 'No' end 
		,case when ws3.list_not_show=1 then 'Yes' else 'No' end 
		,ws1.last_sold_out_reason
		,ws1.is_forever_offsale
		,ws1.first_on_sale_time
		,ws1.off_sale_time
		,ws.sku_id

	) a
	left join
	(
		/*阿联酋迪拜仓库商品明细*/
		select 
		goods_id
		,sku_id
		,sum(case when depot_coverage_area_id=1 then status end)   status
		,sum(case when depot_coverage_area_id=1 then is_stock end) is_stock
		from  jolly.who_sku_depot_coverage_area_status
		group by  
		goods_id
		,sku_id
	) b
	on a.sku_id=b.sku_id
	left join 
	(
		SELECT 
		goods_id
		,sku_id
		,sum(free_stock_in_depot)free_num_china 
		FROM zydb.dpd_blue_sku_detail_num 
		where data_date=from_timestamp(date_sub(now(),1),'yyyyMMdd') 
		and depot_id in (4,5,6)
		group by
		goods_id
		,sku_id
	)c
	on a.sku_id=c.sku_id
	left join 
	jolly.who_sku_relation d
	on a.sku_id=d.rec_id
	left join 
   (
	select 
	goods_id
	,ws.in_price     
	,ws.shop_price_1    shop_price
	,ws.prst_price_1    promote_price
	,ws.is_jc_on_sale   is_on_sale
	from zydb.dim_goods_extend ws 
	where data_date=from_timestamp(date_sub(now(),1),'yyyyMMdd')
   )e
   on a.goods_id=e.goods_id
)ws
group by
 ws.cate_level1_name
,ws.cate_level2_name
,ws.cate_level3_name
,ws.supp_name
,ws.goods_id
,ws.goods_sn
,ws.goods_name
,ws.season
,ws.is_auto_offsale
,ws.is_delete
,ws.list_not_show
,ws.last_sold_out_reason
,ws.is_forever_offsale
,ws.first_on_sale_time
,ws.off_sale_time
,ws.in_price
,ws.shop_price
,ws.promote_price
,ws.is_on_sale
;

insert overwrite table zybiro.blue_uae_goods_detail_part2
select
b.goods_id
,nvl(b.sales_num            ,0)sales_num
,nvl(b.sales_gmv            ,0)sales_gmv
,nvl(b.goods_price          ,0)goods_price
,nvl(b.expo_uv              ,0)expo_uv
,nvl(b.goods_click_uv       ,0)goods_click_uv
,nvl(b.cart_click_uv        ,0)cart_click_uv
,nvl(b.pay_uv               ,0)pay_uv
,nvl(c.uaedepot_sales_num   ,0)uaedepot_sales_num
,nvl(c.uaedepot_sales_gmv   ,0)uaedepot_sales_gmv
,nvl(sku_adq_flag,  'Null'    )sku_conv
,nvl(zx_level,      '5_非滞'  )zx_level
from
(
	select
	ws.goods_id
	,sum(ws.sales_volume)                       sales_num
	,sum(ws.sales_amount)                       sales_gmv
	,if(sum(ws.sales_volume)=0,0,sum(ws.sales_amount)/sum(ws.sales_volume)) goods_price
	--,sum(ws.sales_amount)/sum(ws.sales_volume)  goods_price
	,sum(ws.expo_uv)                            expo_uv
	,sum(ws.goods_click_uv)                     goods_click_uv
	,sum(ws.cart_click_uv)                      cart_click_uv
	,sum(ws.sales_uv)                           pay_uv
	from
	zydb.rpt_sum_goods_daily ws
	where
	ws.data_date=from_timestamp(date_sub(now(),1),'yyyyMMdd')
	and 
	ws.site_id in (400,600,700,800,900)
	group by
	ws.goods_id
)b
left join 
(
	select
	b.goods_id
	,sum(case when a.depod_id=7  then b.original_goods_number               else 0 end)  sadepot_sales_num
	,sum(case when a.depod_id=7  then b.original_goods_number*b.goods_price else 0 end)  sadepot_sales_gmv
    ,sum(case when a.depod_id=15 then b.original_goods_number               else 0 end)  uaedepot_sales_num
    ,sum(case when a.depod_id=15 then b.original_goods_number*b.goods_price else 0 end)  uaedepot_sales_gmv
	,sum(b.original_goods_number) all_num
	,sum(b.original_goods_number*b.goods_price) all_gmv
	from zydb.dw_order_sub_order_fact a
	inner join zydb.dw_order_goods_fact b on a.order_id=b.order_id
	inner join zydb.dim_jc_goods c on b.goods_id=c.goods_id
	where a.site_id in(400,600,700,800,900)
	and a.pay_status in(1,3)
	and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')=from_timestamp(date_sub(now(),1),'yyyyMMdd')
	group by
	b.goods_id
)c
on b.goods_id=c.goods_id
left join 
(
	SELECT 
	goods_id
	,sku_adq_flag
	,zx_level 
	FROM zybiro.t_yf_stock_monitor_v1_12 
	where ds=from_timestamp(date_sub(now(),1),'yyyyMMdd')
	group by
	goods_id
	,sku_adq_flag
	,zx_level
)d
on b.goods_id=cast(d.goods_id as bigint)
;


create table zybiro.blue_uae_goods_detail_partition
(
data_date                 string
,goods_id                 bigint
,free_num                 bigint
,free_num_dubai           bigint
,can_sale                 string
,can_sale_num             bigint
,can_sale_num_dubai       bigint
,sku_conv                 string
,zx_level                 string
,is_sadepot_unique        string
,supp_name                string
,expo_uv                  bigint
,goods_click_uv           bigint
,cart_click_uv            bigint
,pay_uv                   bigint
,goods_price              double
,sales_num                bigint
,sales_gmv                double
,uaedepot_sales_num       bigint
,uaedepot_sales_gmv       double
,goods_sn                 string
,cate_level1_name         string
,cate_level2_name         string
,cate_level3_name         string
,season                   string
,shop_price               double
,in_price                 double
,promote_price            double
,is_on_sale               string
,is_delete                string
,list_not_show            string
,is_auto_offsale          string
,last_sold_out_reason     tinyint
,is_forever_offsale       tinyint
,first_on_sale_time       string
,off_sale_time            string
,goods_name               string
)partition by(ds string);


insert overwrite table zybiro.blue_uae_goods_detail_partition partition(ds=from_timestamp(date_sub(now(),1),'yyyyMMdd'))
select 
from_timestamp(date_sub(now(),1),'yyyy-MM-dd') data_date
,ws.goods_id
,ws.free_num
,ws.free_num_dubai
,case when ws.can_sale=0 then 'No' else 'Yes' end can_sale
,ws.can_sale_num
,ws.can_sale_num_dubai
,ws1.sku_conv
,ws1.zx_level
,case when ws.uae_unique_count=0 then 'Yes' else 'No' end is_uaedepot_unique
,ws.supp_name
,nvl(ws1.expo_uv,0) expo_uv
,nvl(ws1.goods_click_uv,0) goods_click_uv
,nvl(ws1.cart_click_uv,0)  cart_click_uv
,nvl(ws1.pay_uv        ,0) pay_uv
,nvl(ws1.goods_price   ,0) goods_price
,nvl(ws1.sales_num,0) sales_num
,nvl(ws1.sales_gmv,0) sales_gmv
,nvl(ws1.uaedepot_sales_num,0) uaedepot_sales_num
,nvl(ws1.uaedepot_sales_gmv,0) uaedepot_sales_gmv
,ws.goods_sn
,ws.cate_level1_name
,ws.cate_level2_name
,ws.cate_level3_name
,ws.season
,ws.shop_price
,ws.in_price
,ws.promote_price
,case when ws.is_on_sale=1 then 'Yes' ELSE 'No' end is_on_sale
,ws.is_delete
,ws.list_not_show
,ws.is_auto_offsale
,ws.last_sold_out_reason
,ws.is_forever_offsale
,ws.first_on_sale_time
,ws.off_sale_time
,ws.goods_name
from
zybiro.blue_uae_goods_detail_part1 ws
left join 
zybiro.blue_uae_goods_detail_part2 ws1
on ws.goods_id=ws1.goods_id
where
ws.goods_id
in 
(
select
distinct
goods_id
from
zybiro.blue_uae_goods_detail_part3
);

insert overwrite table zybiro.blue_uae_goods_detail
select
*
from
zybiro.blue_uae_goods_detail_partition ws
where
ws.ds=from_timestamp(date_sub(now(),1),'yyyyMMdd');


insert overwrite table zybiro.blue_uae_goods_detail_part4
select from_timestamp(date_sub(now(),1),'yyyyMMdd')data_date, sum(stock_num) stock_num
,sum(case when b.depot_area_type_id=3 then stock_num end) dis_stock_num
from jolly_wms.who_wms_goods_stock_detail a
left join jolly_wms.who_wms_depot_area  b
on a.depot_area_id=b.depot_area_id
where a.depot_id = 15 and stock_num > 0
group by data_date;


insert overwrite table zybiro.blue_uae_goods_detail_part5
select from_timestamp(date_sub(now(),1),'yyyyMMdd') data_date
,sum(case when from_unixtime(change_time,'yyyyMMdd')=from_timestamp(date_sub(now(),1),'yyyyMMdd') and change_type=1 then change_num else 0 end) storing_num
,sum(case when from_unixtime(change_time,'yyyyMMdd')=from_timestamp(date_sub(now(),1),'yyyyMMdd') and change_type=3 then change_num else 0 end) returns_num
,sum(case when from_unixtime(change_time,'yyyyMMdd')=from_timestamp(date_sub(now(),1),'yyyyMMdd') and change_type=5 then change_num else 0 end) out_num
,sum(case when change_type in(1,2,3,4,9)                                                                            then change_num else 0 end) acc_in_stock
from jolly_wms.who_wms_goods_stock_detail_log 
where  depot_id=15
group by from_timestamp(date_sub(now(),1),'yyyyMMdd');

insert overwrite table zybiro.blue_uae_goods_detail_uv
select
	from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date
	,sum(b.original_goods_number*b.in_price) goods_chengben
	,count(distinct a.user_id) purchase_uv
	from zydb.dw_order_sub_order_fact a
	inner join zydb.dw_order_goods_fact b on a.order_id=b.order_id
	inner join zydb.dim_jc_goods c on b.goods_id=c.goods_id
	where a.site_id in(400,600,700,800,900)
	and a.pay_status in(1,3)
	and a.depod_id=15
	and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')=from_timestamp(date_sub(now(),1),'yyyyMMdd')
	group by
	from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd');
	
	
insert overwrite table zybiro.blue_uae_goods_detail_order
select
	from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd') data_date
	,count(distinct a.order_id) orders
	from zydb.dw_order_sub_order_fact a
	inner join zydb.dw_order_goods_fact b on a.order_id=b.order_id
	inner join zydb.dim_jc_goods c on b.goods_id=c.goods_id
	where a.site_id in(400,600,700,800,900)
	and a.pay_status in(1,3)
	and a.depod_id=15
	and from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd')=from_timestamp(date_sub(now(),1),'yyyyMMdd')
	group by
	from_timestamp(case when a.pay_id=41 then a.pay_time else a.result_pay_time end,'yyyyMMdd');



create table zybiro.blue_uae_goods_overall_partition
(
data_date          string  
,depot              string
,sadepot_sales_num  bigint
,num_to_all         string
,sadepot_sales_gmv  double
,gmv_to_all         string
,sales_goods        bigint
,stock_num          bigint
,dis_stock_num      bigint
,free_num           bigint
,can_sale_num       bigint
,free_goods         bigint
,can_sale_goods     bigint
,dynamic            string
,gross_margin       string
,storing_num        bigint
,returns_num        bigint
,out_num            bigint
)partitioned by (ds string);




insert overwrite table zybiro.blue_uae_goods_overall_partition partition(ds=from_timestamp(date_sub(now(),1),'yyyyMMdd'))
select
ws.data_date
,ws.depot
,ws.uaedepot_sales_num
,ws.num_to_all
,ws.uaedepot_sales_gmv
,ws.gmv_to_all
,ws.sales_goods
,ws1.stock_num
,ws1.dis_stock_num
,ws.free_num
,ws.can_sale_num
,ws.free_goods
,ws.can_sale_goods
,concat(cast(round(ws.sales_goods*100/ws.can_sale_goods,2)as string),'%') Dynamic
,ws.gross_margin
,nvl(ws2.storing_num ,0) storing_num
,nvl(ws2.returns_num ,0) returns_num
,nvl(ws2.out_num     ,0) out_num
,nvl(ws2.acc_in_stock,0) acc_in_stock
,nvl(ws3.goods_chengben,0) goods_chengben
,nvl(ws4.orders        ,0) orders
,nvl(ws3.purchase_uv   ,0) purchase_uv
from
(
	select
	ws.data_date
	,'UAE'     depot
	,sum(ws.uaedepot_sales_num)                                                           uaedepot_sales_num
	,concat(cast(round(sum(ws.uaedepot_sales_num)*100/sum(ws.sales_num),2)as string),'%') num_to_all
	,sum(ws.uaedepot_sales_gmv)                                                           uaedepot_sales_gmv
	,concat(cast(round(sum(ws.uaedepot_sales_gmv)*100/sum(ws.sales_gmv),2)as string),'%') gmv_to_all
	,sum(case when ws.uaedepot_sales_num>0 then 1 else 0 end)                             sales_goods
	,sum(ws.free_num_dubai)                                                               free_num
	,sum(ws.can_sale_num_dubai)                                                           can_sale_num
	,sum(case when ws.free_num_dubai    >0 then 1 else 0 end)                             free_goods
	,sum(case when ws.can_sale_num_dubai>0 then 1 else 0 end)                             can_sale_goods
	,concat(cast(round(sum(ws.uaedepot_sales_num*(ws.goods_price-ws.in_price/ws1.config_value))*100/sum(ws.uaedepot_sales_num*ws.goods_price),2)as string),'%') gross_margin

	from
	zybiro.blue_uae_goods_detail_partition ws
	cross join 
	(select cast(config_value as float)config_value from jolly.who_system_config where rec_id=1) ws1
	where
	ws.ds=from_timestamp(date_sub(now(),1),'yyyyMMdd')
	group by
	ws.data_date
)ws
left join 
zybiro.blue_uae_goods_detail_part4 ws1
on from_timestamp(ws.data_date,'yyyyMMdd')=ws1.data_date
left join 
zybiro.blue_uae_goods_detail_part5 ws2
on from_timestamp(ws.data_date,'yyyyMMdd')=ws2.data_date
left join 
zybiro.blue_uae_goods_detail_uv ws3
on from_timestamp(ws.data_date,'yyyyMMdd')=ws3.data_date
left join 
zybiro.blue_uae_goods_detail_order ws4
on from_timestamp(ws.data_date,'yyyyMMdd')=ws4.data_date

;

