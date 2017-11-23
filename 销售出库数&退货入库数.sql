   --销售出库数 sales_whout_num, --退货入库数（退货给仓库的数量）

 select b.cat_level1_name,
        a.depot_id,
     sum(case when a.change_type=5 then a.change_num else 0 end) as sales_whout_num,
     sum(case when a.change_type=3 then a.change_num else 0 end) as returned_depot_num
 from jolly.who_wms_goods_stock_detail_log as a 
 left join 
 zydb.dim_jc_goods as b
 on a.goods_id=b.goods_id
 where a.change_time>=unix_timestamp('${data_date}','yyyyMMdd')
     and a.depot_id in (4,5,6) and a.change_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
 group by b.cat_level1_name,a.depot_id
 
 union all
 
 select b.cat_level1_name,
        a.depot_id,
     sum(case when a.change_type=5 then a.change_num else 0 end) as sales_whout_num,
     sum(case when a.change_type=3 then a.change_num else 0 end) as returned_depot_num
 from jolly_wms.who_wms_goods_stock_detail_log as a 
 left join 
 zydb.dim_jc_goods as b
 on a.goods_id=b.goods_id
 where a.change_time>=unix_timestamp('${data_date}','yyyyMMdd')
     and a.depot_id in (7) and a.change_time<unix_timestamp(date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
 group by b.cat_level1_name,a.depot_id