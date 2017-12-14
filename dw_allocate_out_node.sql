
insert overwrite  zydb.dw_allocate_out_node   
SELECT  
     a.rec_id
    ,a.goods_id
    ,a.goods_sn
    ,a.order_id
    ,a.allocate_num demand_allocate_num
    ,a.allocate_source_type
    ,a.sku_id
    ,a.sku_value
    ,a.status allocate_status
    ,from_unixtime(a.gmt_created) demand_gmt_created
    ,a.from_depot_id
    ,a.to_depot_id
    ,b.allocate_order_id
    ,b.allocate_num
    ,b.org_allocate_num
    ,b.cancel_num
    ,from_unixtime(b.gmt_created) allocate_gmt_created
    ,b.arrive_num
    ,b.real_picking_num
    ,c.allocate_order_sn
    ,c.tracking_no
    ,c.create_type
    ,from_unixtime(c.out_time) out_time
    ,from_unixtime(d.picked_time)
    ,from_unixtime(d.packed_time)
    ,d.total_num
    ,from_unixtime(e.gmt_created) all_detail_gmt_created
    ,e.allocate_num all_detail_allocate_num
    ,e.org_allocate_num all_detail_org_allocate_num
    ,e.cancel_num all_detail_cancel_num
    ,e.exp_num all_detail_exp_num
    ,from_unixtime(e.depot_receive_time)  all_detail_depot_receive_time 
    ,e.picked_num
    ,e.packed_num

   
FROM jolly_spm.jolly_spm_allocate_goods_demand a
  left join 
  (
 select * from jolly_archive.who_wms_allocate_demand_goods_relation_history
 union all 
 select * from jolly_spm.jolly_spm_allocate_demand_goods_relation
 union all 
 select  0 rec_id,
   rec_id allocate_goods_demand_rec_id,
   allocate_order_goods_rec_id  allocate_order_goods_rec_id
   from jolly_spm.jolly_spm_allocate_goods_demand a 
 where from_unixtime(a.gmt_created)>='2017-08-13'
   and from_unixtime(a.gmt_created)<'2017-09-04 13:37:55'
   and rec_id<>10629156
   and rec_id>9925724
   and allocate_order_goods_rec_id<>0
  ) demand on a.rec_id=demand.allocate_goods_demand_rec_id
  
  LEFT JOIN jolly_spm.jolly_spm_allocate_order_goods b ON demand.allocate_order_goods_rec_id = b.rec_id
  LEFT JOIN jolly_spm.jolly_spm_allocate_order_info c ON b.allocate_order_id = c.allocate_order_id
  LEFT JOIN
--出库
(
 SELECT 
   allocate_order_id allocate_out_id,
   allocate_order_sn allocate_out_sn,
   gmt_created,
   admin_id,
   out_time,
   remark,
   status,
   shipping_money,
   package_weight,
   tracking_no,
   tracking_id,
   from_depot_id,
   to_depot_id,
   -1 picked_admin_id,
   0 picked_time,
   finish_packing_admin_id packed_admin_id,
   finish_packing_time packed_time,
   0 depot_receive_time,
   create_type,
   0 total_num
  
  FROM jolly.who_wms_allocate_order_info_history
  union all 
  select *  from jolly.who_wms_allocate_out_info
 ) d ON c.allocate_order_sn = d.allocate_out_sn
  LEFT JOIN 
 (select 
   rec_id ,
   allocate_order_id allocate_out_id ,
   sku_id ,
   sku_value ,
   goods_id ,
   goods_sn ,
   goods_thumb ,
   provider_code supp_code ,
   0 in_price ,
   allocate_num ,
   org_allocate_num ,
   cancel_num ,
   exp_num ,
   allocate_status status ,
   shelf_area_id ,
   shelf_area_sn ,
   gmt_created ,
   0 depot_receive_time ,
   0 picked_num ,
   0 packed_num ,
   pda_status ,
   0 pda_time 
   
 from 
 jolly_archive.who_wms_allocate_order_goods_history
 union all 
 select * from jolly.who_wms_allocate_out_goods 
)e ON d.allocate_out_id = e.allocate_out_id and b.sku_id=e.sku_id
;
