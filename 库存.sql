
----------------- 各仓库 总库存 和 自由库存，历史和当前值取法
*******************************************************************
国内仓，求总库存表：   (hadoop) zydb.ods_who_wms_goods_stock_detail       stock_num          (可以查：当前最新和历史每天快照)


国内仓， 求自由库存：  (hadoop) zydb.ods_who_wms_goods_stock_total_detail ( data from 201704)  (可以查：当前最新和历史每天快照) 
--ZYDB.ODS_WHO_WMS_GOODS_STOCK_TOTAL_DETAIL表中20170813 - 20170822的库存数据可能不太准确 
                                        free_num = total_stock_num-total_order_lock_num-total_allocate_lock_num-total_return_lock_num
          
                                或  jolly.who_wms_goods_stock_total_detail             (可以查：当前最新)

*******************************
沙特仓，求总库存: (hadoop)   jolly_wms.who_wms_goods_stock_detail    stock_num     (可以查：当前最新)  --无法求沙特历史某天快照总库存   

沙特仓，求自由库: (hadoop)   jolly_wms.who_wms_goods_stock_total_detail            (可以查：当前最新)          
                                       free_num= total_stock_num-total_order_lock_num-total_allocate_lock_num-total_return_lock_num   
            
                             jolly_wms.who_wms_goods_stock_total_detail_daily      (可以查：当前最新和历史每天快照) 
                     free_num= total_stock_num-total_order_lock_num-total_allocate_lock_num-total_return_lock_num



-- 库存
--每日各仓某时刻 自由库存统计
select t.depot_id,count(distinct t.goods_id), sum(t.free_num)
from 
(--  CN仓
select depot_id,goods_id,sku_id,
sum(total_stock_num) total_stock_num,
sum(total_stock_num-total_order_lock_num-total_allocate_lock_num-total_return_lock_num) free_num 
from  jolly.who_wms_goods_stock_total_detail s  -- 历史自由库存  201704开始 -zydb.ods_who_wms_goods_stock_total_detail 
where 1=1
and depot_id in (4,5,6)
and total_stock_num-total_order_lock_num-total_allocate_lock_num-total_return_lock_num>0
group by depot_id, goods_id,sku_id
) t
group by t.depot_id
union all
select t.depot_id,count(distinct t.goods_id), sum(t.free_num)
from 
(--  SA仓
select depot_id,goods_id,sku_id,
sum(total_stock_num) total_stock_num,
sum(total_stock_num-total_order_lock_num-total_allocate_lock_num-total_return_lock_num) free_num 
from  jolly_wms.who_wms_goods_stock_total_detail s --历史自由库存 ？jolly_wms.who_wms_goods_stock_total_detail_daily 
where 1=1
and depot_id =7 
and total_stock_num-total_order_lock_num-total_allocate_lock_num-total_return_lock_num>0
group by depot_id, goods_id,sku_id
) t
group by t.depot_id

----各仓库每日进出
select to_date(from_unixtime(change_time)) data_date,
sum(case when  change_type=1 then change_num end) 备货入库件数,
sum(case when  change_type=3 then change_num end) 退货入库件数 ,
sum(case when  change_type=5 then change_num end) 销售出库件数 
from jolly_wms.who_wms_goods_stock_detail_log  a
where 1=1 
and from_unixtime(change_time,'yyyyMMdd')>='20170701'
and from_unixtime(change_time,'yyyyMMdd')< '20170717'
group by to_date(from_unixtime(change_time)) 






