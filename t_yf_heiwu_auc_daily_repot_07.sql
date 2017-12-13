
----------------------------------------------------------------------
--07 取商品SKU颜色、尺码、材质等属性
----------------------------------------------------------------------
--drop table zybiro.t_yf_heiwu_auc_daily_repot_07;
create table if not exists zybiro.t_yf_heiwu_auc_daily_repot_07
(
  sku_id      string
 ,goods_id    string
 ,vname       string
 ,vvale       string
)
partitioned by (ds string)
row format delimited fields terminated by '\t'
lines terminated by '\n'
stored as textfile
;
insert overwrite table zybiro.t_yf_heiwu_auc_daily_repot_07 partition (ds='${last_day}')
select   cast(sku_id   as string) as sku_id
  ,cast(goods_id as string) as goods_id
  ,cast(p1       as string) as vname  
  ,cast(p2       as string) as vvale
from 
(
 select  sku_id
   ,goods_id
   ,split_part(p,':',1) as p1
   ,split_part(p,':',2) as p2
 from 
 (
  select rec_id as sku_id
  ,goods_id
  ,split_part(sku_value,'.',1) as p 
  from   jolly.who_sku_relation
 ) a 
 where split_part(p,':',1) in ('SIZE','COLOR','MATERIAL')
 union all
 select  sku_id
   ,goods_id
   ,split_part(p,':',1) as p1
   ,split_part(p,':',2) as p2
 from 
 (
  select rec_id as sku_id
  ,goods_id
  ,split_part(sku_value,'.',2) as p 
  from   jolly.who_sku_relation
 ) a 
 where split_part(p,':',1) in ('SIZE','COLOR','MATERIAL')
 union all
 select  sku_id
   ,goods_id
   ,split_part(p,':',1) as p1
   ,split_part(p,':',2) as p2
 from 
 (
  select rec_id as sku_id
  ,goods_id
  ,split_part(sku_value,'.',3) as p 
  from   jolly.who_sku_relation
 ) a 
 where split_part(p,':',1) in ('SIZE','COLOR','MATERIAL')
 union all
 select  sku_id
   ,goods_id
   ,split_part(p,':',1) as p1
   ,split_part(p,':',2) as p2
 from 
 (
  select rec_id as sku_id
  ,goods_id
  ,split_part(sku_value,'.',4) as p 
  from   jolly.who_sku_relation
 ) a 
 where split_part(p,':',1) in ('SIZE','COLOR','MATERIAL')
) b 
;
