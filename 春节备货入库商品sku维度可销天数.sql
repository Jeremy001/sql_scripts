select
ws3.sku_id
,ws.goods_id
,ws.onsale_status
,ws.category_group
,ws.cat_level1_name
,ws.cat_level2_name
,ws.cat_level3_name
,ws.supp_name
,ws.co_status
,ws.season
,ws.first_on_sale_time
,ws.last_sold_out_reason
,ws.sku_color
,ws.sku_size
,nvl(ws1.arg_num,0)  arg_num
,ws6.pred_goods_num
,nvl(ws4.onway_num,0)+nvl(ws5.free_stock_num,0) AS stock_num
,(nvl(ws4.onway_num,0)+nvl(ws5.free_stock_num,0))/ws1.arg_num AS can_sale_days
from
(
        select
            b.sku_id
        from zybiro.beryl_chunjiebeihuo_goods_sku_info_temp0111confirmed b
        where b.indepot_num>0
) ws3
left join
(
           select
           ws.*
           ,ws2.rec_id sku_id
           ,case when upper(ws2.sku_value) like'COLOR%SIZE%'
               then substr(ws2.sku_value,instr(upper(ws2.sku_value),':',1,1)+1,instr(upper(ws2.sku_value),'.SIZE:')-instr(upper(ws2.sku_value),':',1,1)-1)
               when upper(ws2.sku_value) like'SIZE%COLOR%'
               then substr(ws2.sku_value,instr(upper(ws2.sku_value),'.COLOR:')+7)
               when upper(ws2.sku_value) not  like'%SIZE%'
               then substr(ws2.sku_value,7)
               when upper(ws2.sku_value) not  like'%COLOR%'
               then null  end   sku_color
           ,case when upper(ws2.sku_value) like'COLOR%SIZE%'
               then substr(ws2.sku_value,instr(upper(ws2.sku_value),'SIZE:')+5)
               when upper(ws2.sku_value) like'SIZE%COLOR%'
               THEN substr(ws2.sku_value,instr(upper(ws2.sku_value),'SIZE:')+5,instr(upper(ws2.sku_value),'.COLOR')-6)
               when upper(ws2.sku_value) not like'%SIZE%'
               then null
               else regexp_replace(upper(ws2.sku_value),'SIZE:','')  end  sku_size
           from
                     (
                       select
                                  case when ws5.is_hide=0 then"合作中"  when ws5.is_hide=1 then"取消合作" else null end co_status
                                  ,case when ws1.is_on_sale=0 then"下架"  when ws1.is_on_sale=1 then"上架" else null end onsale_status
                                  ,ws3.category_group
                                  ,ws.cat_level1_name
                                  ,ws.cat_level2_name
                                  ,ws.cat_level3_name
                                  ,ws.supp_name
                                  ,cast (ws.goods_id as int) goods_id
                                  ,ws4.season
                                  ,ws.first_on_sale_time
                                  ,ws2.last_sold_out_reason
                       from
                                 zydb.dim_jc_goods ws
                                 left join
                                 (select
                                   ds
                                   ,goods_id
                                   ,max(is_on_sale) is_on_sale
                                    from zydb.dw_goods_on_sale
                                    group by ds,goods_id
                                 ) ws1
                                 on cast(ws.goods_id as int)=cast(ws1.goods_id as int)
                                 left join
                                 jolly.who_goods ws2
                                 on cast(ws.goods_id as int)=cast(ws2.goods_id as int)
                                 left join
                                 zybiro.blue_cat_big_info ws3
                                 on ws3.cat_level1_name=ws.cat_level1_name
                                 left join
                                 zybiro.blue_basic_season_info ws4
                                 on ws.goods_season=ws4.goods_season
                                 left join jolly.who_esoloo_supplier ws5 on ws.supp_name=ws5.supp_name
                                 where
                                 TO_DATE(concat(substr(ws1.ds,1,4),'-',substr(ws1.ds,5,2),'-',substr(ws1.ds,7,2))) ='2018-02-28'
                                 group by
                                 case when ws5.is_hide=0 then"合作中"  when ws5.is_hide=1 then"取消合作" else null end
                                 ,case when ws1.is_on_sale=0 then"下架"  when ws1.is_on_sale=1 then"上架" else null end
                                 ,ws3.category_group
                                 ,ws.cat_level1_name
                                 ,ws.cat_level2_name
                                 ,ws.cat_level3_name
                                 ,ws.supp_name
                                 ,ws.goods_id
                                 ,ws4.season
                                 ,ws.first_on_sale_time
                                 ,ws2.last_sold_out_reason
                     ) ws
           left join
                    jolly.who_sku_relation ws2
           on cast(ws.goods_id as int)=cast(ws2.goods_id as int)
)ws
on ws.sku_id=ws3.sku_id
left join
(
       select
                  cc.sku_id ,
                   sum(cc.num_15+cc.num_14+cc.num_13+cc.num_12+cc.num_11+cc.num_10+cc.num_9+cc.num_8+cc.num_7+cc.num_6+cc.num_5+cc.num_4+cc.num_3+cc.num_2+cc.num_1)/120  arg_num
        from
                 (select
                  b.sku_id sku_id ,
                     sum(case when datediff( '2018-03-15' ,TO_DATE (case when c.pay_id=41 then c.pay_time else c.result_pay_time end ))=14 then
                     b.goods_number *15 else 0 end)  num_15,
                   sum(case when datediff( '2018-03-15' ,TO_DATE (case when c.pay_id=41 then c.pay_time else c.result_pay_time end ))=13 then
                    b.goods_number *14 else 0 end) num_14,
                   sum(case when datediff( '2018-03-15' ,TO_DATE (case when c.pay_id=41 then c.pay_time else c.result_pay_time end) )=12 then
                    b.goods_number *13 else 0 end) num_13,
                   sum(case when datediff( '2018-03-15' ,TO_DATE (case when c.pay_id=41 then c.pay_time else c.result_pay_time end ))=11 then
                    b.goods_number *12 else 0 end) num_12,
                   sum(case when datediff( '2018-03-15' ,TO_DATE (case when c.pay_id=41 then c.pay_time else c.result_pay_time end ))=10 then
                    b.goods_number *11 else 0 end) num_11,
                   sum(case when datediff( '2018-03-15' ,TO_DATE (case when c.pay_id=41 then c.pay_time else c.result_pay_time end) )=9 then
                    b.goods_number *10 else 0 end) num_10,
                   sum(case when datediff( '2018-03-15' ,TO_DATE (case when c.pay_id=41 then c.pay_time else c.result_pay_time end) )=8 then
                    b.goods_number *9 else 0 end) num_9,
                   sum(case when datediff( '2018-03-15' ,TO_DATE (case when c.pay_id=41 then c.pay_time else c.result_pay_time end ))=7 then
                    b.goods_number *8 else 0 end) num_8,
                   sum(case when datediff( '2018-03-15' ,TO_DATE (case when c.pay_id=41 then c.pay_time else c.result_pay_time end) )=6 then
                    b.goods_number *7 else 0 end ) num_7,
                   sum(case when datediff( '2018-03-15' ,TO_DATE (case when c.pay_id=41 then c.pay_time else c.result_pay_time end) )=5 then
                    b.goods_number *6 else 0 end)  num_6,
                   sum(case when datediff( '2018-03-15' ,TO_DATE (case when c.pay_id=41 then c.pay_time else c.result_pay_time end ))=4 then
                    b.goods_number *5 else 0 end ) num_5,
                   sum(case when datediff( '2018-03-15' ,TO_DATE (case when c.pay_id=41 then c.pay_time else c.result_pay_time end) )=3 then
                    b.goods_number *4 else 0 end)  num_4,
                   sum(case when datediff( '2018-03-15' ,TO_DATE (case when c.pay_id=41 then c.pay_time else c.result_pay_time end) )=2 then
                    b.goods_number *3 else 0 end) num_3,
                     sum(case when datediff( '2018-03-15' ,TO_DATE (case when c.pay_id=41 then c.pay_time else c.result_pay_time end) )=1 then
                    b.goods_number *2 else 0 end) num_2,
                       sum(case when datediff( '2018-03-15' ,TO_DATE (case when c.pay_id=41 then c.pay_time else c.result_pay_time end) )=0 then
                    b.goods_number *1 else 0 end) num_1
                  from
                 zydb.dw_order_goods_fact b
                 left join zydb.dw_order_sub_order_fact c  on b.order_id=c.order_id
                 where c.pay_status in(1,3)
                 and  TO_DATE (case when c.pay_id=41 then c.pay_time else c.result_pay_time end )<='2018-03-15'
                 and  TO_DATE (case when c.pay_id=41 then c.pay_time else c.result_pay_time end )>date_sub('2018-03-15',15)
                 group by  b.sku_id
                 )cc
                group by cc.sku_id
) ws1
on ws3.sku_id=ws1.sku_id
left join
(
         select
         sku_id,
         sum(onway_num) onway_num
         from
          zybiro.beryl_chunjiebeihuorucang_onway_num_sku
          group by  sku_id
) ws4
on ws3.sku_id=ws4.sku_id
left join
(
        select
        sku_id,
        sum(free_stock_num) free_stock_num
        from
        zybiro.beryl_chunjiebeihuorucang_instockfree_num_sku
        where date_id='2018-03-14'
        group by sku_id
)ws5
on ws3.sku_id=ws5.sku_id
LEFT JOIN
(SELECT t1.sku_id
        ,SUM(t1.goods_number) AS pred_goods_num
FROM zybiro.beryl_sku_predict_temp0111 AS t1
WHERE t1.data_date >= '2018-03-15'
GROUP BY t1.sku_id
) AS ws6
ON ws3.sku_id = ws6.sku_id
;
