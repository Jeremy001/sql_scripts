insert overwrite zydb.rpt_uae_warehouse_report
select * from ( select '${data_date}' as data_date ) a join (
    select * from (
        select sum(change_num) as uae_purin_goods_num 
        from jolly_wms.who_wms_goods_stock_detail_log 
        where change_time >= unix_timestamp('${data_date}','yyyyMMdd') 
        and change_time < unix_timestamp('${data_date}','yyyyMMdd')+86400 
        and depot_id=15 
        and change_type in (1,2,3,4,9,11,14,15,16)
    )a join (
        select count(distinct goods_id) as uae_purin_goodsstyle_num 
        from jolly_wms.who_wms_goods_stock_detail_log 
        where change_time >= unix_timestamp('${data_date}','yyyyMMdd') 
        and change_time < unix_timestamp('${data_date}','yyyyMMdd')+86400 
        and depot_id=15 
        and change_type in (1,2,3,4,9,11,14,15,16)
    )b on 1=1 join (
        select count(distinct sku_id) as uae_purin_sku_num 
        from jolly_wms.who_wms_goods_stock_detail_log 
        where change_time >= unix_timestamp('${data_date}','yyyyMMdd') 
        and change_time < unix_timestamp('${data_date}','yyyyMMdd')+86400 
        and depot_id=15 
        and change_type in (1,2,3,4,9,11,14,15,16)
    )c on 1=1 join (
        select sum(change_num) as uae_delivout_goods_num 
        from jolly_wms.who_wms_goods_stock_detail_log 
        where change_time >= unix_timestamp('${data_date}','yyyyMMdd') 
        and change_time < unix_timestamp('${data_date}','yyyyMMdd')+86400 
        and depot_id=15 
        and change_type in  (5,6,13,17)
    )d on 1=1 join (
        select count(distinct goods_id) as uae_delivout_goodsstyle_num 
        from jolly_wms.who_wms_goods_stock_detail_log 
        where change_time >= unix_timestamp('${data_date}','yyyyMMdd') 
        and change_time < unix_timestamp('${data_date}','yyyyMMdd')+86400 
        and depot_id=15 
        and change_type in  (5,6,13,17)
    )e on 1=1 join (
        select count(distinct sku_id) as uae_delivout_sku_num 
        from jolly_wms.who_wms_goods_stock_detail_log 
        where change_time >= unix_timestamp('${data_date}','yyyyMMdd') 
        and change_time < unix_timestamp('${data_date}','yyyyMMdd')+86400 
        and depot_id=15 
        and change_type in  (5,6,13,17)
    )f on 1=1 join (
        select sum(total_stock_num-total_order_lock_num-total_allocate_lock_num-total_return_lock_num) as uae_freestock_goods_num 
        from jolly_wms.who_wms_goods_stock_total_detail_daily 
        where depot_id=15 and ds='${data_date}'
    )g on 1=1 join (
        select count(distinct goods_id) as uae_freestock_goodsstyle_num 
        from jolly_wms.who_wms_goods_stock_total_detail_daily 
        where depot_id=15 and ds='${data_date}'
        and total_stock_num-total_order_lock_num-total_allocate_lock_num-total_return_lock_num>0
    )h on 1=1 join (
        select count(distinct sku_id) as uae_freestock_sku_num 
        from jolly_wms.who_wms_goods_stock_total_detail_daily
        where depot_id=15 and ds='${data_date}'
        and total_stock_num-total_order_lock_num-total_allocate_lock_num-total_return_lock_num>0
    )i on 1=1
)b on 1=1 join (
    select
        c.总付款单数 as paid_orders,
        decode(c.总付款单数,0,0,c.全UAE仓付款订单/c.总付款单数) as uae_paid_orders_ratio,
        decode(c.总付款单数,0,0,c.全CN仓付款订单/c.总付款单数) as cn_paid_orders_ratio,
        decode(c.总付款单数,0,0,c.混合仓付款订单/c.总付款单数) as mix_paid_orders_ratio,
        decode(c.总下单数,0,0,c.总付款单数/c.总下单数) as order_paid_rate,
        decode(c.全UAE仓订单,0,0,c.全UAE仓付款订单/c.全UAE仓订单) as uae_order_paid_rate,
        decode(c.全CN仓订单,0,0,c.全CN仓付款订单/c.全CN仓订单) as cn_order_paid_rate,
        decode(c.混合仓订单,0,0,c.混合仓付款订单/c.混合仓订单) as mix_order_paid_rate,

        c.总下单数 as order_paid,

        c.全UAE仓付款订单 as uae_paid_orders,
        c.全CN仓付款订单 as cn_paid_orders,
        c.混合仓付款订单 as mix_paid_orders,

        c.全UAE仓订单 as uae_order,
        c.全UAE仓付款订单 as uae_order_paid,

        c.全CN仓订单 as cn_order,
        c.全CN仓付款订单 as cn_order_paid,

        c.混合仓订单 as mix_order,
        c.混合仓付款订单 as mix_order_paid
    from (
        select
            sum(case when a.order_id is not null then 1 else 0 end)总下单数,
            sum(case when b.depot_coverage_area_id=12 and b.source_order_id=0 and a.is_split is null then 1 else 0 end)全UAE仓订单,
            sum(case when b.depot_coverage_area_id=1 and b.source_order_id=0 and a.is_split is null then 1 else 0 end)全CN仓订单,
            sum(case when a.is_split=1 then 1 else 0 end)混合仓订单,
            sum(case when a.pay_status in(1,3) then 1 else 0 end)总付款单数,
            sum(case when a.pay_status in(1,3) and b.depot_coverage_area_id=12 and b.source_order_id=0 and a.is_split is null then 1 else 0 end)全UAE仓付款订单,
            sum(case when a.pay_status in(1,3) and b.depot_coverage_area_id=1 and b.source_order_id=0 and a.is_split is null then 1 else 0 end)全CN仓付款订单,
            sum(case when a.pay_status in(1,3) and a.is_split=1 then 1 else 0 end)混合仓付款订单
        from ( select add_time,order_id,is_split,pay_status from zydb.dw_order_fact where unix_timestamp(add_time) >= unix_timestamp('${data_date}','yyyyMMdd') and unix_timestamp(add_time) < unix_timestamp('${data_date}','yyyyMMdd')+86400 and country_name='United Arab Emirates' and site_id in (600,900)
        )a join ( select depot_coverage_area_id,source_order_id,order_id from jolly.who_order_user_info union select depot_coverage_area_id,source_order_id,order_id from jolly.who_order_user_info_history
        )b on a.order_id=b.order_id
    )c
)c on 1=1 join (
    select *
    --
    --订单结构
    --
    from (
        select
            a.sub_paid_orders,--付款订单数
            b.order_amount,--订单金额
            c.goods_sales_num,--商品销量
            (b.order_amount/a.sub_paid_orders) as avg_order_amount,--单均价
            (c.goods_sales_num/a.sub_paid_orders) as avg_goods_sales_num--单均件
        from (--付款订单数
            select count(distinct a.order_id) sub_paid_orders
            from (
                select order_id,(case when pay_id=41 then pay_time else result_pay_time end) as pay_time 
                from zydb.dw_order_sub_order_fact
                where pay_status=1 and depod_id=15 and unix_timestamp(add_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*10)
            )a
            where unix_timestamp(a.pay_time)>=unix_timestamp('${data_date}','yyyyMMdd') and unix_timestamp(a.pay_time)<unix_timestamp('${data_date}','yyyyMMdd')+86400
        )a join (--订单销售额
            select sum(a.pay_money + a.surplus + a.order_amount)as order_amount
            from (
                select order_id,(case when pay_id=41 then pay_time else result_pay_time end) as pay_time,pay_money,surplus,order_amount
                from zydb.dw_order_sub_order_fact
                where pay_status=1 and depod_id=15 and unix_timestamp(add_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*10)
            )a
            where unix_timestamp(a.pay_time)>=unix_timestamp('${data_date}','yyyyMMdd') and unix_timestamp(a.pay_time)<unix_timestamp('${data_date}','yyyyMMdd')+86400
        )b on 1=1 join (--商品销量
            select sum(b.原始商品件数) as goods_sales_num
            from (
                select order_id,(case when pay_id=41 then pay_time else result_pay_time end) as pay_time
                from zydb.dw_order_sub_order_fact a
                where a.pay_status=1 and a.depod_id=15 and unix_timestamp(a.add_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*10)
            )a join (
                select order_id,sum(original_goods_number) as 原始商品件数
                from zydb.dw_order_goods_fact where unix_timestamp(gmt_created)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*8) group by order_id
            )b on a.order_id=b.order_id
            where unix_timestamp(a.pay_time)>=unix_timestamp('${data_date}','yyyyMMdd') and unix_timestamp(a.pay_time)<unix_timestamp('${data_date}','yyyyMMdd')+86400
        )c on 1=1
    )a

    --
    --作业能力
    --
    join (
        select
            a.total_delivery_num,--总发运订单数
            (b.total_delivery_time/a.total_delivery_num/3600) as avg_delivery_time,--平均时长
            c.rec_num,
            (c.avg_service_time/c.rec_num) as avg_service_time,

            b.total_delivery_time
        from (--总发运订单数
            select
                count(distinct order_id)as total_delivery_num
            from (
                select order_id,shipping_time,(case when pay_id=41 then pay_time else result_pay_time end) as pay_time
                from zydb.dw_order_sub_order_fact
                where pay_status=1 and depod_id=15 and unix_timestamp(shipping_time)>=unix_timestamp('${data_date}','yyyyMMdd') and unix_timestamp(shipping_time)<unix_timestamp('${data_date}','yyyyMMdd')+86400
            )a
        )a join (
            select
                count(distinct case when unix_timestamp(shipping_time)-unix_timestamp(pay_time)>0 then order_id else null end )as total_service_orders,--参与计算订单服务时长的订单数
                sum(case when unix_timestamp(shipping_time)-unix_timestamp(pay_time)>0 
                then unix_timestamp(shipping_time)-unix_timestamp(pay_time) else null end )as total_delivery_time--总时长
            from (
                select order_id,shipping_time,(case when pay_id=41 then pay_time else result_pay_time end) as pay_time
                from zydb.dw_order_sub_order_fact
                where pay_status=1 and depod_id=15 and unix_timestamp(shipping_time)>=unix_timestamp('${data_date}','yyyyMMdd') and unix_timestamp(shipping_time)<unix_timestamp('${data_date}','yyyyMMdd')+86400
            )a
        )b on 1=1 join (--签收数(COD)
            select
                count(distinct a.order_id) as rec_num,
                sum(case when b.update_time-unix_timestamp(a.pay_time)>0 then b.update_time-unix_timestamp(a.pay_time) else null end )/86400 as avg_service_time
            from (
                select a.order_id, a.pay_time
                from zydb.dw_order_sub_order_fact  a 
                where a.depod_id=15 and a.pay_id=41 and a.cod_check_status in (3,5) and unix_timestamp(a.pay_time)>= unix_timestamp('${data_date}','yyyyMMdd')-(86400*60)
            ) as a inner join (
                select b.order_id,b.update_time
                from jolly.who_order_shipping_tracking  b 
                where b.update_time >= unix_timestamp('${data_date}','yyyyMMdd')
                and b.update_time < unix_timestamp('${data_date}','yyyyMMdd')+86400
            )b on a.order_id = b.order_id
        )c on 1=1 
    )b on 1=1

    --
    --待处理&异常
    --
    join (
        select * 
        from (--挂起数(COD)
            select count(distinct order_id) as still_hungup_num
            from zydb.dw_order_sub_order_fact
            where unix_timestamp(pay_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*60)
            and unix_timestamp(pay_time)<unix_timestamp('${data_date}','yyyyMMdd')+86400
            and pay_status=1-- 已付款
            and order_status=1-- 未取消
            and cod_check_status=1--仍挂起
            and pay_id=41--COD
            and depod_id=15--UAE仓
        )a join (--待审数(COD)
            select count(distinct case when  is_problems_order!=2 and is_shiped!=1 then order_id else null end ) as cod_checked_orders
            from zydb.dw_order_sub_order_fact
            where unix_timestamp(add_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*15)
            and unix_timestamp(add_time)<unix_timestamp('${data_date}','yyyyMMdd')+86400
            and pay_status=1
            and order_status=1
            and pay_id=41
            and depod_id=15
        )b on 1=1 join (--待发数
            select count(distinct case when (pay_id!=41 or (pay_id=41 and is_problems_order=2)) and is_shiped!=1 then order_id else null end ) as cod_readytodeliv_orders
            from zydb.dw_order_sub_order_fact
            where unix_timestamp(add_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*15)
            and unix_timestamp(add_time)<unix_timestamp('${data_date}','yyyyMMdd')+86400
            and pay_status=1
            and order_status=1
            and depod_id=15
        )c on 1=1 join (--超期仍挂起(COD)
            select count(distinct order_id) as overdue_hungup_num
            from zydb.dw_order_sub_order_fact
            where unix_timestamp(pay_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*60)
            and unix_timestamp(pay_time)<unix_timestamp('${data_date}','yyyyMMdd')-(86400*15)
            and pay_status=1-- 已付款
            and order_status=1-- 未取消
            and cod_check_status=1--仍挂起
            and pay_id=41--COD
            and depod_id=15--UAE仓
        )d on 1=1 join (--超期未审(COD)
            select count(distinct case when  is_problems_order!=2 and is_shiped!=1 then order_id else null end ) as overdue_not_confirm
            from zydb.dw_order_sub_order_fact
            where unix_timestamp(pay_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*15)
            and unix_timestamp(pay_time)<unix_timestamp('${data_date}','yyyyMMdd')-(86400*3)
            and pay_status=1
            and order_status=1
            and pay_id=41
            and depod_id=15
        )e on 1=1 join (--超期未发select 
            select count(distinct order_id)
            from zydb.dw_order_sub_order_fact 
            where is_shiped<>1
            and unix_timestamp(add_time)>=unix_timestamp('${data_date}', 'yyyyMMdd')-15*86400
            and unix_timestamp(add_time)<unix_timestamp('${data_date}', 'yyyyMMdd')-4*86400
            and pay_status=1
            and is_problems_order=2 
            and order_status=1
            and depod_id=15
        )f on 1=1
    )c on 1=1

    --
    --取消率
    --
    join (
        select
            a.total_uae_canl_rate,          --全UAE仓取消率
            b.mix_uae_canl_rate,            --混UAE仓取消率
            c.total_CN_canl_rate,          --全CN仓取消率
            d.mix_CN_canl_rate,            --混CN仓取消率

            a.total_uae_canl_number,
            a.total_uae_canl_order_number,
            b.mix_uae_canl_number,
            b.mix_uae_canl_order_number,
            c.total_CN_canl_number,
            c.total_CN_canl_order_number,
            d.mix_CN_canl_number,
            d.mix_CN_canl_order_number
        from (--纯UAE仓
            select 
                (b.total_uae_canl_number/a.total_uae_canl_order_number) as total_uae_canl_rate,
                b.total_uae_canl_number,
                a.total_uae_canl_order_number
            from (
                select 
                    count(distinct order_id) as total_uae_canl_order_number
                from zydb.dw_order_sub_order_fact 
                where unix_timestamp(add_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*6) and unix_timestamp(add_time)<unix_timestamp('${data_date}','yyyyMMdd')-(86400*5) 
                and pay_status=1 and depod_id=15 and source_order_id=0
            )a join (
                select count(distinct case when order_status in(2,3) and is_shiped!=1 then order_id else null end)as total_uae_canl_number
                from zydb.dw_order_sub_order_fact 
                where unix_timestamp(add_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*6) and unix_timestamp(add_time)<unix_timestamp('${data_date}','yyyyMMdd')-(86400*5) 
                and pay_status=1 and depod_id=15 and source_order_id=0
            )b on 1=1
        )a join (--混UAE仓
            select 
                (b.mix_uae_canl_number/a.mix_uae_canl_order_number) as mix_uae_canl_rate,
                b.mix_uae_canl_number,
                a.mix_uae_canl_order_number
            from (
                select count(distinct order_id) as mix_uae_canl_order_number
                from zydb.dw_order_sub_order_fact 
                where unix_timestamp(add_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*6) and unix_timestamp(add_time)<unix_timestamp('${data_date}','yyyyMMdd')-(86400*5) 
                and pay_status=1 and depod_id=15 and source_order_id>0
            )a join (
                select count(distinct case when order_status in(2,3) and is_shiped!=1 then order_id else null end)as mix_uae_canl_number
                from zydb.dw_order_sub_order_fact 
                where unix_timestamp(add_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*6) and unix_timestamp(add_time)<unix_timestamp('${data_date}','yyyyMMdd')-(86400*5) 
                and pay_status=1 and depod_id=15 and source_order_id>0
            )b on 1=1
        )b on 1=1 join (--纯CN仓
            select 
                (b.total_CN_canl_number/a.total_CN_canl_order_number) as total_CN_canl_rate,
                b.total_CN_canl_number,
                a.total_CN_canl_order_number
            from (
                select count(distinct order_id) as total_CN_canl_order_number
                from zydb.dw_order_sub_order_fact 
                where unix_timestamp(add_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*6) and unix_timestamp(add_time)<unix_timestamp('${data_date}','yyyyMMdd')-(86400*5) 
                and pay_status=1 and depod_id!=15 and source_order_id=0
            )a join (
                select count(distinct case when order_status in(2,3) and is_shiped!=1 then order_id else null end)as total_CN_canl_number
                from zydb.dw_order_sub_order_fact 
                where unix_timestamp(add_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*6) and unix_timestamp(add_time)<unix_timestamp('${data_date}','yyyyMMdd')-(86400*5) 
                and pay_status=1 and depod_id!=15 and source_order_id=0
            )b on 1=1
        )c on 1=1 join (--纯CN仓
            select 
                (b.mix_CN_canl_number/a.mix_CN_canl_order_number) as mix_CN_canl_rate,
                b.mix_CN_canl_number,
                a.mix_CN_canl_order_number
            from (
                select count(distinct order_id) as mix_CN_canl_order_number
                from zydb.dw_order_sub_order_fact 
                where unix_timestamp(add_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*6) and unix_timestamp(add_time)<unix_timestamp('${data_date}','yyyyMMdd')-(86400*5) 
                and pay_status=1 and depod_id!=15 and source_order_id>0
            )a join (
                select count(distinct case when order_status in(2,3) and is_shiped!=1 then order_id else null end)as mix_CN_canl_number
                from zydb.dw_order_sub_order_fact 
                where unix_timestamp(add_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*6) and unix_timestamp(add_time)<unix_timestamp('${data_date}','yyyyMMdd')-(86400*5) 
                and pay_status=1 and depod_id!=15 and source_order_id>0
            )b on 1=1
        )d on 1=1
    )d on 1=1

    --
    --签收率
    --
    join (
        select
            a.total_uae_rec_rate,           --全UAE仓签收率
            b.mix_uae_rec_rate,             --混UAE仓签收率
            c.total_CN_rec_rate,           --全CN仓签收率
            d.mix_CN_rec_rate,             --混CN仓签收率

            a.total_uae_rec_number,
            a.total_uae_rec_arrival_number,
            b.mix_uae_rec_number,
            b.mix_uae_rec_arrival_number,
            c.total_CN_rec_number,
            c.total_CN_rec_arrival_number,
            d.mix_CN_rec_number,
            d.mix_CN_rec_arrival_number
        from (--纯UAE仓
            select 
                (b.total_uae_rec_number/a.total_uae_rec_arrival_number) as total_uae_rec_rate,
                b.total_uae_rec_number,
                a.total_uae_rec_arrival_number
            from (
                select count(distinct a.order_id) as total_uae_rec_arrival_number
                from jolly.who_prs_cod_order_shipping_time as a join zydb.dw_order_sub_order_fact as b on a.order_id=b.order_id
                where a.destination_time>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*6) 
                and a.destination_time<unix_timestamp('${data_date}','yyyyMMdd')-(86400*5)
                and unix_timestamp(b.add_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*29) 
                and b.country_name='United Arab Emirates' 
                and b.pay_id=41 
                and b.depod_id=15 
                and b.source_order_id=0
            )a join (
                select count(distinct case when  b.cod_check_status in(3,5)then a.order_id else null end )as total_uae_rec_number
                from jolly.who_prs_cod_order_shipping_time as a join zydb.dw_order_sub_order_fact as b on a.order_id=b.order_id
                where a.destination_time>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*6) 
                and a.destination_time<unix_timestamp('${data_date}','yyyyMMdd')-(86400*5)
                and unix_timestamp(b.add_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*29) 
                and b.country_name='United Arab Emirates' 
                and b.pay_id=41 
                and b.depod_id=15 
                and b.source_order_id=0
            )b on 1=1
        )a join (--混UAE仓
            select 
                (b.mix_uae_rec_number/a.mix_uae_rec_arrival_number) as mix_uae_rec_rate,
                b.mix_uae_rec_number,
                a.mix_uae_rec_arrival_number
            from (
                select count(distinct a.order_id) as mix_uae_rec_arrival_number
                from jolly.who_prs_cod_order_shipping_time as a join zydb.dw_order_sub_order_fact as b on a.order_id=b.order_id
                where a.destination_time>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*6) 
                and a.destination_time<unix_timestamp('${data_date}','yyyyMMdd')-(86400*5)
                and unix_timestamp(b.add_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*29) 
                and b.country_name='United Arab Emirates' 
                and b.depod_id=15 
                and b.source_order_id>0
            )a join (
                select count(distinct case when  b.cod_check_status in(3,5)then a.order_id else null end )as mix_uae_rec_number
                from jolly.who_prs_cod_order_shipping_time as a join zydb.dw_order_sub_order_fact as b on a.order_id=b.order_id
                where a.destination_time>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*6) 
                and a.destination_time<unix_timestamp('${data_date}','yyyyMMdd')-(86400*5)
                and unix_timestamp(b.add_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*29) 
                and b.country_name='United Arab Emirates' 
                and b.depod_id=15 
                and b.source_order_id>0
            )b on 1=1
        )b on 1=1 join (--纯CN仓
            select 
                (b.total_CN_rec_number/a.total_CN_rec_arrival_number) as total_CN_rec_rate,
                b.total_CN_rec_number,
                a.total_CN_rec_arrival_number
            from (
                select count(distinct a.order_id) as total_CN_rec_arrival_number
                from jolly.who_prs_cod_order_shipping_time as a join zydb.dw_order_sub_order_fact as b on a.order_id=b.order_id
                where a.destination_time>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*6) 
                and a.destination_time<unix_timestamp('${data_date}','yyyyMMdd')-(86400*5)
                and unix_timestamp(b.add_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*29) 
                and b.country_name='United Arab Emirates' 
                and b.depod_id!=15 
                and b.source_order_id=0
            )a join (
                select count(distinct case when  b.cod_check_status in(3,5)then a.order_id else null end )as total_CN_rec_number
                from jolly.who_prs_cod_order_shipping_time as a join zydb.dw_order_sub_order_fact as b on a.order_id=b.order_id
                where a.destination_time>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*6) 
                and a.destination_time<unix_timestamp('${data_date}','yyyyMMdd')-(86400*5)
                and unix_timestamp(b.add_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*29) 
                and b.country_name='United Arab Emirates' 
                and b.depod_id!=15 
                and b.source_order_id=0
            )b on 1=1
        )c on 1=1 join (--混CN仓
            select 
                (b.mix_CN_rec_number/a.mix_CN_rec_arrival_number) as mix_CN_rec_rate,
                b.mix_CN_rec_number,
                a.mix_CN_rec_arrival_number
            from (
                select count(distinct a.order_id) as mix_CN_rec_arrival_number
                from jolly.who_prs_cod_order_shipping_time as a join zydb.dw_order_sub_order_fact as b on a.order_id=b.order_id
                where a.destination_time>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*6) 
                and a.destination_time<unix_timestamp('${data_date}','yyyyMMdd')-(86400*5)
                and unix_timestamp(b.add_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*29) 
                and b.country_name='United Arab Emirates' 
                and b.depod_id!=15 
                and b.source_order_id>0
            )a join (
                select count(distinct case when  b.cod_check_status in(3,5)then a.order_id else null end )as mix_CN_rec_number
                from jolly.who_prs_cod_order_shipping_time as a join zydb.dw_order_sub_order_fact as b on a.order_id=b.order_id
                where a.destination_time>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*6) 
                and a.destination_time<unix_timestamp('${data_date}','yyyyMMdd')-(86400*5)
                and unix_timestamp(b.add_time)>=unix_timestamp('${data_date}','yyyyMMdd')-(86400*29) 
                and b.country_name='United Arab Emirates' 
                and b.depod_id!=15 
                and b.source_order_id>0
            )b on 1=1
        )d on 1=1
    )e on 1=1
)d on 1=1

UNION

select * from zydb.rpt_uae_warehouse_report
where data_date != '${data_date}';

insert overwrite zydb.rpt_uae_warehouse_report_rec_num --签收数(COD)
select
    FROM_UNIXTIME(unix_timestamp('${data_date}','yyyyMMdd')-(86400*1), 'yyyyMMdd') as data_date,
    count(distinct a.order_id) as rec_num
from (
    select a.order_id, a.pay_time
    from zydb.dw_order_sub_order_fact  a 
    where a.depod_id=15 and a.pay_id=41 and a.cod_check_status in (3,5) and unix_timestamp(a.pay_time)>= unix_timestamp('${data_date}','yyyyMMdd')-(86400*60)
) as a inner join (
    select b.order_id,b.update_time
    from jolly.who_order_shipping_tracking  b 
    where b.update_time >= unix_timestamp('${data_date}','yyyyMMdd')-(86400*1)
    and b.update_time < unix_timestamp('${data_date}','yyyyMMdd')+86400-(86400*1)
)b on a.order_id = b.order_id;

insert into zydb.rpt_uae_warehouse_report_rec_num --签收数(COD)
select
    FROM_UNIXTIME(unix_timestamp('${data_date}','yyyyMMdd')-(86400*2), 'yyyyMMdd') as data_date,
    count(distinct a.order_id) as rec_num
from (
    select a.order_id, a.pay_time
    from zydb.dw_order_sub_order_fact  a 
    where a.depod_id=15 and a.pay_id=41 and a.cod_check_status in (3,5) and unix_timestamp(a.pay_time)>= unix_timestamp('${data_date}','yyyyMMdd')-(86400*60)
) as a inner join (
    select b.order_id,b.update_time
    from jolly.who_order_shipping_tracking  b 
    where b.update_time >= unix_timestamp('${data_date}','yyyyMMdd')-(86400*2)
    and b.update_time < unix_timestamp('${data_date}','yyyyMMdd')+86400-(86400*2)
)b on a.order_id = b.order_id;