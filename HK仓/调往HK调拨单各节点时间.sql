/*
HK�ֵ���
1.���ɵ�������ʱ��
2.���ɵ�����ʱ�䣬 2-1 = ����������Ӧʱ��
3.����������ʱ�䣬 3-2 = �����ֵ�����ҵʱ��
4.������ǩ��ʱ�䣬 4-3 = ��������;ʱ��
5.�������ϼܿ�ʼʱ�䣬 �޷�ȡ��ǩ��ʱ�䣬��5-3 = ��������;ʱ��
6.�������ϼܽ���ʱ�䣬 6-5 = �ϼ�ʱ����
 */


/*
order_id  ����id
order_sn  ������
goods_number  �������ͬ��ʱ�����Ʒ����
original_goods_number ����ԭʼ��Ʒ����
depot_id  �ֿ�
is_shiped �����������ͬ��ʱ��״̬:˳��Ӧ����   0����ȫû��ʼ�����---4������ƥ�䣩----5����ȫƥ�䣩-----7���������---8������У�---6�������ɣ�---3����������--2�����ַ�����---1���ѷ�����
pay_time  ��������ʱ��
order_check_time  ����ϵͳ��˷���ֿ�ʱ��
lock_check_time ���������ʼ��������桢��ʼ�ɹ�������
allocate_demand_start ������������ʼʱ�䣨���У����������һ��������ʼʱ�䣩
allocate_order_start  �����������ڵ�������ʼ����ʱ�䣨���У����������һ������������ʱ�䣩
allocate_order_out  �����������ڵ�������ʼ����ʱ�䣨���У����������һ������������ʱ�䣩
allocate_order_start_onself �����������ڵ�������ʼ���ʱ�䣨���У����������һ�����������ʱ�䣩
allocate_order_finish_onself  �����������ڵ�����������ʱ�䣨���У����������һ��������������ʱ�䣩
lock_last_modified_time ����������ʱ��
no_problems_order_uptime  �����ͷ��󵥷���ʱ��
outing_stock_time ������ʱ���񴥷�ʱ��=��������WMS�Ŀɼ����ʼʱ��
picking_time  �����ļ��������ʱ�䣨��Ӧ���һ�����������ʱ�䣩
order_pack_time �����Ĵ��ʱ��
shipping_time �����ķ���ʱ��
oos_num �����ж�Ӧ��ȱ����Ʒ��
type  ����ȱ����Ʒ��Ӧ��ȱ�����ͣ��˴�����Ƕ����Ʒȱ�����ܻ��ظ����ֶ�������
create_time �����Ǽ�ȱ����ʱ��
*/

WITH 
-- ���������ջ��������ϼܿ�ʼ/����ʱ��
t00 AS
(SELECT p2.pur_order_sn
        ,p2.depot_id
        ,SUM(p2.deliver_num) AS deliver_num     -- �ջ�����
        ,MAX(p2.gmt_created) AS gmt_created      -- �ʼ����ʱ�䣬Ҳ���ϼܿ�ʼʱ��
        ,MAX(p3.finish_time) AS finish_time     -- �ϼܽ���ʱ��
FROM jolly.who_wms_pur_deliver_goods p2 
INNER JOIN jolly.who_wms_pur_deliver_info  p3 
                 ON p2.deliver_id = p3.deliver_id
WHERE p2.type = 2
GROUP BY p2.pur_order_sn
        ,p2.depot_id
),
t01 AS
(SELECT p1.order_id
        ,p1.from_depot_id
        ,p1.allocate_order_sn
        ,MAX(demand_gmt_created) AS demand_gmt_created
        ,MAX(allocate_gmt_created) AS allocate_gmt_created
        ,MAX(out_time) AS out_time
        ,FROM_UNIXTIME(MAX(t00.gmt_created)) AS deli_gmt_created
        ,FROM_UNIXTIME(MAX(t00.finish_time))  AS finish_time
        ,SUM(p1.allocate_num) AS allocate_num 
FROM zydb.dw_allocate_out_node p1
LEFT JOIN t00
             ON p1.allocate_order_sn=t00.pur_order_sn 
WHERE 1=1
     AND p1.to_depot_id = 6     -- ����HK
/*     AND p1.allocate_gmt_created  >= DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),60)
     AND p1.allocate_gmt_created < DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),0)*/
GROUP BY p1.order_id
        ,p1.from_depot_id
        ,p1.allocate_order_sn
),
t02 AS
(SELECT p.order_id
        ,p.order_sn
        ,t01.allocate_order_sn
        ,t01.from_depot_id
        ,p.goods_number
        ,p.original_goods_number
        ,p.depot_id
        ,p.is_shiped
        ,p.pay_time
        ,p.order_check_time
        ,p.lock_check_time
        ,t01.demand_gmt_created AS allocate_demand_start    -- ��������ʼʱ��
        ,t01.allocate_gmt_created AS allocate_order_start    -- �����������ڵ�������ʼ����ʱ��
        ,t01.out_time AS allocate_order_out    -- ����������ʱ��
        ,t01.deli_gmt_created AS allocate_order_start_onself    -- �����������ڵ�������ʼ���ʱ��
        ,t01.finish_time AS allocate_order_finish_onself    -- �����������ڵ�����������ʱ��
        ,p.lock_last_modified_time
        ,p.no_problems_order_uptime
        ,p.outing_stock_time
        ,p.picking_time
        ,p.order_pack_time
        ,p.shipping_time
        ,t01.allocate_num
        ,p4.oos_num
        ,p4.type
        ,FROM_UNIXTIME(p4.create_time) AS create_time
FROM zydb.dw_order_node_time p
LEFT JOIN t01
             ON p.order_id = t01.order_id 
LEFT JOIN jolly.who_wms_order_oos_log p4 
             ON p.order_id = p4.order_id
WHERE 1=1
     AND p.pay_time >= '2017-01-01 00:00:00'  
-- AND p.pay_time < '2017-10-01 00:00:00'
--  >= DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),7)
--  < DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd'),0)
     AND p.depot_id = 6 --ֻȡ6
),

t03 AS
(SELECT order_sn
        ,allocate_order_sn
        ,from_depot_id
        ,TO_DATE(pay_time) AS pay_date
        ,SUBSTR(CAST(pay_time AS string), 1, 7) AS pay_month
        ,allocate_num
        ,allocate_demand_start
        ,allocate_order_start
        ,allocate_order_out
        ,allocate_order_start_onself
        ,allocate_order_finish_onself
        ,((UNIX_TIMESTAMP(allocate_order_start) - UNIX_TIMESTAMP(allocate_demand_start)) / 3600) AS allocate_response_duration    -- ����������Ӧʱ�� = ����������ʱ�� - ������������ʱ��
        ,((UNIX_TIMESTAMP(allocate_order_out) - UNIX_TIMESTAMP(allocate_order_start)) / 3600) AS allocate_work_duration    -- ��������ҵʱ�� = ����ʱ�� - ����������ʱ��
        ,((UNIX_TIMESTAMP(allocate_order_start_onself) - UNIX_TIMESTAMP(allocate_order_out)) / 3600) AS allocate_onway_duration    -- ������;ʱ�� = ��ʼ���ʱ�� - ����ʱ��
        ,((UNIX_TIMESTAMP(allocate_order_finish_onself) - UNIX_TIMESTAMP(allocate_order_start_onself)) / 3600) AS allocate_onshelf_duration    -- �������ϼ�ʱ�� = ������ʱ�� - ��ʼ���ʱ��
FROM t02
WHERE allocate_demand_start IS NOT NULL
AND allocate_order_start IS NOT NULL
AND allocate_order_out IS NOT NULL
AND allocate_order_start_onself IS NOT NULL
AND allocate_order_finish_onself IS NOT NULL
AND allocate_order_start > allocate_demand_start
)

-- ÿ��������ڵ�ƽ��ʱ��
SELECT pay_date
        ,AVG(allocate_response_duration) AS ��������ƽ����Ӧʱ��
        ,AVG(allocate_work_duration) AS ������ƽ����ҵʱ��
        ,AVG(allocate_onway_duration) AS ������ƽ����;ʱ��
        ,AVG(allocate_onshelf_duration) AS ������ƽ���ϼ�ʱ��
        ,SUM(allocate_num) AS ��������
        ,COUNT(order_sn) AS ��������
FROM t03
GROUP BY pay_date
ORDER BY pay_date;

-- ��ѯĳһ��Ķ���
SELECT * 
FROM t03
WHERE pay_date IN ('2017-11-08', '2017-11-09')
     AND allocate_work_duration < 0
;



-- ÿ�µ������ڵ�ƽ��ʱ��
SELECT pay_month
        ,AVG(allocate_response_duration) AS ��������ƽ����Ӧʱ��
        ,AVG(allocate_work_duration) AS ������ƽ����ҵʱ��
        ,AVG(allocate_onway_duration) AS ������ƽ����;ʱ��
        ,AVG(allocate_onshelf_duration) AS ������ƽ���ϼ�ʱ��
        ,SUM(allocate_num) AS ��������
        ,COUNT(order_sn) AS ��������
FROM t03
WHERE pay_date < TO_DATE(DATE_SUB(NOW(), 5))  -- ȡ6����ǰ��֧������
     AND pay_date NOT IN ('2017-11-08', '2017-11-09')
GROUP BY pay_month
ORDER BY pay_month;

-- =====================================================
-- HK�ֶ�����Ʒ������δ������Ʒ����������������Ʒ��
-- =====================================================

WITH 
-- �ϼ���⵽HK��
t00 AS
(SELECT p2.pur_order_sn
        ,SUM(p2.deliver_num) AS deliver_num     -- �ϼ�����
        ,MAX(p2.gmt_created) AS gmt_created      -- �ϼܿ�ʼʱ��
        ,MAX(p3.finish_time) AS finish_time     -- �ϼܽ���ʱ��
FROM jolly.who_wms_pur_deliver_goods p2 
INNER JOIN jolly.who_wms_pur_deliver_info  p3 
                 ON p2.deliver_id = p3.deliver_id AND p2.type = 2
GROUP BY p2.pur_order_sn
), 
-- ���������δ��������
t02 AS
(SELECT order_id
        ,SUM(num) AS still_need_num
FROM jolly_oms.who_wms_goods_need_lock_detail
GROUP BY order_id
),
-- ����������
t01 AS
(SELECT p2.order_id
        ,SUBSTR(p2.pay_time, 1, 10) AS pay_date
        ,p2.depot_id
        ,p2.original_goods_number
        ,t02.still_need_num
        ,SUM(NVL(p1.demand_allocate_num, 0)) AS demand_allocate_num        -- ����������Ʒ����
        ,SUM(NVL(CASE WHEN p1.allocate_gmt_created IS NULL THEN NULL ELSE p1.demand_allocate_num END, 0)) AS order_allocate_num          -- ���ɵ�������Ʒ����
        ,SUM(NVL(CASE WHEN p1.out_time IS NULL THEN NULL ELSE p1.demand_allocate_num END, 0)) AS allocate_out_num          -- ����������Ʒ����
        ,SUM(NVL(CASE WHEN t00.finish_time IS NULL THEN NULL ELSE p1.demand_allocate_num END, 0)) AS allocate_onshelf_num          -- �ϼܵ�HK����Ʒ����
FROM zydb.dw_order_node_time p2
LEFT JOIN zydb.dw_allocate_out_node p1
             ON p1.order_id = p2.order_id
LEFT JOIN t00 
             ON t00.pur_order_sn = p1.allocate_order_sn
LEFT JOIN t02 
             ON p1.order_id = t02.order_id
GROUP BY p2.order_id
        ,SUBSTR(pay_time, 1, 10)
        ,p2.depot_id
        ,p2.original_goods_number
        ,t02.still_need_num
)

SELECT pay_date
        ,COUNT(order_id) AS order_num
        ,SUM(original_goods_number) AS org_goods_num
        ,SUM(still_need_num) AS still_need_num
        ,SUM(demand_allocate_num) AS demand_allocate_num
        ,SUM(order_allocate_num) AS order_allocate_num
        ,SUM(allocate_out_num) AS allocate_out_num
        ,SUM(allocate_onshelf_num) AS allocate_onshelf_num
FROM t01
WHERE pay_date >= '2017-11-20'
     AND depot_id = 6
GROUP BY pay_date
ORDER BY pay_date
;

SELECT *
FROM t01
LIMIT 10;



-- ÿ�����������Ʒ����
SELECT SUBSTR(p1.out_time, 1, 10) AS out_date
        ,SUM(p1.demand_allocate_num) AS demand_allocate_num
FROM zydb.dw_allocate_out_node p1
WHERE p1.out_time >= '2017-11-23'
GROUP BY SUBSTR(p1.out_time, 1, 10)
ORDER BY out_date
;

WITH 
-- ���������ջ��������ϼܿ�ʼ/����ʱ��
t00 AS
(SELECT p2.pur_order_sn
        ,p2.depot_id
        ,SUM(p2.deliver_num) AS deliver_num     -- �ջ�����
        ,MAX(p2.gmt_created) AS gmt_created      -- �ʼ����ʱ�䣬Ҳ���ϼܿ�ʼʱ��
        ,MAX(p3.finish_time) AS finish_time     -- �ϼܽ���ʱ��
FROM jolly.who_wms_pur_deliver_goods p2 
INNER JOIN jolly.who_wms_pur_deliver_info  p3 
                 ON p2.deliver_id = p3.deliver_id
WHERE p2.type = 2
GROUP BY p2.pur_order_sn
        ,p2.depot_id
),
-- ÿ��HK���ջ���Ʒ����
t01 AS
(SELECT FROM_UNIXTIME(t00.gmt_created, 'yyyy-MM-dd') AS onshelf_begin_date
        ,SUM(t00.deliver_num) AS deliver_num     -- �ջ�����
FROM t00
WHERE t00.depot_id = 6 
     AND t00.gmt_created >= UNIX_TIMESTAMP('2017-11-20')
GROUP BY FROM_UNIXTIME(gmt_created, 'yyyy-MM-dd')
)


ORDER BY onshelf_begin_date






