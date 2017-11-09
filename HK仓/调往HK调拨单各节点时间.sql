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
        ,SUM(p2.deliver_num) AS deliver_num     -- �ջ�����
        ,MAX(p2.gmt_created) AS gmt_created      -- �ʼ����ʱ�䣬Ҳ���ϼܿ�ʼʱ��
        ,MAX(p3.finish_time) AS finish_time     -- �ϼܽ���ʱ��
FROM jolly.who_wms_pur_deliver_goods p2 
INNER JOIN jolly.who_wms_pur_deliver_info  p3 
                 ON p2.deliver_id = p3.deliver_id AND p2.type = 2
GROUP BY p2.pur_order_sn
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
/*     AND p1.allocate_gmt_created  >= DATE_SUB(FROM_UNIXTIME(unix_timestamp(),'yyyy-MM-dd'),60)
     AND p1.allocate_gmt_created < DATE_SUB(FROM_UNIXTIME(unix_timestamp(),'yyyy-MM-dd'),0)*/
GROUP BY p1.order_id
        ,p1.from_depot_id
        ,p1.allocate_order_sn
),
t02 AS
(SELECT p.order_id
        ,p.order_sn
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
--  >= DATE_SUB(FROM_UNIXTIME(unix_timestamp(),'yyyy-MM-dd'),7)
--  < DATE_SUB(FROM_UNIXTIME(unix_timestamp(),'yyyy-MM-dd'),0)
     AND p.depot_id = 6 --ֻȡ6
),

t03 AS
(SELECT order_sn
        ,from_depot_id
        ,TO_DATE(pay_time) AS pay_date
        ,SUBSTR(CAST(pay_time AS string), 1, 7) AS pay_month
        ,allocate_num
        ,allocate_demand_start
        ,allocate_order_start
        ,allocate_order_out
        ,allocate_order_start_onself
        ,allocate_order_finish_onself
        ,((unix_timestamp(allocate_order_start) - unix_timestamp(allocate_demand_start)) / 3600) AS allocate_response_duration    -- ����������Ӧʱ�� = ����������ʱ�� - ������������ʱ��
        ,((unix_timestamp(allocate_order_out) - unix_timestamp(allocate_order_start)) / 3600) AS allocate_work_duration    -- ��������ҵʱ�� = ����ʱ�� - ����������ʱ��
        ,((unix_timestamp(allocate_order_start_onself) - unix_timestamp(allocate_order_out)) / 3600) AS allocate_onway_duration    -- ������;ʱ�� = ��ʼ���ʱ�� - ����ʱ��
        ,((unix_timestamp(allocate_order_finish_onself) - unix_timestamp(allocate_order_start_onself)) / 3600) AS allocate_onshelf_duration    -- �������ϼ�ʱ�� = ������ʱ�� - ��ʼ���ʱ��
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
GROUP BY pay_month
ORDER BY pay_month;
