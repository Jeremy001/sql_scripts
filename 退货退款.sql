
-- 客户客诉退货流程：
-- 1.提交退货退款申请
-- 2.客服接收到申请，审核原因
-- 3.对于确实需要退货退款的，审核通过，erp系统根据退货原因的规则，推送至对应的业务部门；
-- 4.各业务部门在erp中收到推送，核实，如果是自己部门的原因，进行确认，如果不是自己的原因，则推送至二级部门

-- jolly.who_wms_returned_order_goods
-- return_reason：客户提交时选择的原因；
-- check_return_reason：客服确认的一级原因
-- second_check_return_reason：业务部门确认的二级原因
-- duty_part：责任部门

-- jolly.who_wms_returned_order_info
-- return_status：退货退款状态，0：未审核，1：已审核，2：审核未通过，3：已寄回，4：已收货，5：已退货退款，6：部分收货，7：已关闭，8：Logistics Added
-- 说明：客户提交后，状态是0未审核；
-- 如果退货退款不成立，状态会改成2审核未通过；
-- 如果经过协商发红包就可以处理，不需要推送到业务方，那就改成7已关闭；
-- 如果需要推送给业务方，则改成1已审核
-- 如果需要上门取件，物流商上门取件了，那么就变成8Logistics Added
-- 已审核的后续状态是：客户填写面单，告知面单号：3已寄回；
-- 仓库收到货品并入库：4已收货；如果只有部分到货，则是6部分收货；
-- 客服进行退款：已退货退款

-- 查询明细：
WITH t1 AS
(SELECT p1.returned_order_id
        ,p3.order_sn
        ,p2.depot_id
        ,p6.reason_name_cn AS 客户退货原因
        ,p4.reason_name_cn AS 审核一级原因
        ,p5.reason_name_cn AS 审核二级原因
        ,DECODE(p1.duty_depart, 0, '其他', 1, '品控', 2, '后端供应链', 3, '仓库',  4, '文案翻译') AS 未确认前责任部门
        ,DECODE(p7.depart_duty, 0, '其他', 1, '品控', 2, '后端供应链', 3, '仓库',  4, '文案翻译') AS 确认责任部门
        ,DECODE(p7.push_status, 1, '未确认', 2, '已确认') AS 推送状态
        ,FROM_UNIXTIME(p2.apply_time) AS 申请时间
        ,FROM_UNIXTIME(p1.gmt_created) AS 添加时间
        ,FROM_UNIXTIME(p2.audit_time) AS 客服审核时间
        ,FROM_UNIXTIME(p7.gmt_push) AS 推送时间
        ,FROM_UNIXTIME(p7.gmt_confirm) AS 确认时间
FROM jolly.who_wms_returned_order_goods p1
LEFT JOIN jolly.who_wms_returned_order_info p2 
             ON p1.returned_rec_id = p2.returned_rec_id
LEFT JOIN jolly.who_order_info p3
             ON p1.returned_order_id = p3.order_id
LEFT JOIN jolly.who_wms_returned_goods_reason p4
             ON p1.check_return_reason = p4.reason_id
LEFT JOIN jolly.who_wms_returned_goods_reason p5
             ON p1.second_check_return_reason = p5.reason_id
LEFT JOIN jolly.who_wms_returned_goods_reason p6
             ON p1.return_reason = p6.reason_id
LEFT JOIN jolly.who_cs_duty_push p7
             ON p2.returned_rec_id = p7.returned_rec_id
WHERE (p1.check_return_reason IN (43, 44, 49) OR p1.second_check_return_reason IN (3, 4))       -- 可能归属到仓库责任的原因，会推送给仓库的只有49
     AND p2.return_status IN (1, 3, 4, 5, 6, 8)        -- 会推送到仓库的退货退款单的状态
     AND p1.duty_depart >= 1
GROUP BY p1.returned_order_id
        ,p3.order_sn
        ,p2.depot_id
        ,p6.reason_name_cn
        ,p4.reason_name_cn
        ,p5.reason_name_cn
        ,DECODE(p1.duty_depart, 0, '其他', 1, '品控', 2, '后端供应链', 3, '仓库',  4, '文案翻译')
        ,DECODE(p7.depart_duty, 0, '其他', 1, '品控', 2, '后端供应链', 3, '仓库',  4, '文案翻译')
        ,DECODE(p7.push_status, 1, '未确认', 2, '已确认')
        ,FROM_UNIXTIME(p2.apply_time)
        ,FROM_UNIXTIME(p1.gmt_created)
        ,FROM_UNIXTIME(p2.audit_time)
        ,FROM_UNIXTIME(p7.gmt_push)
        ,FROM_UNIXTIME(p7.gmt_confirm)
)
-- 查看前10行
SELECT * 
FROM t1
LIMIT 10;
-- 根据推送时间在10月份，筛选确认责任部门是仓储的退货退款单
SELECT *
FROM t1
WHERE 推送时间 >= '2017-10-01'
     AND 推送时间 < '2017-11-01'
     AND 确认责任部门 = '仓库'
     AND 推送状态 = '已确认';


-- =====================================================================================================

-- 退货原因
-- 客户提交的退货原因，用zydb.dim_returned_reason
-- 其实猜测也可以用jolly.who_wms_returned_goods_reason
SELECT *
FROM zydb.dim_returned_reason
LIMIT 10;

-- 一二级审核的退货原因用jolly.who_wms_returned_goods_reason退货商品原因信息表
-- first_depart： 一级责任部门：1-品控，2-后端供应链，3-仓库，4-文案翻译，0-无
-- next_depart：二级责任部门
SELECT *
FROM jolly.who_wms_returned_goods_reason
LIMIT 10;



-- 推送记录
-- jolly.who_cs_duty_push
-- 根据推送时间来计入相应月份
SELECT * 
FROM jolly.who_cs_duty_push
LIMIT 10;

-- 一个订单可能会有多个推送记录
SELECT FROM_UNIXTIME(p1.gmt_push) AS push_time
        ,FROM_UNIXTIME(p1.gmt_confirm) AS confirm_time
        ,FROM_UNIXTIME(p1.gmt_modified) AS modified_time
        ,p1.*
FROM jolly.who_cs_duty_push p1
WHERE order_sn = 'JARA17100906415601058941'
;

SELECT p1.shipping_depot_id
        ,COUNT(DISTINCT p1.order_sn) AS order_num
FROM jolly.who_cs_duty_push p1
WHERE p1.gmt_push >= UNIX_TIMESTAMP('2017-10-01')
     AND p1.gmt_push < UNIX_TIMESTAMP('2017-11-01')
     AND p1.push_status = 2
     AND p1.depart_duty = 3
GROUP BY p1.shipping_depot_id;

SELECT p1.shipping_depot_id
        ,p1.order_sn
        ,p2.reason_name_cn AS first_reason_name
        ,p3.reason_name_cn AS second_reason_name
        ,FROM_UNIXTIME(p1.gmt_push) AS push_time
        ,FROM_UNIXTIME(p1.gmt_confirm) AS confirm_time
FROM jolly.who_cs_duty_push p1
LEFT JOIN jolly.who_wms_returned_goods_reason p2
             ON p1.first_check_reason = p2.reason_id
LEFT JOIN jolly.who_wms_returned_goods_reason p3
             ON p1.second_check_reason = p3.reason_id
WHERE p1.gmt_push >= UNIX_TIMESTAMP('2017-10-01')
     AND p1.gmt_push < UNIX_TIMESTAMP('2017-11-01')
     AND p1.push_status = 2
     AND p1.depart_duty = 3
GROUP BY p1.shipping_depot_id
        ,p1.order_sn
        ,p2.reason_name_cn
        ,p3.reason_name_cn
        ,FROM_UNIXTIME(p1.gmt_push)
        ,FROM_UNIXTIME(p1.gmt_confirm);
