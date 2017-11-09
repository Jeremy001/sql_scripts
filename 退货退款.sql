-- 仓储部门客诉率
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
WITH t1 AS
(SELECT p1.returned_order_id
        ,p3.order_sn
        ,p1.return_reason
        ,p1.duty_depart    -- 责任部门
        ,p2.depot_id
        ,p1.check_return_reason    -- 一级原因
        ,p4.reason_name_cn AS reason_name1
        ,p1.second_check_return_reason    -- 二级原因
        ,p5.reason_name_cn AS reason_name2
        ,FROM_UNIXTIME(p2.audit_time) AS audit_time    -- 审核时间
        ,FROM_UNIXTIME(p2.audit_time, 'yyyy-MM-dd') AS audit_date    -- 审核日期
FROM jolly.who_wms_returned_order_goods p1
LEFT JOIN jolly.who_wms_returned_order_info p2 
             ON p1.returned_rec_id = p2.returned_rec_id
LEFT JOIN jolly.who_order_info p3
             ON p1.returned_order_id = p3.order_id
LEFT JOIN jolly.who_wms_returned_goods_reason p4
             ON p1.check_return_reason = p4.reason_id
LEFT JOIN jolly.who_wms_returned_goods_reason p5
             ON p1.second_check_return_reason = p5.reason_id
WHERE (p1.check_return_reason IN (43, 44, 49) OR p1.second_check_return_reason IN (3, 4))       -- 归属到仓库责任的原因
     AND return_status IN (1, 3, 4, 5, 6, 8)        -- 会推送到仓库的退货退款单的状态
     AND p2.audit_time >= UNIX_TIMESTAMP('2017-10-01')      -- 10月审核的
     AND p2.audit_time < UNIX_TIMESTAMP('2017-11-01')
     AND p2.
GROUP BY p1.returned_order_id
        ,p3.order_sn
        ,p1.return_reason
        ,p1.duty_depart    -- 责任部门
        ,p2.depot_id
        ,p1.check_return_reason    -- 一级原因
        ,p4.reason_name_cn
        ,p1.second_check_return_reason    -- 二级原因
        ,p5.reason_name_cn
        ,FROM_UNIXTIME(p2.audit_time)    -- 审核时间
        ,FROM_UNIXTIME(p2.audit_time, 'yyyy-MM-dd')
ORDER BY audit_time DESC
)
SELECT depot_id
        ,audit_date
        ,COUNT(DISTINCT order_sn) AS order_num
FROM t1
GROUP BY depot_id
        ,audit_date
ORDER BY depot_id
        ,audit_date
;

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



