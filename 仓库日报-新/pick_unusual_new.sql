-- 作者：王政鸣
-- 更新时间：2018-03-14
-- SQL脚本类型：hive

-- 0.选定zybiro库，zybiro账号只对zybiro库有DML和DDL操作权限
USE zybiro;

-- 1.DROP TABLE
DROP TABLE IF EXISTS zybiro.neo_pick_unusual;

-- 2.CREATE TABLE
CREATE TABLE zybiro.neo_pick_unusual
AS
SELECT p1.depot_id
        ,p1.order_id
        ,p1.order_sn
        ,p1.picking_id
        ,p1.picking_sn
        ,p1.goods_sn
        ,p1.sku_id
        ,p1.picking_total_num
        ,p1.exception_num
        ,(CASE WHEN p1.exception_problem = 1 THEN '1商品数量多出'
               WHEN p1.exception_problem = 2 THEN '2商品数量缺少'
               WHEN p1.exception_problem = 3 THEN '3商品质量有问题'
               WHEN p1.exception_problem = 4 THEN '4商品不属于该订单'
               ELSE '其他'
          END) AS exception_type
        ,(CASE WHEN p1.status = 0 THEN '0未处理'
               WHEN p1.status = 1 THEN '1已处理'
               ELSE '其他'
          END) AS status
        ,(CASE WHEN p1.op_type = 1 THEN '1通知客服退货'
               WHEN p1.op_type = 2 THEN '2货物找到'
               ELSE '其他'
          END) AS op_type
        ,p1.op_remark
FROM jolly.who_wms_picking_exception_detail AS p1
WHERE p1.gmt_created >= UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE(), 1), 'yyyy-MM-dd')
  AND p1.gmt_created <  UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE(), 0), 'yyyy-MM-dd')
ORDER BY p1.depot_id
        ,p1.order_id
;
