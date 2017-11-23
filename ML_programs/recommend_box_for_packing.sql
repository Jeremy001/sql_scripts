-- 1.包裹称重信息表：jolly.who_wms_weigh_package_info
-- 2.订单包裹打包信息表：jolly.who_wms_order_package
-- 3.订单商品表：jolly.who_order_goods
-- 4.订单表：jolly.who_order_info
-- 5.商品属性表：zydb.dim_jc_goods
-- 6.订单发运信息表 jolly.who_wms_order_shipping_info
-- 7.物料信息表：jolly.who_wms_material

WITH 
-- 明细数据
t1 AS
(SELECT p1.order_id    -- 订单id
        ,p1.order_sn    -- 订单编号
        --,p1.depot_id    -- 订单所属仓库id
        --,p2.result_code    -- 称重结果代码
        --,p2.remark    -- 称重结果说明
        --,p3.shipping_no    -- 货运单号
        
        -- 以下是打包耗材的信息
        ,p3.material_id    -- 打包耗材ID
        ,p3.material_sn    -- 打包耗材编码
        ,p3.material_name    -- 打包耗材名称
        ,p3.material_weight    -- 打包耗材重量
        ,p3.material_volume    -- 打包耗材体积
        ,p3.material_standard   -- 打包耗材的标准尺寸
        
        -- 以下是包裹的信息，包括各个重量，体积，尺寸等
        ,p2.weight      -- 包裹重量
        ,p6.package_weight    -- 包裹重量
        ,p6.express_paper_weight    -- 面单重量，即承运商用于计算费用的重量
        ,p6.package_size    -- 包裹尺寸规格
        ,p6.package_type    -- 0箱子， 1袋子
        --,p6.real_shipping_id    -- 物流承运商id
        --,p6.real_shipping_name    -- 物流承运商名称
        ,p6.package_num AS package_goods_num   -- 包裹内商品数量
        ,p6.goods_weight AS total_goods_weight    -- 商品总重量
        ,p6.real_shipping_price    -- 系统计算出的实际运费
        ,p6.total_packages    -- 包裹数量
        ,p6.package_volume_weight    -- 包裹抛重，即根据体积计算
        ,p6.total_volume    -- 包裹总体积
        
        -- 以下是商品信息
        ,p4.goods_id    -- 订单所含商品id
        ,p4.sku_id    -- 订单所含skuid
        ,p4.sku_value    -- sku属性（序列化）
        ,p4.goods_number    -- 商品数量
        ,p4.goods_price    -- 商品成交价格（美元）
        ,p4.in_price     -- 商品采购价格（人民币）
        ,p5.goods_weight     -- 商品重量
        ,p5.cat_level1_name    -- 商品的一级类目名称
        ,p5.cat_level1_id    -- 商品的一级类目id
        ,p5.cat_level2_name    -- 商品的二级类目名称
        ,p5.cat_level2_id    -- 商品的二级类目id
        ,p5.cat_level3_name    -- -- 商品的三级类目名称
        ,p5.cat_level3_id    -- 商品的三级类目id
FROM jolly.who_order_info p1
LEFT JOIN jolly.who_wms_weigh_package_info p2 
             ON p1.order_id = p2.order_id
          AND p2.result_code IN (0, 4)    -- 称重成功和抛重过高
LEFT JOIN jolly.who_wms_order_package p3
             ON p1.order_id = p3.order_id
LEFT JOIN jolly.who_wms_order_shipping_info p6
             ON p1.order_id = p6.order_id
LEFT JOIN jolly.who_order_goods p4
             ON p1.order_id = p4.order_id
LEFT JOIN zydb.dim_jc_goods p5
             ON p4.goods_id = p5.goods_id
WHERE p1.is_shiped = 1
     AND p1.order_status = 1
     AND p1.depot_id IN (4, 5, 6)
)

-- 数据行数
SELECT COUNT(*)
FROM t1;

-- 前20条数据
SELECT * 
FROM t1
LIMIT 20;


-- 查询打包物料
-- jolly.who_wms_material 
SELECT p1.* 
FROM jolly.who_wms_material p1
WHERE p1.material_type = 3    -- 3代表打包物料
     AND p1.status = 1    -- 1代表正常，非禁用
;

-- 查询物料的库存数量
-- jolly.who_wms_material_stock_detail

