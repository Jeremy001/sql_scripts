/*
内容：上架明细
日期：20171120
作者：Neo王政鸣
 */

-- impala hue =======================================
WITH
t1 AS
(SELECT a.depot_id
        ,b.on_shelf_sn
        ,b.on_shelf_admin_id AS onshelf_staff_id
        ,c.user_name AS onshelf_staff_name
        ,FROM_UNIXTIME(b.gmt_created) AS onshelf_begin_time
        ,FROM_UNIXTIME(b.on_shelf_finish_time) AS onshelf_finish_time
        --,b.total_num AS need_onshelf_num
        ,SUM(a.checked_num) AS onshelf_num
FROM jolly.who_wms_on_shelf_goods a
LEFT JOIN jolly.who_wms_on_shelf_info b
             ON a.on_shelf_id=b.on_shelf_id
LEFT JOIN jolly.who_rbac_user c
             ON b.on_shelf_admin_id = c.user_id
WHERE a.gmt_created >=UNIX_TIMESTAMP('${data_date}','yyyyMMdd')
AND a.gmt_created < UNIX_TIMESTAMP(DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP('${data_date}','yyyyMMdd')),1))
AND b.on_shelf_finish_time >=UNIX_TIMESTAMP('${data_date}','yyyyMMdd')
AND b.on_shelf_finish_time < UNIX_TIMESTAMP(DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP('${data_date}','yyyyMMdd')),1))
GROUP BY a.depot_id
        ,b.on_shelf_sn
        ,b.on_shelf_admin_id
        ,c.user_name
        ,FROM_UNIXTIME(b.gmt_created)
        ,FROM_UNIXTIME(b.on_shelf_finish_time)
)
SELECT *
FROM t1;

-- impala 客户端 ===============================================
WITH
t1 AS
(SELECT a.depot_id
        ,b.on_shelf_sn
        ,b.on_shelf_admin_id AS onshelf_staff_id
        ,c.user_name AS onshelf_staff_name
        ,FROM_UNIXTIME(b.gmt_created) AS onshelf_begin_time
        ,FROM_UNIXTIME(b.on_shelf_finish_time) AS onshelf_finish_time
        --,b.total_num AS need_onshelf_num
        ,SUM(a.checked_num) AS onshelf_num
FROM jolly.who_wms_on_shelf_goods a
LEFT JOIN jolly.who_wms_on_shelf_info b
    ON a.on_shelf_id=b.on_shelf_id
LEFT JOIN jolly.who_rbac_user c
             ON b.on_shelf_admin_id = c.user_id
WHERE a.gmt_created >=UNIX_TIMESTAMP('$[&data_date]','yyyyMMdd')
AND a.gmt_created < UNIX_TIMESTAMP(DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP('$[&data_date]','yyyyMMdd')),1))
AND b.on_shelf_finish_time >=UNIX_TIMESTAMP('$[&data_date]','yyyyMMdd')
AND b.on_shelf_finish_time < UNIX_TIMESTAMP(DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP('$[&data_date]','yyyyMMdd')),1))
GROUP BY a.depot_id
        ,b.on_shelf_sn
        ,b.on_shelf_admin_id
        ,c.user_name
        ,FROM_UNIXTIME(b.gmt_created)
        ,FROM_UNIXTIME(b.on_shelf_finish_time)
)
SELECT COUNT(*)
FROM t1;


-- hive 客户端 ===============================================
WITH
t1 AS
(SELECT a.depot_id
        ,b.on_shelf_sn
        ,b.on_shelf_admin_id AS onshelf_staff_id
        ,c.user_name AS onshelf_staff_name
        ,FROM_UNIXTIME(b.gmt_created) AS onshelf_begin_time
        ,FROM_UNIXTIME(b.on_shelf_finish_time) AS onshelf_finish_time
        --,b.total_num AS need_onshelf_num
        ,SUM(a.checked_num) AS onshelf_num
FROM jolly.who_wms_on_shelf_goods a
LEFT JOIN jolly.who_wms_on_shelf_info b
             ON a.on_shelf_id=b.on_shelf_id
LEFT JOIN jolly.who_rbac_user c
             ON b.on_shelf_admin_id = c.user_id
WHERE a.gmt_created >=UNIX_TIMESTAMP('$[&data_date]','yyyyMMdd')
AND a.gmt_created < UNIX_TIMESTAMP(DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP('$[&data_date]','yyyyMMdd')),1), 'yyyy-MM-dd')
AND b.on_shelf_finish_time >=UNIX_TIMESTAMP('$[&data_date]','yyyyMMdd')
AND b.on_shelf_finish_time < UNIX_TIMESTAMP(DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP('$[&data_date]','yyyyMMdd')),1), 'yyyy-MM-dd')
GROUP BY a.depot_id
        ,b.on_shelf_sn
        ,b.on_shelf_admin_id
        ,c.user_name
        ,FROM_UNIXTIME(b.gmt_created)
        ,FROM_UNIXTIME(b.on_shelf_finish_time)
)
SELECT COUNT(*)
FROM t1;


-- hive 推送 ===============================================
WITH
t1 AS
(SELECT a.depot_id
        ,b.on_shelf_sn
        ,b.on_shelf_admin_id AS onshelf_staff_id
        ,c.user_name AS onshelf_staff_name
        ,FROM_UNIXTIME(b.gmt_created) AS onshelf_begin_time
        ,FROM_UNIXTIME(b.on_shelf_finish_time) AS onshelf_finish_time
        --,b.total_num AS need_onshelf_num
        ,SUM(a.checked_num) AS onshelf_num
FROM jolly.who_wms_on_shelf_goods a
LEFT JOIN jolly.who_wms_on_shelf_info b
             ON a.on_shelf_id=b.on_shelf_id
LEFT JOIN jolly.who_rbac_user c
             ON b.on_shelf_admin_id = c.user_id
WHERE a.gmt_created >= UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE(), 1), 'yyyy-MM-dd')
AND a.gmt_created < UNIX_TIMESTAMP(CURRENT_DATE(), 'yyyy-MM-dd')
AND b.on_shelf_finish_time >=UNIX_TIMESTAMP(DATE_SUB(CURRENT_DATE(), 1), 'yyyy-MM-dd')
AND b.on_shelf_finish_time < UNIX_TIMESTAMP(CURRENT_DATE(), 'yyyy-MM-dd')
GROUP BY a.depot_id
        ,b.on_shelf_sn
        ,b.on_shelf_admin_id
        ,c.user_name
        ,FROM_UNIXTIME(b.gmt_created)
        ,FROM_UNIXTIME(b.on_shelf_finish_time)
)
SELECT COUNT(*)
FROM t1;



-- ================================================
/*
内容：质检明细
作者：Neo王政鸣
类型：hive & impala
时间：20171122
 */


-- hive hue 带日期参数 ===================================

SELECT p2.depot_id
        ,p2.on_shelf_id
        ,p2.on_shelf_sn
        ,FROM_UNIXTIME(p2.gmt_created) AS
        ,SUM(checked_num) AS check_goods_num
FROM jolly.who_wms_on_shelf_info p2
LEFT JOIN jolly.who_wms_on_shelf_goods a
             ON a.on_shelf_id = p2.on_shelf_id
WHERE p2.gmt_created >=UNIX_TIMESTAMP('${data_date}','yyyyMMdd')
     AND p2.gmt_created < UNIX_TIMESTAMP(DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP('${data_date}','yyyyMMdd')),1),'yyyy-MM-dd')
GROUP BY depot_id





jolly.who_wms_delivered_receipt_info
