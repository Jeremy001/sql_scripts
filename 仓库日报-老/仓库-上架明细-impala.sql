/*
作者：王政鸣
更新时间：2017-10-24
SQL脚本类型：impala
*/

-- 上架单列表
SELECT t1.depot_id
        ,t1.on_shelf_sn
        ,t1.total_num AS onshelf_goods_num
        ,t1.on_shelf_admin_id AS onshelf_staff_id
        ,t2.user_name AS onshelf_staff_name
        ,FROM_UNIXTIME(t1.gmt_created) AS onshelf_begin_time
        ,FROM_UNIXTIME(t1.on_shelf_finish_time) AS onshelf_finish_time
FROM JOLLY.who_wms_on_shelf_info t1
LEFT JOIN jolly.who_rbac_user t2 
             ON t1.on_shelf_admin_id = t2.user_id
WHERE t1.status = 3     -- 上架完成
     AND t1.on_shelf_finish_time >= UNIX_TIMESTAMP(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd'), 1))
     AND t1.on_shelf_finish_time < UNIX_TIMESTAMP(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd'), 0))
ORDER BY t1.depot_id
        ,FROM_UNIXTIME(t1.gmt_created)
;
