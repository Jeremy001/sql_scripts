
WITH t1 AS
(select
               p1.goods_id
               ,regexp_replace(p1.goods_name,',','.') goods_name
               ,p1.cat_level1_name
               ,p1.cat_level2_name
               ,p1.cat_level3_name
               ,p1.cat_level4_name
               ,p1.cat_level5_name
               ,p1.cat_level6_name
               ,p1.cat_level7_name
               ,p3.pattern_key_name
               ,p3.`desc`  key_name_desc
               ,p4.pattern_value
               ,p4.`desc` pattern_value_desc
from
               (
                       select 
                            b.goods_id
                            ,b.goods_name
                            ,b.cat_level1_id 
                            ,b.cat_level1_name 
                            ,b.cat_level2_name
                            ,b.cat_level3_name
                            ,b.cat_level4_name
                            ,b.cat_level5_name
                            ,b.cat_level6_name
                            ,b.cat_level7_name
                       from 
                        zydb.dim_jc_goods b 
                       inner join jolly.who_goods a on b.goods_id=a.goods_id
                       group by
                           b.goods_id
                           ,b.goods_name
                           ,b.cat_level1_id 
                            ,b.cat_level1_name 
                            ,b.cat_level2_name
                            ,b.cat_level3_name
                            ,b.cat_level4_name
                            ,b.cat_level5_name
                            ,b.cat_level6_name
                            ,b.cat_level7_name
               ) p1
left join
                  jolly.who_pattern_relation p2 on p1.goods_id=p2.goods_id
left  join 
                  jolly.who_pattern_key p3 on  p2.key_id=p3.pattern_key_id
left  join 
                  jolly.who_pattern_value p4 on p4.pattern_key_id=p3.pattern_key_id and p2.value_id=p4.pattern_value_id
)

SELECT * 
FROM t1
WHERE pattern_key_name=category
LIMIT 10;