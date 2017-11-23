/*
????Neo Wang ???
???ʱ?䣺2017-7-4
?ű???ͣ?Impala
 */


-- ?Ʒ??
-- 1.JOLLY.WHO_GOODS
SELECT *
FROM JOLLY.WHO_GOODS
LIMIT 10;

-- ??ڼ۸?
-- 1.??λ?ֱ???ô????????ң?????Ԫ??
-- 2.MARKET_PRICE??N_PRICE?ʲô??????ô?󲿷?????????۸????
-- 459043???Ʒ???44613???Ʒ?????۸??
/*
IS_ON_SALE
IS_DELETE
IS_STOCK
IS_BEST
IS_NEW
IS_HOT
IS_PROMOTE
IS_PRESALE
IS_CUSTOMIZATION
*/

SELECT COUNT(*)
FROM JOLLY.WHO_GOODS;
-- 46????Ʒ(459043)

-- IS_ON_SALE ?Ʒ??״̬,1??,0???Ĭ?1
SELECT IS_ON_SALE
        ,COUNT(GOODS_ID)
FROM JOLLY.WHO_GOODS
GROUP BY IS_ON_SALE;
-- ???Ʒ??20??????????SKU,???GOODS_ID??
-- 1   199401
-- 0   259642

-- IS_DELETE ?Ʒɾ??״̬,1ɾ??,0δɾ??,Ĭ?0
SELECT IS_DELETE
        ,COUNT(GOODS_ID)
FROM JOLLY.WHO_GOODS
GROUP BY IS_DELETE;
-- ?????ô????????????״̬Ҳ??ɾ?????

-- IS_BEST ?Ʒ??Ʒ״̬,1??Ʒ,0?Ǿ?Ʒ??Ĭ?Ϊ0
SELECT IS_BEST
        ,COUNT(GOODS_ID)
FROM JOLLY.WHO_GOODS
WHERE IS_ON_SALE = 1
GROUP BY IS_BEST;
-- ???Ľ?20W???Ʒ????88????Ʒ
-- ?????????Ʒ??
-- 88????Ʒ?86????Ʒ??Ī?Ǿ?Ʒ?Ҫ??????????????????

-- IS_NEW ?Ʒ״̬,1Ϊ?Ʒ,0????,Ĭ?1
SELECT IS_NEW
        ,COUNT(GOODS_ID)
FROM JOLLY.WHO_GOODS
WHERE IS_ON_SALE = 1
GROUP BY IS_NEW;
-- ?Ʒ??5???????20%

-- IS_HOT ??״̬,1Ϊ??,0Ϊ????,Ĭ?0
SELECT IS_HOT
        ,COUNT(GOODS_ID)
FROM JOLLY.WHO_GOODS
WHERE IS_ON_SALE = 1
GROUP BY IS_HOT;
-- ????200???Ʒ????????ǧ???һ???

-- IS_PROMOTE ??۴??״̬,1Ϊ??۴??,0Ϊ??ؼ۴??,Ĭ?0
SELECT IS_PROMOTE
        ,COUNT(GOODS_ID)
FROM JOLLY.WHO_GOODS
WHERE IS_ON_SALE = 1
GROUP BY IS_PROMOTE;
-- ??7?????????״̬?????????????һ??

-- IS_PRESALE  ???????Ʒ(0:????;1:Ԥ?)
SELECT IS_PRESALE
        ,COUNT(GOODS_ID)
FROM JOLLY.WHO_GOODS
WHERE IS_ON_SALE = 1
    AND IS_NEW = 1
GROUP BY IS_PRESALE;
-- Ԥ??Ʒ?????ٵ?
-- ????ʲô?Ԥ??Ʒ??

-- PRICE_TYPE???۸???0:?Ʒ?۸?:SKU?۸?

-- ADD_TIME????????
SELECT FROM_UNIXTIME(ADD_TIME, 'yyyy-MM') AS ADD_MONTH
        ,COUNT(GOODS_ID) AS GOODS_NUM
FROM JOLLY.WHO_GOODS
GROUP BY FROM_UNIXTIME(ADD_TIME, 'yyyy-MM')
ORDER BY ADD_MONTH DESC;
-- 2017??????˺ܶ???ѽ??????-5-6????

-- PROVIDER_CODE 
SELECT COUNT(DISTINCT PROVIDER_CODE)
FROM JOLLY.WHO_GOODS;
-- 3117?????ѽ???ܶ?Ѿ???????˰ɣ?
SELECT COUNT(DISTINCT PROVIDER_CODE)
FROM JOLLY.WHO_GOODS
WHERE IS_ON_SALE = 1;
-- 2014??????????

-- GOODS_SEASON ??Ʒ???? 1.?? 2.? 3.? 4.?? 5.??? 6.??? 7.???? 8.?? 9.??? 10.???
SELECT GOODS_SEASON
        ,COUNT(GOODS_ID)
FROM JOLLY.WHO_GOODS
WHERE IS_ON_SALE = 1
GROUP BY GOODS_SEASON
ORDER BY GOODS_SEASON;
-- ????0????ﶬ???10????ﶬ???ôĿǰ???????????һ??GOODS_SEASON?0???
-- ?0Ҳ?10??????10???????٣?

-- IS_STOCK ?????:0 ???? 1??? 2deals 4 sku???
-- ????2DEALS?ʲô????
SELECT IS_STOCK
        ,COUNT(GOODS_ID)
FROM JOLLY.WHO_GOODS
WHERE IS_ON_SALE = 1
GROUP BY IS_STOCK;
-- ?Ҫ?????????20????Ʒ???18.5?????????ģ?

-- ITEM_CATEGORY_ID ?ƷƷ??D
-- ??? JOLLY.WHO_CATEGORY??
SELECT T1.GOODS_ID
        ,T1.GOODS_SN
        ,T1.GOODS_NAME
        ,T1.ITEM_CATEGORY_ID
        ,T2.CN_CAT_NAME
FROM JOLLY.WHO_GOODS T1
LEFT JOIN JOLLY.WHO_CATEGORY T2 ON T1.ITEM_CATEGORY_ID = T2.CAT_ID
LIMIT 10;

-- ???????????㼶?????
WITH T AS
(SELECT T1.GOODS_ID
        ,T1.GOODS_SN
        ,T1.GOODS_NAME
        ,T1.ITEM_CATEGORY_ID
        ,T2.CN_CAT_NAME
        ,T2.CAT_LEVEL
FROM JOLLY.WHO_GOODS T1
LEFT JOIN JOLLY.WHO_CATEGORY T2 ON T1.ITEM_CATEGORY_ID = T2.CAT_ID
)
SELECT T.CAT_LEVEL
        ,COUNT(T.GOODS_ID)
FROM T
GROUP BY T.CAT_LEVEL
ORDER BY T.CAT_LEVEL;



-- ON_SALE_TIME ?????? FIRST_ON_SALE_TIME ?????ϼ????
-- OFF_SALE_TIME ??????

-- LEVEL ?Ʒ?㼶??Ĭ?0, 1??A,2??B,3??C,4??D
-- ????LEVEL?ʲô??????ô?ֵģ?
SELECT LEVEL
        ,COUNT(GOODS_ID)
FROM JOLLY.WHO_GOODS
WHERE IS_ON_SALE = 1
GROUP BY LEVEL
ORDER BY LEVEL;
/*
0   59616
1   9236
2   16081
3   55943
4   58525
*/

-- ????û??Ʒ?Ϣ??п??????ֶΣ????ֶδ???????˵û??????Σ?
-- ???????ֶΣ??ܶ????ǲ???Ա????????????????

-- 2.ZYDB.DIM_JC_GOODS
-- ǰʮ????
SELECT * 
FROM ZYDB.DIM_JC_GOODS
LIMIT 10;
-- һ??????Ʒ?
SELECT CAT_LEVEL1_NAME
        ,COUNT(GOODS_ID) AS GOODS_NUM
FROM ZYDB.DIM_JC_GOODS
GROUP BY CAT_LEVEL1_NAME
ORDER BY GOODS_NUM DESC;
/*
Women's Clothing    189969
Women's Shoes   46247
Women's Accessories 40731
Men's Clothing  31197
Kids    28469
Women's Bags    28167
Baby&Moms   15299
Men's Shoes 15107
Beauty  14428
Home Decor  12568
 */

SELECT CAT_LEVEL
        ,COUNT(*)
FROM ZYDB.DIM_JC_GOODS
WHERE IS_DELETE = 0
GROUP BY CAT_LEVEL
ORDER BY CAT_LEVEL;
-- ???һ???????
/*
2   460096
3   2
    38
1   1633
 */

/*
IS_ON_SALE
IS_DELETE
IS_STOCK
IS_NEW
IS_CUSTOMIZATION
*/

-- ??Թ?Ӧ????Ʒ?
SELECT SUPP_NAME
    ,COUNT(*)
FROM ZYDB.DIM_JC_GOODS
WHERE SUPP_NAME LIKE '???'
  OR SUPP_NAME LIKE 'Test%'
GROUP BY SUPP_NAME;
/*
??Թ?Ӧ?3  7
??Թ?Ӧ?1  22
??Թ?Ӧ?2  26
Test    47
 */


-- ?Ʒ??? JOLLY.WHO_CATEGORY
SELECT * 
FROM JOLLY.WHO_CATEGORY
LIMIT 10;

-- IS_SHOW
SELECT IS_SHOW
        ,COUNT(*)
FROM JOLLY.WHO_CATEGORY
GROUP BY IS_SHOW;
/*
1   510
0   134
 */

-- CAT_LEVEL ?????
SELECT CAT_LEVEL
        ,COUNT(*)
FROM JOLLY.WHO_CATEGORY
WHERE IS_SHOW = 1
GROUP BY CAT_LEVEL
ORDER BY CAT_LEVEL;
/*
1   33
2   243
3   217
4   17
 */
-- ????㼶??ô????

-- ??˾Ŀǰ?????Ʒ???
SELECT CAT_ID
        ,PARENT_ID
        ,CN_CAT_NAME
        ,CAT_LEVEL
FROM JOLLY.WHO_CATEGORY
WHERE IS_SHOW = 1;

-- ?ݹ?ѯ??????????㼶?????????


-- ?????һ????????????




-- ?鿴ÿ??Ʒ???¼??
SELECT * 
FROM ZYDB.DW_GOODS_ON_SALE 
WHERE DATA_DATE = '20170717'
LIMIT 10;


SELECT IS_ON_SALE
    ,COUNT(GOODS_ID) 
FROM ZYDB.DW_GOODS_ON_SALE
WHERE DATA_DATE = '20170717'
GROUP BY IS_ON_SALE;


-- Υ??Ʒ
/*
key_id = 728          value_id in (7524, 7526, 7536)
728 Prohibited Goods    7536    Unprohibited    32353
728 Prohibited Goods    7526    Fuzzy Prohibited    5169
728 Prohibited Goods    7524    Completely Prohibited   6162
7524:?ȫΥ??
7526:ģ??Υ??
7536:??Υ??
*/
-- ?ϸ
SELECT goods_id
        ,key_id
        ,'Prohibited' AS key_name
        ,value_id
        ,(CASE WHEN value_id = 7524 THEN '?ȫΥ??' 
                     WHEN value_id = 7526 THEN 'ģ??Υ??' 
                     WHEN value_id = 7536 THEN '??Υ??' 
                     ELSE '??'
          END) AS value_name
FROM jolly.who_pattern_relation
WHERE key_id = 728;



SELECT key_id
    ,key_name    
    ,value_id
    ,value_name
    ,COUNT(goods_id)
FROM jolly.who_pattern_relation
WHERE key_id = 728
GROUP BY key_id
    ,key_name    
    ,value_id
    ,value_name
;







-- ??Ӧ??Ϣ??JOLLY.WHO_ESOLOO_SUPPLIER
SELECT * 
FROM JOLLY.WHO_ESOLOO_SUPPLIER
WHERE ADDRESS IS NOT NULL
LIMIT 10;
-- ?Щ??Ӧ???ľ????????????????

-- GREAT_TIME??Ҳ????ˣ??????ˣ???REATEд???REAT
-- LAST_TIME, ?????ε??ʱ?䣿ʲô?????????ʱ?䣿
-- IS_HIDE????????????0???
SELECT IS_HIDE
        ,COUNT(*)
FROM JOLLY.WHO_ESOLOO_SUPPLIER
GROUP BY IS_HIDE;
/*
1   1846
0   1856
 */
-- PRICE_RATE, ?Ӽ?????ʲô?????
-- MONTH_CAPACITY, ????????λ?ʲô??
SELECT CODE
        ,SUPP_NAME
        ,MONTH_CAPACITY
FROM JOLLY.WHO_ESOLOO_SUPPLIER
WHERE MONTH_CAPACITY IS NOT NULL
AND IS_HIDE = 0
ORDER BY MONTH_CAPACITY DESC
LIMIT 30;
-- NEW_CYCLE, ????
-- ???˵???û??̶?ʱ?䵥λ????????
SELECT CODE
        ,SUPP_NAME
        ,NEW_CYCLE
FROM JOLLY.WHO_ESOLOO_SUPPLIER
WHERE NEW_CYCLE IS NOT NULL
AND IS_HIDE = 0
ORDER BY NEW_CYCLE DESC
LIMIT 30;
/*
045 ZYСŮ?   ÿ???
04D ZY N????? ÿ?һ??
276 JM G??????  ÿ???
1TV ZY A?±?????  ÿ?
078 ZY H??ΰ?? ÿ??
051 ZY P?????ÿ??
278 JM L????չ?ó?????˾ ÿ?һ??
 */
-- ADMIN_ID, ?ɹ?ԱID
-- CANCEL_REASON, ȡ????ԭ??????˵???????ű?У?
-- SUPP_DISCOUNT, ?ɹ?????ɹ???

-- CREDIT_RANK, ???ȼ???ʲô?????Ӧ????????õȼ????
-- ?ȣ???????ˣ??????θ???û????
SELECT CREDIT_RANK
        ,COUNT(*)
FROM JOLLY.WHO_ESOLOO_SUPPLIER
GROUP BY CREDIT_RANK
ORDER BY CREDIT_RANK;
/*
0   3690
1   5
2   4
3   2
    1
 */

-- ORDER_PLATFORM, 1:???,2:?è,3:????ƽ̨,4:???ɹ???,5:1688,6:???????0:?? 
-- ??????ʲô???????ѡ??????????ѡ??????ͣ?̫??ˣ?
SELECT ORDER_PLATFORM
        ,COUNT(*)
FROM JOLLY.WHO_ESOLOO_SUPPLIER
WHERE IS_HIDE = 0
GROUP BY ORDER_PLATFORM
ORDER BY ORDER_PLATFORM;
/*
0   1144
1   67
2   19
3   4
4   321
5   271
6   26
7   4
 */

-- DELIVERY_CYCLE, ????????ʲô??λ?????
SELECT DELIVERY_CYCLE
        ,COUNT(*)
FROM JOLLY.WHO_ESOLOO_SUPPLIER
GROUP BY DELIVERY_CYCLE
ORDER BY DELIVERY_CYCLE;
/*
0   542
2   1366
3   1616
4   73
5   1
7   1
10  1
    102
 */
-- ????2-3?

-- RETURNED_DATE, ?????0??,1һ?֮?,2?????,3һ????,4?????,5?????????
SELECT RETURNED_DATE
        ,COUNT(*)
FROM JOLLY.WHO_ESOLOO_SUPPLIER
GROUP BY RETURNED_DATE
ORDER BY RETURNED_DATE;
/*
0   246
1   1777
2   527
3   920
4   37
5   95
    100
 */

-- IS_DEPOSIT, ???б?֤??0,??1?)
SELECT IS_DEPOSIT
        ,COUNT(*)
FROM JOLLY.WHO_ESOLOO_SUPPLIER
WHERE IS_HIDE = 0
GROUP BY IS_DEPOSIT;
/*
1   848
0   1008
 */
-- DEPOSIT, ??֤????
-- ?????????֤??????֤?????0????û???֤????????

-- SETTLEMENT_TYPE, ??㷽ʽ??0????᣻1????᣻2????᣻ 3??Ԥ???4????½᣻5??1.5??᣻6??2??᣻7??3??᣻8???????
SELECT SETTLEMENT_TYPE
        ,COUNT(*)
FROM JOLLY.WHO_ESOLOO_SUPPLIER
GROUP BY SETTLEMENT_TYPE
ORDER BY SETTLEMENT_TYPE;
/*
0   1495
1   241
2   1518
3   1
4   398
5   12
6   31
7   3
8   3
 */
-- ??????Ĺ?Ӧ????????
SELECT SETTLEMENT_TYPE
        ,COUNT(*)
FROM JOLLY.WHO_ESOLOO_SUPPLIER
WHERE IS_HIDE = 0
GROUP BY SETTLEMENT_TYPE
ORDER BY SETTLEMENT_TYPE;
/*
0   18
1   166
2   1314
4   313
5   12
6   30
7   3
 */
-- Ŀǰ??????ᣬ???һ????ǰ?½????

-- PROVINCE, CITY, ??Ӧ???ʡ??ı?룬????ر????أ?

-- IS_SYSTEM, ?ɹ????ڹ?Ӧ?ϵͳ?????1?,0?? ??Ӧ??????ǵ?MS???
SELECT IS_SYSTEM
        ,COUNT(*)
FROM JOLLY.WHO_ESOLOO_SUPPLIER
WHERE IS_HIDE = 0
GROUP BY IS_SYSTEM;
/*
0   11
1   1845
 */
-- ??????????????MSϵͳ
SELECT IS_SYSTEM
        ,COUNT(*)
FROM JOLLY.WHO_ESOLOO_SUPPLIER
GROUP BY IS_SYSTEM;
/*
1   1849
0   1853
 */
-- ?ǰ?Ĺ?Ӧ?????????????Ҳȡ?????ˡ?

-- SHIPPING_TYPE, ??ѷ?ʽ??1˳?ᵽ????2?????3??? 4??????? 3/4?ô??⣿????????£?
SELECT SHIPPING_TYPE
        ,COUNT(*)
FROM JOLLY.WHO_ESOLOO_SUPPLIER
WHERE IS_HIDE = 0
GROUP BY SHIPPING_TYPE
ORDER BY SHIPPING_TYPE;
/*
0   1
1   19
2   114
3   946
4   776
 */

-- OOS_DELAY_TIME ȱ????ʱʱ??h) ????????
SELECT OOS_DELAY_TIME
        ,COUNT(*)
FROM JOLLY.WHO_ESOLOO_SUPPLIER
WHERE IS_HIDE = 0
GROUP BY OOS_DELAY_TIME
ORDER BY COUNT(*) DESC;
/*
48  1201
36  257
60  154
40  120
72  24
 */
-- ?Ҫ?48Сʱ

-- CHECK_TYPE 0δѡ???1??졢2????3ȫ??
SELECT CHECK_TYPE
        ,COUNT(*)
FROM JOLLY.WHO_ESOLOO_SUPPLIER
WHERE IS_HIDE = 0
GROUP BY CHECK_TYPE
ORDER BY CHECK_TYPE;
/*
0   11
1   19
2   114
3   1712
 */
-- ?????????ȫ?죬??????ߣ?Ӧ?ü?????Ҫȫ????

-- BUYER_ADMIN_ID

-- PUR_DEMAND_PUSH_TIME, ?ɹ?????ʱ??
SELECT CODE
        ,SUPP_NAME
        ,FROM_UNIXTIME(PUR_DEMAND_PUSH_TIME)
FROM JOLLY.WHO_ESOLOO_SUPPLIER
WHERE PUR_DEMAND_PUSH_TIME >0
LIMIT 30;
/*
1PM YT A?????Ʒ    2017-07-06 08:00:00
1PN ZY M????װ    2017-07-06 08:00:00
1PP JE X??ֵ??   2017-07-05 09:20:00
1PQ JE Oŷ??????Ƽ?  2017-07-06 08:00:00
1PR ZY H???????    2017-07-06 08:00:00
1PT ZY MMuľľ?Ҿӹ?2017-07-06 08:00:00
 */
-- ?ӣ??????μ???ľ?Ȼ????һ?βɹ?????????䣡
SELECT COUNT(*)
FROM JOLLY.WHO_ESOLOO_SUPPLIER
WHERE PUR_DEMAND_PUSH_TIME > 0
    AND IS_HIDE = 0;
-- 1856?Һ???Ĺ?Ӧ????1850??й??ɹ?????????Ȼ???6?????????Ī????º????ʽ??
-- ????ģ?ORDER_PLATFROM = 4?Ĺ?Ӧ??300??
-- ???һ??ɹ??????????
WITH T AS
(SELECT CODE
        ,CONCAT_WS(':'
            ,CAST(HOUR(FROM_UNIXTIME(PUR_DEMAND_PUSH_TIME)) AS STRING)
            ,CAST(MINUTE(FROM_UNIXTIME(PUR_DEMAND_PUSH_TIME)) AS STRING)
            ) AS PUSH_TIME
FROM JOLLY.WHO_ESOLOO_SUPPLIER
WHERE PUR_DEMAND_PUSH_TIME >0
    AND IS_HIDE = 0
)
SELECT PUSH_TIME
        ,COUNT(*)
FROM T
GROUP BY PUSH_TIME
ORDER BY COUNT(*) DESC;
/*
8:0 1832
9:30    8
10:0    4
9:0 2
13:30   1
9:40    1
8:30    1
14:0    1
 */
-- ???????8:00??
-- ??????????
WITH T AS
(SELECT CODE
        ,FROM_UNIXTIME(PUR_DEMAND_PUSH_TIME, 'yyyy-MM-dd') AS PUSH_TIME
FROM JOLLY.WHO_ESOLOO_SUPPLIER
WHERE PUR_DEMAND_PUSH_TIME >0
    AND IS_HIDE = 0
)
SELECT PUSH_TIME
        ,COUNT(*)
FROM T
GROUP BY PUSH_TIME
ORDER BY PUSH_TIME DESC;
/*
2017-07-06  1826
2017-07-05  24
 */
-- ????Ŀǰ????Ĺ?Ӧ?????????????ô???

-- ORDER_TYPE ?ɹ????0-????ɹ?,1-?????ɹ?
SELECT ORDER_TYPE
        ,COUNT(*)
FROM JOLLY.WHO_ESOLOO_SUPPLIER
WHERE IS_HIDE = 0
GROUP BY ORDER_TYPE;
/*
1   75
0   1781
 */
-- ?????ɹ??Ļ????????Ӧ??????????????ɹ????

-- IS_POP ????POP???0-??1-?
SELECT IS_POP
        ,COUNT(*)
FROM JOLLY.WHO_ESOLOO_SUPPLIER
WHERE IS_HIDE = 0
GROUP BY IS_POP;
/*
1   1
0   1855
 */
-- ?????????ξ??????????????ҪȥIS_POP=0??K?ˣ?

-- MAIN_CAT_ID????Ӧ??ӪƷ??
SELECT T1.MAIN_CAT_ID
        ,T2.CN_CAT_NAME
        ,COUNT(*) AS SUPP_NUM
FROM JOLLY.WHO_ESOLOO_SUPPLIER T1
LEFT JOIN JOLLY.WHO_CATEGORY T2 ON T1.MAIN_CAT_ID = T2.CAT_ID
WHERE T1.IS_HIDE = 0
    AND T1.MAIN_CAT_ID > 0
GROUP BY T1.MAIN_CAT_ID
        ,T2.CN_CAT_NAME
ORDER BY SUPP_NUM DESC;
/*
2   Ůװ  164
59  Ь   127
35  ??   63
31  ?Ʒ  53
179 ??ͯ??Ь???  46
 */

-- SUPPLIER_NATURE ??Ӧ??? 1???????? 2?????? 3??OEM?? ????
SELECT SUPPLIER_NATURE
        ,COUNT(*)
FROM JOLLY.WHO_ESOLOO_SUPPLIER
WHERE IS_HIDE = 0
GROUP BY SUPPLIER_NATURE
ORDER BY COUNT(*) DESC;
/*
1   1448
2   372
3   23
    13
 */

-- IS_REAL_TIME_PUSH_PUR_DEMAND ????ʱ???ɹ???:0-??1-?
SELECT IS_REAL_TIME_PUSH_PUR_DEMAND
        ,COUNT(*)
FROM JOLLY.WHO_ESOLOO_SUPPLIER
WHERE IS_HIDE = 0
GROUP BY IS_REAL_TIME_PUSH_PUR_DEMAND;
-- ???ʵʱ??ò????????ʵʱ?????ÿ???8:00????


-- ??Ӧ??????JOLLY.WHO_ESOLOO_SUPPLIER_ADDRESS
-- ?????????
SELECT * 
FROM JOLLY.WHO_ESOLOO_SUPPLIER_ADDRESS
LIMIT 10;

-- ??Ӧ??Ʒ?????
SELECT * 
FROM JOLLY.WHO_ESOLOO_SUPPLIER_CAT
LIMIT 10;
-- ??Ӧ?һ?㶼??????????
SELECT AVG(CAT_NUM)
FROM 
(SELECT SUPP_ID
        ,COUNT(CAT_ID) AS CAT_NUM
FROM JOLLY.WHO_ESOLOO_SUPPLIER_CAT
GROUP BY SUPP_ID) T;
-- 3.13
-- ?Ʒ????????Ӧ?TOP30??
SELECT SUPP_ID
        ,COUNT(CAT_ID) AS CAT_NUM
FROM JOLLY.WHO_ESOLOO_SUPPLIER_CAT
GROUP BY SUPP_ID
ORDER BY CAT_NUM DESC
LIMIT 31;
/*
170 46
2392    43
2270    43
1023    41
2398    40
 */
-- ?ô??????????????????????Ҳ̫??????
WITH T1 AS
(SELECT CODE
        ,SUPP_NAME
FROM JOLLY.WHO_ESOLOO_SUPPLIER
WHERE IS_HIDE = 0
),
T2 AS
(SELECT CAST(SUPP_ID AS STRING) AS CODE
        ,COUNT(CAT_ID) AS CAT_NUM
FROM JOLLY.WHO_ESOLOO_SUPPLIER_CAT
GROUP BY CAST(SUPP_ID AS STRING)
)
SELECT T1.*
        ,T2.CAT_NUM
FROM T1 
LEFT JOIN T2 ON T1.CODE = T2.CODE
WHERE T2.CAT_NUM IS NOT NULL
ORDER BY T2.CAT_NUM DESC;


-- ??Ӧ???ڱ?WHO_ESOLOO_SUPPLIER_HOLIDAY
SELECT *
FROM JOLLY.WHO_ESOLOO_SUPPLIER_HOLIDAY
LIMIT 10;


-- =============================================== ???????ݣ????? ===================================================
/* 
 1.????գ?ÿ????????
 2.???仯???????¼??
 3.??λ?
 4.???ṹ
 */ 

-- ?ֿ?λ??============================================
-- jolly_wms.who_wms_depot_shelf_area  
-- jolly.who_wms_depot_shelf_area
-- ȥ?˽⣺
-- 1.??λ?????ô??
-- ?ش𣺻?λ????Ʒ?ĵط??????ݸ???????DG1-A01-0101
-- DG1???????ݸ1??
-- A01??A???A?????01???????????
-- 0101???????01???????У??ڶ???01???????㣬?????һ??


SELECT * 
FROM jolly_wms.who_wms_depot_shelf_area
LIMIT 10;
/* ?????
shelf_area_id: ???, ??λid
shelf_area_sn: ??λ?ţ??????ҵ??˵?Ļ?λ?Ų???ָ??????????? - ??λ - ????
depot_shelf_id??????d
stock_num??????ܿ?
 */

# ɳ??????51970????λ
SELECT COUNT(shelf_area_id)
FROM jolly_wms.who_wms_depot_shelf_area;
# CN??ֹ?519011????λ???ô???
SELECT COUNT(shelf_area_id)
FROM jolly.who_wms_depot_shelf_area;

-- ?ֿ??ܱ?=============================================
-- jolly_wms.who_wms_depot_shelf
-- jolly.who_wms_depot_shelf
SELECT * 
FROM jolly_wms.who_wms_depot_shelf
LIMIT 10;
/* ?????
shelf_id: ???, ????d
shelf_sn: ???ܺ?
depot_area_id?????id
shelf_row?????
shelf_line?????
 */

SELECT * 
FROM jolly.who_wms_depot_shelf
LIMIT 10;


# ɳ??????1549??????
SELECT COUNT(shelf_id)
FROM jolly_wms.who_wms_depot_shelf;


# ?Ʒ???ϸ
# ???????????Ŀ??????ô????
# ??????ΪʲôҪ????ʼ???ʱ?䣿
SELECT * 
FROM jolly.who_wms_goods_stock_detail
WHERE stock_num > 0
LIMIT 10;

SELECT COUNT(*)
        ,SUM(stock_num)
FROM jolly.who_wms_goods_stock_detail


SELECT * 
FROM jolly.who_wms_goods_stock_total_detail
LIMIT 10;


-- ???????
-- SA?֣?jolly_wms.who_wms_goods_stock_detail_log
-- ????֣?jolly.who_wms_goods_stock_detail_log

# change_type ?????1:?ɹ????2:?????????3:????????4:?ӯ??? 5:??????????6:???????7:??λת?,8:???9:??????10: ??⵽??ѷ,11:fba?Ʒ???12:?????,13:????????14:???쳣???15:???????16-??????????17-???????????

-- SA??
SELECT depot_id
        ,change_type
        ,SUM(change_num) AS change_num
FROM jolly_wms.who_wms_goods_stock_detail_log
WHERE depot_id = 7 
     AND change_type = 3
     -- AND change_time >= UNIX_TIMESTAMP('2017-08-01', 'yyyy-MM-dd')
     AND change_time >= UNIX_TIMESTAMP(DATE_SUB(CURRENT_TIMESTAMP(), 1)) 
     AND change_time < UNIX_TIMESTAMP(TO_DATE(CURRENT_TIMESTAMP()))
GROUP BY depot_id
        ,change_type;

-- CN??
SELECT depot_id
        ,change_type
        ,SUM(change_num) AS change_num
FROM jolly.who_wms_goods_stock_detail_log
WHERE depot_id IN (4, 5, 6)
     AND change_type = 3
     AND change_time >= UNIX_TIMESTAMP(DATE_SUB(CURRENT_TIMESTAMP(), 1)) 
     AND change_time < UNIX_TIMESTAMP(TO_DATE(CURRENT_TIMESTAMP()))
GROUP BY depot_id
        ,change_type;


-- SA??????????
SELECT FROM_UNIXTIME(p1.change_time, 'yyyy-MM') AS in_month
        ,SUM(p1.change_num) AS in_num
FROM jolly_wms.who_wms_goods_stock_detail_log p1
WHERE p1.change_type IN (1, 2, 3, 4, 9, 11, 14, 15, 16, 18)
GROUP BY FROM_UNIXTIME(p1.change_time, 'yyyy-MM')
ORDER BY in_month;

-- ɳ??ָ?sku??????ɿ?
WITH t1 AS 
(SELECT p1.goods_id
        ,p1.sku_id
        ,p2.supp_name
        ,--p1.total_stock_num
        ,(p1.total_stock_num - p1.total_order_lock_num - p1.total_allocate_lock_num - p1.total_return_lock_num) AS free_stock_num
FROM jolly_wms.who_wms_goods_stock_total_detail p1
LEFT JOIN zydb.dim_jc_goods p2
     ON p1.goods_id = p2.goods_id
)
SELECT count(*)
FROM t1;


-- ÿ??ɹ????
SELECT FROM_UNIXTIME(change_time, 'yyyy-MM-dd') AS change_date
        ,SUM(change_num) AS change_num
FROM jolly_wms.who_wms_goods_stock_detail_log
WHERE change_time >= UNIX_TIMESTAMP('2017-08-01', 'yyyy-MM-dd')
    AND change_time < UNIX_TIMESTAMP('2017-09-01', 'yyyy-MM-dd')
    AND depot_id = 7
    AND change_type = 1
GROUP BY FROM_UNIXTIME(change_time, 'yyyy-MM-dd')
ORDER BY change_date;


# change_type ?????1:?ɹ????2:?????????3:????????4:?ӯ??? 5:??????????6:???????7:??λת?,8:???9:??????10: ??⵽??ѷ,11:fba?Ʒ???12:?????,13:????????14:???쳣???15:???????16-??????????17-???????????

WITH 
t1 AS
(SELECT (CASE WHEN p1.change_type = 1 THEN '?ɹ????
                            WHEN p1.change_type = 2 THEN '?????????
                            WHEN p1.change_type = 3 THEN '????????
                            WHEN p1.change_type = 4 THEN '?ӯ???
                            WHEN p1.change_type = 9 THEN '??????
                            WHEN p1.change_type = 11 THEN 'FBA?Ʒ???
                            WHEN p1.change_type = 14 THEN '???쳣???
                            WHEN p1.change_type = 15 THEN '???????
                            WHEN p1.change_type = 16 THEN '??????????
                            WHEN p1.change_type = 18 THEN '???????????
                            WHEN p1.change_type = 5 THEN '??????????
                            WHEN p1.change_type = 6 THEN '???????
                            WHEN p1.change_type = 12 THEN '?????'
                            WHEN p1.change_type = 13 THEN '????????
                            WHEN p1.change_type = 17 THEN '???????????
                            ELSE '??' END) AS change_type2
        ,(CASE WHEN p1.change_type IN (1, 2, 3, 4, 9, 11, 14, 15, 16, 18) THEN 'IN'
                      WHEN p1.change_type IN (5, 6, 12, 13, 17) THEN 'OUT'
                      ELSE 'Others' END) AS change_type1
        ,p2.cat_level1_name
        ,FROM_UNIXTIME(p1.change_time, 'yyyy-MM') AS change_month
        ,FROM_UNIXTIME(p1.change_time, 'yyyy-MM-dd') AS change_date
        ,SUM(p1.change_num) AS change_num
FROM jolly_wms.who_wms_goods_stock_detail_log p1
LEFT JOIN zydb.dim_jc_goods p2 
             ON p1.goods_id = p2.goods_id
WHERE p1.depot_id = 7
     AND p1.change_time >= UNIX_TIMESTAMP('2017-07-01')
     AND p1.change_time < UNIX_TIMESTAMP('2017-10-01')
GROUP BY (CASE WHEN p1.change_type = 1 THEN '?ɹ????
                            WHEN p1.change_type = 2 THEN '?????????
                            WHEN p1.change_type = 3 THEN '????????
                            WHEN p1.change_type = 4 THEN '?ӯ???
                            WHEN p1.change_type = 9 THEN '??????
                            WHEN p1.change_type = 11 THEN 'FBA?Ʒ???
                            WHEN p1.change_type = 14 THEN '???쳣???
                            WHEN p1.change_type = 15 THEN '???????
                            WHEN p1.change_type = 16 THEN '??????????
                            WHEN p1.change_type = 18 THEN '???????????
                            WHEN p1.change_type = 5 THEN '??????????
                            WHEN p1.change_type = 6 THEN '???????
                            WHEN p1.change_type = 12 THEN '?????'
                            WHEN p1.change_type = 13 THEN '????????
                            WHEN p1.change_type = 17 THEN '???????????
                            ELSE '??' END)
        ,(CASE WHEN p1.change_type IN (1, 2, 3, 4, 9, 11, 14, 15, 16, 18) THEN 'IN'
                      WHEN p1.change_type IN (5, 6, 12, 13, 17) THEN 'OUT'
                      ELSE 'Others' END)
        ,p2.cat_level1_name
        ,FROM_UNIXTIME(p1.change_time, 'yyyy-MM')
        ,FROM_UNIXTIME(p1.change_time, 'yyyy-MM-dd')
)
-- ÿ?ÿ??????
SELECT change_month
        ,change_date
        ,SUM(change_num) AS change_num
FROM t1
WHERE change_type1 = 'OUT'
GROUP BY change_month
        ,change_date
ORDER BY change_month
        ,change_date;
-- ??һ??????ĳ?????????
SELECT *
FROM t1
ORDER BY cat_level1_name
        ,change_month
        ,change_type1
        ,change_type2
;


# ????ձ??zydb.ods_who_wms_goods_stock_detail
# ÿ?һ????գ???????

SELECT * 
FROM zydb.ods_who_wms_goods_stock_detail
LIMIT 10;

-- ???????????????Ŀ?ռ??
WITH 
-- ÿ???Ʒ?Ŀ?
t1 AS
(SELECT p1.data_date
        ,p1.goods_id
        ,p2.cat_level1_name AS cat1_name
        ,p2.cat_level2_name AS cat2_name
        ,CONCAT_WS(' - ', p2.cat_level1_name, p2.cat_level2_name) AS cat12_name
        ,stock_num
FROM zydb.ods_who_wms_goods_stock_detail p1
LEFT JOIN zydb.dim_jc_goods p2 ON p1.goods_id = p2.goods_id
WHERE p1.data_date >= '20170701'
     AND p1.data_date <= '20170831'
)
SELECT t1.data_date
        ,t1.cat1_name
        ,SUM(stock_num) AS stock_num
FROM t1
GROUP BY t1.data_date
        ,t1.cat1_name
ORDER BY t1.data_date
        ,t1.cat1_name;

/*
-- ?ɹ??????;????cn + sa??
-- jolly.who_wms_goods_stock_onway_total
-- ?????ydb.ods_wms_goods_stock_onway_total????????ata_date = 20170820
 */
SELECT depot_id
        ,SUM(allocate_order_onway_num + pur_shiped_order_onway_num + pur_order_onway_num) AS pur_allocate_onway_num
FROM jolly.who_wms_goods_stock_onway_total
WHERE depot_id in (4, 5, 6)
GROUP BY depot_id;



----------------- ???ֿ???? ??????????ʷ?͵?ǰֵȡ??
*******************************************************************
????֣????????   (hadoop) zydb.ods_who_wms_goods_stock_detail       stock_num          (??Բ飺??ǰ?????ʷÿ????


????֣? ???????  (hadoop) zydb.ods_who_wms_goods_stock_total_detail ( data FROM 201704)  (??Բ飺??ǰ?????ʷÿ???? 
--ZYDB.ODS_WHO_WMS_GOODS_STOCK_TOTAL_DETAIL???0170813 - 20170822?Ŀ???ݿ?ܲ?̫׼ȷ 
                                        free_num = total_stock_num-total_order_lock_num-total_allocate_lock_num-total_return_lock_num
          
                                ?? jolly.who_wms_goods_stock_total_detail             (??Բ飺??ǰ??)

*******************************
ɳ??֣?????: (hadoop)   jolly_wms.who_wms_goods_stock_detail    stock_num     (??Բ飺??ǰ??)  --????ɳ???ʷĳ????ܿ?   

ɳ??֣?????? (hadoop)   jolly_wms.who_wms_goods_stock_total_detail            (??Բ飺??ǰ??)          
                                       free_num= total_stock_num-total_order_lock_num-total_allocate_lock_num-total_return_lock_num   
            
                             jolly_wms.who_wms_goods_stock_total_detail_daily      (??Բ飺??ǰ?????ʷÿ???? 
                     free_num= total_stock_num-total_order_lock_num-total_allocate_lock_num-total_return_lock_num



-- ??
--ÿ??????ʱ??????ͳ??
select t.depot_id,count(distinct t.goods_id), sum(t.free_num)
FROM 
(--  CN??
select depot_id,goods_id,sku_id,
sum(total_stock_num) total_stock_num,
sum(total_stock_num-total_order_lock_num-total_allocate_lock_num-total_return_lock_num) free_num 
FROM  jolly.who_wms_goods_stock_total_detail s  -- ??ʷ????  201704??ʼ -zydb.ods_who_wms_goods_stock_total_detail 
where 1=1
and depot_id in (4,5,6)
and total_stock_num-total_order_lock_num-total_allocate_lock_num-total_return_lock_num>0
group by depot_id, goods_id,sku_id
) t
group by t.depot_id
union all
select t.depot_id,count(distinct t.goods_id), sum(t.free_num)
FROM 
(--  SA??
select depot_id,goods_id,sku_id,
sum(total_stock_num) total_stock_num,
sum(total_stock_num-total_order_lock_num-total_allocate_lock_num-total_return_lock_num) free_num 
FROM  jolly_wms.who_wms_goods_stock_total_detail s --??ʷ???? ??jolly_wms.who_wms_goods_stock_total_detail_daily 
where 1=1
and depot_id =7 
and total_stock_num-total_order_lock_num-total_allocate_lock_num-total_return_lock_num>0
group by depot_id, goods_id,sku_id
) t
group by t.depot_id

----???ֿ???????
select to_date(FROM_UNIXTIME(change_time)) data_date,
sum(case when  change_type=1 then change_num end) ????????,
sum(case when  change_type=3 then change_num end) ??????? ,
sum(case when  change_type=5 then change_num end) ??????? 
FROM jolly_wms.who_wms_goods_stock_detail_log  a
where 1=1 
and FROM_UNIXTIME(change_time,'yyyyMMdd')>='20170701'
and FROM_UNIXTIME(change_time,'yyyyMMdd')< '20170717'
group by to_date(FROM_UNIXTIME(change_time)) 


-- ?ֿ??????
SELECT SUBSTR(p1.data_date, 1, 6) AS month
        ,p1.data_date
        ,SUM(p1.total_stock_num) AS stock_num
FROM zydb.ods_who_wms_goods_stock_total_detail p1
WHERE p1.data_date >= '20170701'
     AND p1.data_date <= '20170930'
     AND p1.depot_id = 6
GROUP BY SUBSTR(p1.data_date, 1, 6)
        ,p1.data_date
;

-- SA???????????ͳɱ???
SELECT ds
        ,SUM(p1.total_stock_num) AS total_stock_num
        ,SUM(p1.total_stock_num * p2.in_price) AS totck_stock_cost
FROM jolly_wms.who_wms_goods_stock_total_detail_daily p1
LEFT JOIN zydb.dim_jc_goods p2 ON p1.goods_id = p2.goods_id
WHERE ds >= '20170501'
     AND depot_id = 7
GROUP BY ds
ORDER BY ds;





-- =============================================== ???????ݣ??ף? ===================================================


















/*
-- ?????
-- cn??jolly.who_wms_goods_stock_total_detail
-- sa??jolly_wms.who_wms_goods_stock_total_detail
 */
-- ???
WITH 
-- cn??ڿ???
t1 AS
(SELECT depot_id
        ,SUM(total_stock_num) AS total_stock_num
FROM jolly.who_wms_goods_stock_total_detail
WHERE depot_id in (4, 5, 6)
GROUP BY depot_id
),
-- sa??
t2 AS
(SELECT depot_id
        ,SUM(total_stock_num) AS total_stock_num
FROM jolly_wms.who_wms_goods_stock_total_detail
GROUP BY depot_id
)
-- union
SELECT t1.* FROM t1
uniON 
SELECT t2.* FROM t2;

-- ????
WITH 
-- cn??ڿ???
t1 AS
(SELECT depot_id
        ,SUM(total_stock_num - total_order_lock_num - total_allocate_lock_num - total_return_lock_num) AS total_free_stock_num
FROM jolly.who_wms_goods_stock_total_detail
WHERE depot_id in (4, 5, 6)
GROUP BY depot_id
),
-- sa??
t2 AS
(SELECT depot_id
        ,SUM(total_stock_num) AS total_stock_num
FROM jolly_wms.who_wms_goods_stock_total_detail
GROUP BY depot_id
)
-- union
SELECT t1.* FROM t1
uniON 
SELECT t2.* FROM t2;


-- ???
-- ????㷽ʽ??δ???

SELECT return_status
        ,COUNT(*)
        ,SUM(returned_goods_num) AS returned_num
FROM jolly.who_wms_returned_order_info
GROUP BY return_status
ORDER BY return_status;

-- ????;??㷽ʽһ
SELECT p2.returned_depot_id
    ,COUNT(*)
    ,SUM(p1.returned_num)
FROM jolly.who_wms_returned_order_goods p1
LEFT JOIN jolly.who_wms_returned_order_info p2 ON p1.returned_rec_id = p2.returned_rec_id
WHERE p1.stock_END_time = UNIX_TIMESTAMP('1970-01-01 08:00:00')
GROUP BY p2.returned_depot_id;
-- ?????󣬲?̫???
/*
4   5946345 6447107
7   1040863 1100934
    10  13
 */

-- ??㷽ʽ??
SELECT p2.returned_depot_id
        ,SUM(returned_num)
FROM jolly.who_wms_returned_order_goods p1
LEFT JOIN jolly.who_wms_returned_order_info p2 ON p1.returned_rec_id = p2.returned_rec_id
WHERE p1.is_stock = 0
GROUP BY p2.returned_depot_id
ORDER BY p2.returned_depot_id;
-- ????????????
/*
4   6728369.0
7   2337678.0
    13.0
 */

-- ??㷽ʽ?
SELECT returned_depot_id
        ,SUM(returned_goods_num) AS return_onway_num
FROM jolly.who_wms_returned_order_info
WHERE return_status = 1 or return_status = 3
GROUP BY returned_depot_id
ORDER BY returned_depot_id;
-- û?ô????
/*
4   998236.0
7   908689.0
 */

-- ??㷽ʽ?
WITH 
-- ????;??˻???
t1 AS
(SELECT returned_rec_id
        ,returned_depot_id
FROM jolly.who_wms_returned_order_info
WHERE return_status = 1 or return_status = 3
GROUP BY returned_rec_id
        ,returned_depot_id
)
SELECT t1.returned_depot_id
        ,SUM(p1.returned_num) AS returned_onway_num
FROM t1
LEFT JOIN jolly.who_wms_returned_order_goods p1 ON t1.returned_rec_id = p1.returned_rec_id
GROUP BY t1.returned_depot_id
ORDER BY t1.returned_depot_id;
/*
7   920999
4   1037130
 */









-- ????Ʒcn???ɿ?
WITH 
t1 AS
(SELECT p1.sku_id
        ,p1.depot_id
        ,p1.depot_area_id
        ,p1.shelf_id
        ,p1.shelf_area_id
        ,CONCAT_WS('-', cast(p1.depot_area_id AS string), 
                                        cast(p1.shelf_id AS string), 
                                        cast(p1.shelf_area_id AS string)) AS depot_shelf_id
        ,p1.stock_num
        ,(p1.stock_num - p1.order_lock_num - p1.lock_allocate_num - p1.return_lock_num) AS free_num
FROM jolly.who_wms_goods_stock_detail p1
WHERE p1.depot_id in (4, 5, 6)
    AND p1.sku_id in (118210,309684,344302,1587192,1657288,1802762,1827808,1828072,1853206,1943172,2054688,2081684,2141352,2164894,2240496,2277088,2277684,2320616,2388020,2449794,2485078,2485082,2486374,2501668,2567744,2568776,2578082,2580830,2612404,2653362,2699302,2699322,2742776,2769348,2783792,2784378,2834970,2881580,2892288,2942610,2944058,2944352,2967524,3011176,3034698,3054562,3055138,3100974,3111650,3214758,3227548,799993,800149,807153,857577,1044995,3248202,3302704,1159535,1176017,1201427,1201429,3382184,3598636,3841414,5290654,1813116,1813118,1813120,1813122,1813124,1815044,1858182,2285162,3051878,3124914,1615482,2429186,2431114,1614226,347804,309652)
),
-- ????õ?shelf_area_sn
t2 AS
(SELECT t1.* 
        ,p1.shelf_area_sn
FROM t1
LEFT JOIN jolly.who_wms_picking_goods_detail p1 ON t1.shelf_area_id = p1.shelf_area_id
)
-- ????
SELECT sku_id
        ,depot_id
        ,depot_shelf_id
        ,shelf_area_sn
        ,SUM(stock_num) AS stock_num
        ,SUM(free_num) AS free_num
FROM t2
GROUP BY sku_id
        ,depot_id
        ,depot_shelf_id
        ,shelf_area_sn;


-- jolly.who_wms_goods_stock_total_detail
-- jolly_oms.who_wms_goods_stock_total_detail
-- zydb.ods_who_wms_goods_stock_total_detail
WITH 
-- wms
t1 AS
(SELECT depot_id
        ,sku_id
        ,total_stock_num
        ,total_order_lock_num
        ,total_allocate_lock_num
        ,total_return_lock_num
        ,'wms' AS source
FROM jolly.who_wms_goods_stock_total_detail
WHERE sku_id = 6173662
),
-- zydb
t2 AS
(SELECT depot_id
        ,sku_id
        ,total_stock_num
        ,total_order_lock_num
        ,total_allocate_lock_num
        ,total_return_lock_num
        ,'zydb' AS source
FROM zydb.ods_who_wms_goods_stock_total_detail
WHERE sku_id = 6173662
  AND data_date = '20170822'
)
SELECT * FROM t1
uniON all
SELECT * FROM t2
;





-- ???sku???????λ?ŵĿ?
WITH 
-- ???ϸ
t1 AS
(SELECT e.depot_id
        ,e.sku_id
        ,concat(d.depot_sn,'-',c.depot_area_sn,b.shelf_sn,'-',a.shelf_area_sn) AS shelf_area_sn
        ,(e.stock_num - e.order_lock_num - e.lock_allocate_num - e.return_lock_num) AS free_num
FROM jolly.who_wms_depot_shelf_area a
        ,jolly.who_wms_depot_shelf b
        ,jolly.who_wms_depot_area c
        ,jolly.who_wms_depot d
        ,jolly.who_wms_goods_stock_detail e
WHERE a.depot_shelf_id = b.shelf_id
     AND b.depot_area_id = c.depot_area_id
     AND c.depot_id = d.depot_id 
     AND a.shelf_area_id = e.shelf_area_id 
     AND e.stock_num >0 
     AND e.depot_id IN (4, 5, 6)
),
--  sku???λ?ŵ????????????ku?????λ?
t2 AS
(SELECT sku_id
        ,COUNT(shelf_area_sn) AS shelf_num
FROM t1
GROUP BY sku_id
)
-- ??????????ku?????λ????
SELECT COUNT (sku_id)
FROM t2
WHERE shelf_num >=2;
-- ?8000??sku
SELECT * 
FROM t1
LIMIT 10;


/*
?????????????????????????????????????״̬??
?Ҫ??ݱ??
-- ??????????? jolly.who_order_shipping_tracking
-- ??????????Ĺ?ʱ?? jolly.who_prs_cod_order_shipping_time
 */

-- ??????????? jolly.who_order_shipping_tracking
SELECT * 
FROM jolly.who_order_shipping_tracking
LIMIT 10;
-- invoice_no: ?????
-- status: ??״̬: polling:?????shutdown:?????abort:?ֹ??updateall????????????????Ϊ?ǩ?ʱstatus=shutdown????messageΪ??3????????????0???仯ʱ??status= abort
SELECT status
        ,COUNT(order_id)
FROM jolly.who_order_shipping_tracking
GROUP BY status;
/*
1   polling 344657
2   shutdown    3189839
3   abort   645319
4   issEND  2350
5   updateall   2720
 */
-- shipping_state 0?;???1??????2????3?ǩ???4?ǩ??5ͬ????С?6??أ??ͻ???أ?δǩ?????˻أ?Ͷ?????˻صȣ???7ת????8???ա?13??????????״̬
3??ǩ?
6/8?????˻?
!=3/6/8/13?????
-- ????ʵ?ʲ???????15??ֵ??9-13??110?ĺ???ֱ???ô??
-- ??ȡֵ????¹???δ??????ڿ???????
SELECT shipping_state
        ,COUNT(order_id)
FROM jolly.who_order_shipping_tracking
GROUP BY shipping_state
ORDER BY shipping_state;
/*
1   0   527893
2   1   5617
3   2   28255
4   3   3189800
5   4   68
6   5   3344
7   6   406922
8   7   2720
9   8   1368
10  9   5773
11  10  2754
12  11  7694
13  12  70
14  13  257
15  110 2350
 */
-- result_type: ????ࣺ1???????? 2????????3?????? 4?????????5????
SELECT result_type
        ,COUNT(order_id)
FROM jolly.who_order_shipping_tracking
GROUP BY result_type
ORDER BY result_type;
/*
1   0   2088228
2   1   38
3   2   102
4   3   462
5   4   1316
6   5   2094739
 */
-- tracking_code
-- result_code

-- ??????????Ĺ?ʱ?? jolly.who_prs_cod_order_shipping_time
-- ע???destination_time??????ĵ?????Ĺ?ʱ??
SELECT *
FROM jolly.who_prs_cod_order_shipping_time
LIMIT 10;







-- ????
SELECT real_shipping_id
        ,real_shipping_name
FROM jolly.who_order_info
WHERE real_shipping_id IN (40, 168, 170, 172, 174, 176)
GROUP BY real_shipping_id
        ,real_shipping_name
ORDER BY real_shipping_id;


-- ?ɹ???????Ϣ??
-- jolly.who_wms_pur_order_tracking_info
SELECT *
FROM jolly.who_wms_pur_order_tracking_info
LIMIT 10;
-- ????????????ʽtracking_pay_typeȡֵ???
-- 0/1/2/3/4







/*
1.1?????????԰?????Ŷ???????
2.1????????????ܷ????????????????
3.????????jolly.who_wms_picking_info.picking_id = jolly.who_wms_picking_goods_detail.picking_id
4.picking_finish_time??jolly.who_wms_picking_info.finish_time
5.??һ??????????????ż???????ʱ??????????Ǹ?ʱ????ö????????ʱ??
*/

/*
?Ȥ???⣺
1.һ??????һ???ɶ?ٸ???????
2.һ??????һ??????ٸ???????
3.һ??????һ????ټ??Ʒ??
4.һ??????һ?㻨?೤ʱ?????
5.һ??????һ?㻨?೤ʱ???????
*/

-- ?????Ϣ??
SELECT * FROM jolly.who_wms_picking_info limit 10;

-- ???Ʒ?ϸ??
SELECT * FROM jolly.who_wms_picking_goods_detail limit 10;

-- ???ĳ??????ļ??Ϣ
SELECT a.order_sn, a.picking_id, b.finish_time
FROM jolly.who_wms_picking_goods_detail a
LEFT JOIN jolly.who_wms_picking_info b ON a.picking_id = b.picking_id
WHERE a.order_sn = 'arsi201612190456301526';

-- ???ĳ??????ּ??ηּ?
SELECT COUNT(DISTINCT picking_id) AS pick_num
FROM jolly.who_wms_picking_goods_detail
WHERE order_sn = 'arsi201612190456301526';

-- ??????ֿ????????
SELECT depot_id, COUNT(picking_id) AS pick_num
FROM jolly.who_wms_picking_info
GROUP BY depot_id;

-- ???order_COUNT?ļ??????
SELECT order_COUNT, COUNT(picking_id) AS pick_num
FROM jolly.who_wms_picking_info
GROUP BY order_COUNT
ORDER BY pick_num desc;




-- ?????Ϣ??ho_order_info
-- prepare_pay_time????????ô??????庬??ʲô??
SELECT
(CASE WHEN pay_id = 41 THEN 'cod' ELSE 'ncod' END) AS pay_id2,
(CASE WHEN prepare_pay_time = 0 THEN 0 ELSE 1 END) AS prepare_pay_time2,
COUNT(order_sn) AS order_num
FROM jolly.who_order_info
GROUP BY
(CASE WHEN pay_id = 41 THEN 'cod' ELSE 'ncod' END),
(CASE WHEN prepare_pay_time = 0 THEN 0 ELSE 1 END);




SELECT substr(order_sn, 0, 3) AS order_sn
        ,COUNT(order_sn) 
FROM zydb.dw_order_sub_order_fact
GROUP BY substr(order_sn, 0, 3)
ORDER BY COUNT(order_sn) desc;





-- ????ɹ?????????????
WITH t as
(SELECT pur_order_sn
    ,COUNT(DISTINCT tracking_no) AS tracking_no
FROM zydb.ods_wms_pur_deliver_receipt 
GROUP BY pur_order_sn
)
SELECT tracking_no
    ,COUNT(pur_order_sn) AS order_no
FROM t
GROUP BY tracking_no
ORDER BY tracking_no;

-- ????????????Ĳɹ????
WITH t as
(SELECT tracking_no
    ,COUNT(DISTINCT pur_order_sn) AS order_no
FROM zydb.ods_wms_pur_deliver_receipt 
GROUP BY tracking_no
)
SELECT order_no
    ,COUNT(tracking_no) AS tracking_no
FROM t
GROUP BY order_no
ORDER BY order_no;



-- ɳ?????0170719
-- ÿ????????????еĶ????????
WITH
-- ɳ?????????Ķ????ϸ
t1 as
(SELECT FROM_UNIXTIME(shipping_time, 'yyyy-MM-dd') AS ship_date
        ,order_id
FROM jolly.who_order_info
WHERE is_shiped = 1
    AND depot_id = 7
    AND shipping_time >= UNIX_TIMESTAMP('2017-07-15', 'yyyy-MM-dd')
    AND shipping_time <= UNIX_TIMESTAMP('2017-07-19', 'yyyy-MM-dd')
),
-- ???ÿ????е????
t2 as
(SELECT region_id
        ,region_name
FROM jolly.who_region 
WHERE region_type = 2
    AND region_status = 1
),
-- ???jolly.who_order_user_info??2???õ?????????ĳ?????
t3 AS 
(SELECT t1.*
        ,t2.region_name AS city_name
FROM t1
LEFT JOIN jolly.who_order_user_info p1 ON t1.order_id = p1.order_id
LEFT JOIN t2 ON p1.city = t2.region_id
),
-- ????
t as
(SELECT ship_date
        ,city_name
        ,COUNT(*) AS order_num
FROM t3
GROUP BY ship_date
        ,city_name
)
-- ??????????
SELECT * 
FROM t
ORDER BY ship_date desc
        ,order_num desc;


-- ???ĳ?????ҵ????????????ʽ)
WITH 
-- ??????ʡ??
t1 as
(SELECT region_id
FROM jolly.who_region
WHERE parent_id = 1876
    AND region_type = 1
)
-- ????????
SELECT region_id
        ,region_name
FROM jolly.who_region
WHERE parent_id in (SELECT * FROM t1)
    AND region_type = 2



SELECT p1.order_id
        ,p1.pay_id
        ,p1.cod_check_status
        ,p1.shipping_status
        ,p2.shipping_state
FROM jolly.who_order_info p1
LEFT JOIN jolly.who_order_shipping_tracking p2 
            ON p1.order_id = p2.order_id
WHERE p1.order_status = 1
    AND p1.pay_status = 1
    AND p1.is_shiped = 1
limit 10;

-- ɳ?????ÿ??Сʱ????????
SELECT FROM_UNIXTIME(shipping_time, 'yyyy-MM-dd') AS ship_date
        ,FROM_UNIXTIME(shipping_time, 'HH') AS ship_hour
        ,COUNT(order_id) AS order_num
FROM jolly.who_order_info
WHERE is_shiped = 1
    AND depot_id = 7
    AND shipping_time >= UNIX_TIMESTAMP('2017-06-01', 'yyyy-MM-dd')
    AND shipping_time <= UNIX_TIMESTAMP('2017-08-28', 'yyyy-MM-dd')
GROUP BY FROM_UNIXTIME(shipping_time, 'yyyy-MM-dd')
        ,FROM_UNIXTIME(shipping_time, 'HH')
ORDER BY ship_date
        ,ship_hour;


-- ɳ?????ÿ??Сʱ?ͻ?????
WITH t AS
(SELECT *
        ,(CASE WHEN prepare_pay_time = 0 THEN pay_time ELSE prepare_pay_time END) AS real_pay_time
FROM jolly.who_order_info
WHERE depot_id = 7
     AND order_status = 1
     AND pay_status IN (1, 3)
),
t2 AS
(SELECT FROM_UNIXTIME(real_pay_time, 'yyyy-MM-dd') AS date_BeiJing
        ,FROM_UNIXTIME(real_pay_time, 'HH') AS hour_BeiJing
        ,COUNT(order_id) AS order_num
FROM t
WHERE real_pay_time >= UNIX_TIMESTAMP('2017-06-01', 'yyyy-MM-dd')
    AND real_pay_time <= UNIX_TIMESTAMP('2017-08-29', 'yyyy-MM-dd')
GROUP BY FROM_UNIXTIME(real_pay_time, 'yyyy-MM-dd')
        ,FROM_UNIXTIME(real_pay_time, 'HH')
ORDER BY date_BeiJing
        ,hour_BeiJing
),

-- ɳ?????ÿ??Сʱ?????ɶ???????ɼ???
t3 AS
(SELECT FROM_UNIXTIME(gmt_created, 'yyyy-MM-dd') AS date_BeiJing
        ,FROM_UNIXTIME(gmt_created, 'HH') AS hour_BeiJing
        ,COUNT(DISTINCT order_id) AS order_num
FROM jolly_wms.who_wms_outing_stock_detail
WHERE depot_id = 7
    AND gmt_created >= UNIX_TIMESTAMP('2017-06-01', 'yyyy-MM-dd')
    AND gmt_created <= UNIX_TIMESTAMP('2017-08-29', 'yyyy-MM-dd')
GROUP BY FROM_UNIXTIME(gmt_created, 'yyyy-MM-dd')
        ,FROM_UNIXTIME(gmt_created, 'HH') 
ORDER BY date_BeiJing
        ,hour_BeiJing
)
SELECT t2.date_BeiJing
        ,t2.hour_BeiJing
        ,t2.order_num AS pay_order_num
        ,t3.order_num AS can_pick_order_num
FROM t2
JOIN t3 
ON t2.date_BeiJing = t3.date_BeiJing AND t2.hour_BeiJing = t3.hour_BeiJing
ORDER BY t2.date_BeiJing
        ,t2.hour_BeiJing;




-- ÿ??????????ɼ?????????????????

-- ɳ??????ͻ?????????
WITH t1 AS
(SELECT *
        ,(CASE WHEN prepare_pay_time = 0 THEN pay_time ELSE prepare_pay_time END) AS real_pay_time
FROM jolly.who_order_info
WHERE depot_id = 7
     AND order_status = 1
     AND pay_status IN (1, 3)
),
t2 AS
(SELECT FROM_UNIXTIME(real_pay_time, 'yyyy-MM-dd') AS date_BeiJing
        ,COUNT(order_id) AS order_num
FROM t1
WHERE real_pay_time >= UNIX_TIMESTAMP('2017-08-01', 'yyyy-MM-dd')
    AND real_pay_time <= UNIX_TIMESTAMP('2017-09-04', 'yyyy-MM-dd')
GROUP BY FROM_UNIXTIME(real_pay_time, 'yyyy-MM-dd')
ORDER BY date_BeiJing
),

-- ɳ??????????ɶ???????ɼ???
t3 AS
(SELECT FROM_UNIXTIME(gmt_created, 'yyyy-MM-dd') AS date_BeiJing
        ,COUNT(DISTINCT order_id) AS order_num
FROM jolly_wms.who_wms_outing_stock_detail
WHERE depot_id = 7
    AND gmt_created >= UNIX_TIMESTAMP('2017-08-01', 'yyyy-MM-dd')
    AND gmt_created <= UNIX_TIMESTAMP('2017-09-04', 'yyyy-MM-dd')
GROUP BY FROM_UNIXTIME(gmt_created, 'yyyy-MM-dd')
ORDER BY date_BeiJing
)
SELECT t2.date_BeiJing
        ,t2.order_num AS pay_order_num
        ,t3.order_num AS can_pick_order_num
FROM t2
LEFT JOIN t3 
            ON t2.date_BeiJing = t3.date_BeiJing
ORDER BY t2.date_BeiJing;




-- =====================================================================================
-- ??ܿ?ʼ?ͽ??ʱ??
WITH t AS 
(SELECT  b.depot_id
        ,b.pur_order_sn
        ,b.order_id
        ,b.goods_id
        ,b.sku_id
        ,finish_check_time
        ,on_shelf_finish_time
        ,on_shelf_num
        ,shipping_time
        ,(UNIX_TIMESTAMP(on_shelf_finish_time) - UNIX_TIMESTAMP(finish_check_time)) AS onshelf_duration
FROM zydb.dw_order_sub_order_fact a
LEFT JOIN zydb.dw_demAND_pur b
ON a.order_id=b.order_id
LEFT JOIN zydb.dw_delivered_onself_info c
ON b.pur_order_sn=c.delivered_order_sn
AND b.sku_id=c.sku_id
WHERE shipping_time>='2017-09-01'
AND shipping_time<'2017-09-25'
AND on_shelf_finish_time IS NOT NULL
AND finish_check_time IS NOT NULL
)

-- ÿ???ܽ??ʱ??????ʱ????Ʒ?
SELECT to_date(shipping_time) AS ship_date
        ,SUM(CASE WHEN onshelf_duration < 0 THEN 1 ELSE 0 END) AS less_num
        ,COUNT(1) AS total_num
FROM t
GROUP BY to_date(shipping_time)
ORDER BY ship_date;

-- ???????????ķֲ?
SELECT floor(onshelf_duration / 3600) AS on_shelf_hour
        ,COUNT(*) AS num
FROM t 
WHERE onshelf_duration <= 0
GROUP BY floor(onshelf_duration / 3600)
ORDER BY on_shelf_hour;




--- 7???????ҵʱ??
select depot_id
    ,avg(receipt_quality_duration + quality_onshelf_duration + picking_duration + package_duration + shipping_duration) as LT2
FROM zydb.rpt_depot_daily_report
where depot_id in (4, 5)
 AND data_date >= '2017-07-01'
 AND data_date <= '2017-07-31'
group by depot_id;


-- HK?ֳ????ϸ
SELECT order_sn
        ,depot_id
        ,is_shiped
        ,is_problems_order AS ???쳣????
        ,pay_time AS ???????
        ,no_problems_order_uptime AS ??ʱ??
        ,lock_last_modified_time AS ?????????
        ,outing_stock_time AS ?ɼ?ʱ??
        ,picking_finish_time AS picking_finish_time
        ,order_pack_time AS ????????
        ,shipping_time AS ???ʱ??
FROM zydb.dw_order_node_time
WHERE depot_id = 6
AND is_shiped = 1
AND is_problems_order IN (0, 2)
AND order_status = 1
AND pay_status IN (1, 3)
AND shipping_time >= '2017-10-01'
AND shipping_time < '2017-10-31'
ORDER BY shipping_time;


-- ?Ԫ???Ա?????Ʒ????

WITH 
-- ?????????ϸ
t1 AS
(SELECT p1.order_id
        ,p1.pay_time
        ,TO_DATE(CAST(p1.pay_time AS STRING)) AS pay_date
        ,SUBSTR(CAST(p1.pay_time AS STRING), 12, 2) AS pay_hour
        ,p2.goods_id
        ,SUM(p2.original_goods_number) AS org_goods_num
        ,SUM(p2.original_goods_number * p2.goods_price) AS org_goods_amount
FROM zydb.dw_order_node_time p1
LEFT JOIN jolly.who_order_goods p2 ON p1.order_id = p2.order_id
WHERE p1.pay_time >= FROM_UNIXTIME(UNIX_TIMESTAMP('2017-09-29'))
     AND p1.pay_time < FROM_UNIXTIME(UNIX_TIMESTAMP('2017-10-01'))
     AND p1.pay_status IN (1, 3)
     -- AND p1.order_status = 1
GROUP BY p1.order_id
        ,p1.pay_time
        ,TO_DATE(CAST(p1.pay_time AS STRING)) 
        ,SUBSTR(CAST(p1.pay_time AS STRING), 12, 2)
        ,p2.goods_id
),
t2 AS
(SELECT goods_id
        ,pay_date
        ,pay_hour
        ,COUNT(order_id ) AS order_num
        ,SUM(org_goods_num) AS org_goods_num
        ,SUM(org_goods_amount) AS org_goods_amount
FROM t1
GROUP BY goods_id
        ,pay_date
        ,pay_hour
)
SELECT * 
FROM t2;






SELECT * 
FROM t2
LIMIT 10;
ORDER BY goods_id
        ,pay_date
        ,pay_hour;






SELECT pay_date
        ,COUNT(DISTINCt goods_id)
        ,SUM(org_goods_num)
        ,SUM(org_goods_amount)
FROM t2
GROUP BY pay_date;


-- ??who_order_info???ho_order_shipping_tracking??Ķ???״̬
SELECT p1.pay_id
        ,p1.cod_check_status
        ,p2.shipping_state
        ,count(p1.order_id)
FROM jolly.who_order_info p1
             ,jolly.who_order_shipping_tracking p2
WHERE p1.order_id = p2.order_id
GROUP BY p1.pay_id
        ,p1.cod_check_status
        ,p2.shipping_state
ORDER BY p1.pay_id
        ,p1.cod_check_status
        ,p2.shipping_state;



-- =============================================== ??????ݣ????? ===================================================
/*
??????ݿ?
-- ??ܵ????jolly.who_wms_on_shelf_info??ֻ?2017-08-13???????ݣ? -- 2017-08-13?ǰ????ݣ??jolly.who_wms_pur_deliver_info????
-- ????????jolly.who_wms_on_shelf_goods
 */

-- ??ܵ????jolly.who_wms_on_shelf_info
-- gmt_created??????ʱ??
-- status????ܵ?״̬??1-?????2-????3-????
-- source_type????Դ??ͣ?1-????????ҵ??,2-?ͨ?ɹ???(?ǹ?Ӧ???????,3-??????ⵥ,4-??Ӧ??????ɹ???
SELECT * 
FROM jolly.who_wms_on_shelf_info
LIMIT 10;

-- ??ϼܵ????jolly.who_wms_pur_deliver_info
-- deliver_sn????ܵ???
-- gmt_created????ܵ?????ʱ??
-- status????ܵ?״̬??1-?????2-????3-????
-- user_id???²??ϼ??ID
-- finish_time??????ʱ??
-- FROM_type????Դ??ͣ?1-????????ҵ??,2-?ͨ?ɹ???(?ǹ?Ӧ???????,3-??????ⵥ,4-??Ӧ??????ɹ???
SELECT * 
FROM jolly.who_wms_pur_deliver_info
LIMIT 10;

SELECT depot_id
        ,FROM_UNIXTIME(gmt_created, 'yyyy-MM-dd') AS ??????
        ,status
        ,(CASE WHEN status = 1 THEN '?????
                      WHEN status = 2 THEN '????
                      WHEN status = 3 THEN '????'
                      ELSE '??' END) AS ????̬
        ,COUNT(on_shelf_sn) AS ??ܵ????
        ,SUM(total_num) AS ????????
FROM jolly.who_wms_on_shelf_info
WHERE gmt_created >= UNIX_TIMESTAMP('2017-10-16')
     AND gmt_created < UNIX_TIMESTAMP('2017-10-17')
GROUP BY depot_id
        ,FROM_UNIXTIME(gmt_created, 'yyyy-MM-dd')
        ,status
        ,(CASE WHEN status = 1 THEN '?????
                      WHEN status = 2 THEN '????
                      WHEN status = 3 THEN '????'
                      ELSE '??' END)
ORDER BY depot_id
        ,FROM_UNIXTIME(gmt_created, 'yyyy-MM-dd')
        ,status;


-- ????????ǰ??ɵģ???ǰ?δ???ϼܵ?ϼܵ?
-- 1.???ϼܵ????ϼ??????
SELECT depot_id
        ,(CASE WHEN status = 1 THEN '?????
                      WHEN status = 2 THEN '????
                      WHEN status = 3 THEN '????'
                      ELSE '??' END) AS ????̬
        ,COUNT(on_shelf_sn) AS ??ܵ????
        ,SUM(total_num) AS ????????
FROM jolly.who_wms_on_shelf_info
WHERE gmt_created <= UNIX_TIMESTAMP(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP('$[&data_date]', 'yyyyMMdd')), 0))
     AND (status = 1 OR status  = 2)    -- 1/2??????ܺ?ϼ??
GROUP BY depot_id
        ,(CASE WHEN status = 1 THEN '?????
                      WHEN status = 2 THEN '????
                      WHEN status = 3 THEN '????'
                      ELSE '??' END)
ORDER BY depot_id;
-- 2.??ܵ?list
SELECT p1.on_shelf_sn
        ,p1.depot_id
        ,FROM_UNIXTIME(p1.gmt_created) AS ????ʱ??
        ,(CASE WHEN p1.status = 1 THEN '?????
                      WHEN p1.status = 2 THEN '????
                      WHEN p1.status = 3 THEN '????'
                      ELSE '??' END) AS ????̬
        ,(CASE WHEN p1.source_type = 1 THEN '????????ҵ??' 
                      WHEN p1.source_type = 2 THEN '?ͨ?ɹ???(?ǹ?Ӧ?????)' 
                      WHEN p1.source_type = 3 THEN '??????ⵥ' 
                      WHEN p1.source_type = 4 THEN '??Ӧ??????ɹ???'
                      ELSE '??' END) AS ??Դ???
        ,p1.on_shelf_admin_id AS ????ID
        ,p2.user_name
        ,p1.total_num
FROM jolly.who_wms_on_shelf_info p1
LEFT JOIN jolly.who_rbac_user p2 ON p2.user_id = p1.on_shelf_admin_id
WHERE p1.gmt_created <= UNIX_TIMESTAMP(DATE_SUB(FROM_UNIXTIME(UNIX_TIMESTAMP('$[&data_date]', 'yyyyMMdd')), 0))    -- 0/1/2/3...:?????ǰ??0???????ɵ?ϼܵ?
     AND (p1.status = 1 OR status  = 2)    -- 1/2??????ܺ?ϼ??
ORDER BY p1.depot_id;


WITH 
-- ?????ÿ????ϼܵ????, ?Ʒ???, ???????????ƽ??????????
t1 AS
(SELECT FROM_UNIXTIME(p1.on_shelf_finish_time, 'yyyy-MM-dd') AS onshelf_finish_date
        ,p1.depot_id
        ,p1.on_shelf_admin_id AS onshelf_staff_id
        ,p2.user_name AS onshelf_staff_name
        ,COUNT(p1.on_shelf_sn) AS onshelf_order_num
        ,SUM(p1.total_num) AS onshelf_goods_num
        ,SUM((p1.on_shelf_finish_time - p1.gmt_created) / 3600) AS onshelf_hours
FROM jolly.who_wms_on_shelf_info p1
LEFT JOIN jolly.who_rbac_user p2 
             ON p1.on_shelf_admin_id = p2.user_id
WHERE p1.status = 3    -- ???ϼ?
     AND p1.on_shelf_finish_time >= UNIX_TIMESTAMP('2017-04-01')
GROUP BY FROM_UNIXTIME(p1.on_shelf_finish_time, 'yyyy-MM-dd')
        ,p1.depot_id
        ,p1.on_shelf_admin_id
        ,p2.user_name
)

SELECT * 
FROM t1
ORDER BY onshelf_finish_date
        ,depot_id
        ,onshelf_staff_id;




-- =============================================== ??????ݣ??ף? ===================================================



-- ???????????????ִ??ĸ???ϵͳ??û??б??
-- jolly.who_rbac_user
SELECT * 
FROM jolly.who_rbac_user
LIMIT 10;



-- ============================================== ???????????ݣ????? ===================================================
/*
-- ˵???????????????
-- ????Neo???
-- ???ʱ?䣺2017-10-19
 */

-- ?????????jolly.who_wms_allocate_order_info
-- ??????е?rrive_time??????ʱ????ʲôʱ?䣿
SELECT * 
FROM jolly.who_wms_allocate_order_info
LIMIT 10;

-- ??????ǩ????
-- jolly.who_wms_pur_deliver_receipt  ????????????/???????һ?Զ?ϵ
-- zydb.ods_wms_pur_deliver_receipt  ????????????/???????һ??????
-- ??е?mt_created?ǩ?ʱ?䣻

-- ??????ʱ??????zydb.dw_allocate_out_node


-- ?????????????ʱ??
WITH t1 AS
(SELECT p1.allocate_order_sn
        ,p1.FROM_depot_id
        ,p1.to_depot_id
        ,FROM_UNIXTIME(p1.gmt_created) AS ??????????ʱ??
        ,FROM_UNIXTIME(p1.off_shelf_time) AS ??????
        ,FROM_UNIXTIME(p1.finish_packing_time) AS ????????
        ,FROM_UNIXTIME(p1.out_time) AS ?????ֳ??????
        ,FROM_UNIXTIME(p1.arrive_time) AS ??????????
        ,FROM_UNIXTIME(p2.gmt_created) AS receive_time
FROM jolly.who_wms_allocate_order_info p1
LEFT JOIN zydb.ods_wms_pur_deliver_receipt p2 
             ON p1.allocate_order_sn = p2.pur_order_sn
WHERE p1.gmt_created >= UNIX_TIMESTAMP('2017-01-01')
--     AND p1.gmt_created < UNIX_TIMESTAMP('2017-10-09')
)
-- ????ϸ
SELECT * 
FROM t1
WHERE receive_time IS NOT NULL
LIMIT 10;
-- ??????ٵ?????û?ǩ?ʱ??
SELECT to_depot_id
        ,COUNT(allocate_order_sn) AS total_num
        ,COUNT(CASE WHEN receive_time IS NULL THEN 1 ELSE NULL END) AS no_receive_time_num
FROM t1
GROUP BY to_depot_id
ORDER BY to_depot_id;



-- ============================================== ???????????ݣ??ף? ===================================================








-- zydb.dw_goods_on_sale
-- ???Ʒÿ???ۼ? prst_price








-- zydb.ods_who_wms_goods_stock_total_detail & zydb.ods_who_wms_goods_stock_detail

SELECT data_date
        ,count(1)
FROM zydb.ods_who_wms_goods_stock_total_detail
GROUP BY data_date
ORDER BY data_date
;


SELECT *
FROM jolly_tms_center.tms_order_info
WHERE shipped_time >0
LIMIT 10
;


with t1 as
(SELECT p1.customer_order_id
        ,count(1) AS num
FROM jolly_tms_center.tms_order_info p1
GROUP BY p1.customer_order_id
)
select customer_order_id
        ,num
FROM t1
ORDER BY num desc
LIMIT 10;




SELECT tms_order_id
        ,customer_order_id
        ,tracking_no
        ,shipping_status
        ,depot_id
        ,FROM_UNIXTIME(shipped_time) AS shipping_time
        ,FROM_UNIXTIME(gmt_modified) AS gmt_modified
FROM jolly_tms_center.tms_order_info
WHERE customer_order_id = 25181391
;


SELECT * 
FROM jolly.who_order_info
where order_id  = 25892073
;




-- ????????hipping_status vs cod_check_status
WITH t1 AS
(SELECT p1.customer_order_id
        ,p2.order_sn
        ,p1.shipping_status
        ,p2.cod_check_status        
FROM jolly_tms_center.tms_order_info p1
LEFT JOIN jolly.who_order_info p2
on p1.customer_order_id = p2.order_id
WHERE p1.shipped_time > 0
)
-- ??״̬????Ķ????
select shipping_status
        ,cod_check_status
        ,count(customer_order_id) AS order_num
FROM t1
GROUP BY shipping_status
        ,cod_check_status;
-- ?˲????״̬????Ķ???
SELECT * 
FROM t1
WHERE cod_check_status = 0
     AND shipping_status = 3
LIMIT 5;


-- tms_order_info
SELECT * 
FROM jolly_tms_center.tms_order_info p1
LIMIT 10;


SELECT *
        ,FROM_UNIXTIME(gmt_created)
FROM jolly.who_wms_on_shelf_goods_price
where delivered_order_sn = 'GZ2DB171022092806186Y94IF'
limit 10;


SELECT * 
FROM jolly.who_wms_on_shelf_info
LIMIT 10;

SELECT *
FROM zydb.dw_goods_on_sale
WHERE goods_id = 374404
LIMIT 10;



-- HK?????????????????
SELECT SUBSTR(shipping_time, 1, 10) AS ship_date
        ,COUNT(DISTINCT order_sn) AS order_num
        ,SUM(goods_number) AS goods_num
FROM zydb.dw_order_node_time p1
WHERE p1.depot_id = 6
     AND p1.is_shiped = 1
     AND p1.order_status = 1
     AND p1.pay_status IN (1, 3)
GROUP BY SUBSTR(shipping_time, 1, 10)
ORDER BY ship_date;

-- HK?????????
SELECT data_date
        ,SUM(total_stock_num) AS total_stock_num
FROM zydb.ods_who_wms_goods_stock_total_detail_aws
WHERE depot_id = 6
GROUP BY data_date
ORDER BY data_date;


-- 11?????????ǰ10???????⵱??????
WITH t1 AS
(SELECT SUBSTR(p1.pay_time, 1, 10) AS pay_date
        ,p2.sku_id
        ,SUM(p2.goods_number) AS sales
FROM zydb.dw_order_node_time p1
LEFT JOIN jolly.who_order_goods p2 ON p1.order_id = p2.order_id
WHERE p1.pay_time >= '2017-10-01'
     AND p1.pay_time < '2017-10-12'
GROUP BY SUBSTR(p1.pay_time, 1, 10)
        ,p2.sku_id
)

SELECT *
FROM t1;





-- ???sku???
WITH t1 AS
(SELECT p1.sku_id
        ,p2.depot_id 
        ,SUM(CASE WHEN p2.pay_time < '2017-10-20' THEN p1.goods_number ELSE NULL END) AS sales_num_19
        ,SUM(CASE WHEN p2.pay_time >= '2017-10-20' THEN p1.goods_number ELSE NULL END) AS sales_num_26
FROM jolly.who_order_goods p1
RIGHT JOIN zydb.dw_order_node_time p2 ON p1.order_id = p2.order_id
WHERE p2.pay_time >= '2017-10-12'
     AND p2.pay_time < '2017-10-27'
GROUP BY p1.sku_id
        ,p2.depot_id 
)
SELECT * 
FROM t1
WHERE sku_id IN ()
;







SELECT *
FROM jolly.who_wms_goods_stock_detail_log
WHERE depot_id IN (4, 5, 6)
AND change_type = 12
AND change_time >= unix_timestamp()
LIMIT 10;


-- ??ݸ??????????Ʒ????ķֲ?
SELECT SUBSTR(shipping_time, 1, 7) AS month
        ,(CASE WHEN goods_number <= 4 THEN '<=4??' 
                      WHEN goods_number <= 8 THEN '5-8??'
                      WHEN goods_number >=9 THEN '>=9??'
                      ELSE '??' END) AS goods_num_class
        ,COUNT(order_sn) AS order_num
FROM zydb.dw_order_node_time 
WHERE depot_id = 4
     AND goods_number >=1
GROUP BY SUBSTR(shipping_time, 1, 7) 
        ,(CASE WHEN goods_number <= 4 THEN '<=4??' 
                      WHEN goods_number <= 8 THEN '5-8??'
                      WHEN goods_number >=9 THEN '>=9??'
                      ELSE '??' END)
;


-- ??ֿ?ձ???????????

select depod_id depot_id,count(*) pickup_orders from 
    zydb.dw_order_shipping_tracking_node a
    left join  
    zydb.dw_order_sub_order_fact b
    on a.order_id=b.order_id
    where lading_time>=from_unixtime(unix_timestamp('${data_date}','yyyyMMdd'))
    and lading_time<date_add(from_unixtime(unix_timestamp('${data_date}','yyyyMMdd')),1)
    group by depod_id

-- ????jolly.who_wms_lading_order 
WITH t1 AS
(SELECT p1.lading_sn
        ,p1.depot_id
        ,FROM_UNIXTIME(p1.create_time, 'yyyy-MM-dd') AS create_date
        ,FROM_UNIXTIME(p1.create_time) AS create_time
        ,FROM_UNIXTIME(p1.operate_time, 'yyyy-MM-dd') AS operate_date
        ,FROM_UNIXTIME(p1.operate_time) AS operate_time
        ,COUNT(p2.order_sn) AS order_num
FROM jolly.who_wms_lading_order p1
LEFT JOIN jolly.who_wms_lading_order_detail p2
             ON p1.id = p2.lading_order_id
WHERE p1.operate_time >= unix_timestamp('2017-11-01', 'yyyy-MM-dd')
     AND p1.operate_time < unix_timestamp('2017-11-08', 'yyyy-MM-dd')
     AND p1.lading_status = 2
GROUP BY p1.lading_sn
        ,p1.depot_id
        ,FROM_UNIXTIME(p1.create_time, 'yyyy-MM-dd')
        ,FROM_UNIXTIME(p1.create_time) 
        ,FROM_UNIXTIME(p1.operate_time, 'yyyy-MM-dd') 
        ,FROM_UNIXTIME(p1.operate_time) 
)
SELECT operate_date
        ,create_date
        ,depot_id
        ,SUM(order_num) AS order_num
FROM t1
WHERE depot_id = 4
GROUP BY operate_date
        ,create_date
        ,depot_id
;

-- ӡ???????????Ʒ????ķֲ?
-- jolly.who_region, region_id = 1788, region_name = 'Indonesia'
-- ָ????ƶ??????????
WITH t1 AS
(SELECT p2.goods_num AS total_goods_num
        ,p3.cat_level1_name
        ,p3.cat_level2_name
        ,(CASE WHEN p5.region_id = 1788 THEN 'Indonesia' ELSE 'Others' END) AS country
        ,COUNT(DISTINCT p2.order_sn) AS order_num
        ,SUM(p1.goods_number) AS goods_num
FROM jolly.who_order_info p2 
LEFT JOIN jolly.who_order_goods p1 
             ON p1.order_id = p2.order_id
LEFT JOIN zydb.dim_jc_goods p3 
             ON p3.goods_id = p1.goods_id
LEFT JOIN jolly.who_order_user_info p4 
             ON p2.order_id = p4.order_id 
          --AND p4.country = 1788
LEFT JOIN jolly.who_region p5 
             ON p4.country = p5.region_id AND p5.region_type = 0 AND p5.region_status = 1
WHERE p5.region_id IS NOT NULL
     AND P2.shipping_time >= UNIX_TIMESTAMP('2017-08-01')
     AND P2.shipping_time < UNIX_TIMESTAMP('2017-11-01')
     AND p2.goods_num <=5
     AND p2.goods_num >=1
     AND p2.order_status = 1
     AND p2.is_shiped = 1
GROUP BY p2.goods_num
        ,p3.cat_level1_name
        ,p3.cat_level2_name
        ,(CASE WHEN p5.region_id = 1788 THEN 'Indonesia' ELSE 'Others' END)
)
SELECT *
FROM t1;





SELECT * 
FROM zydb.dw_delivered_order_info
WHERE start_onself_time >= '2017-11-01'
LIMIT 100;

SELECT *
FROM zydb.dw_delivered_receipt_onself p1
WHERE p1.delivered_order_sn = 'GZ2FHD201711071801481083'
;

SELECT pur_order_sn
        ,goods_id
        ,from_unixtime(gmt_created)
from jolly.who_wms_pur_deliver_goods
WHERE pur_order_sn = 'GZ2FHD201711071801481083'
;

SELECT from_unixtime(max(gmt_created))
FROM zydb.ods_wms_pur_deliver_receipt 
--WHERE pur_order_sn = 'GZ2FHD201711071801481083'
;

SELECT from_unixtime(gmt_created) 
        ,pur_order_sn
FROM jolly.who_wms_pur_deliver_receipt
WHERE pur_order_sn = 'GZ2FHD201711071801481083'
;



WITH t1 AS
(SELECT p1.depot_id
        ,p1.delivered_order_sn
        ,P1.goods_id
        ,p1.goods_sn
        ,SUM(p1.delivered_num) AS ǩ??Ʒ???
        ,SUM(p1.checked_num) AS ????????
        ,SUM(p1.exp_num) AS ????Ʒ???
        ,MIN(p1.start_receipt_time) AS ??쿪ʼʱ??
        ,MAX(p1.finish_check_time) AS ????ʱ??
FROM zydb.dw_delivered_receipt_onself p1
WHERE p1.depot_id IN (4, 5, 6, 7)
GROUP BY p1.depot_id
        ,p1.delivered_order_sn
        ,P1.goods_id
        ,p1.goods_sn
)
SELECT depot_id
        ,sum(????????)
FROM t1
WHERE ????ʱ??>= '2017-11-09'
     AND ????ʱ??< '2017-11-10'
group by depot_id
order by depot_id;


-- ?????????????&???????
WITH 
-- GZ2 + DG1 + HK
t1 AS
(SELECT p1.depot_id
        ,FROM_UNIXTIME(p1.change_time, 'yyyy-MM-dd') AS data_date
        ,SUM(CASE WHEN p1.change_type = 5 THEN p1.change_num ELSE 0 END) AS whout_num
        ,SUM(CASE WHEN p1.change_type = 3 THEN p1.change_num ELSE 0 END) AS return_num
FROM jolly.who_wms_goods_stock_detail_log p1
WHERE p1.depot_id IN (4, 5, 6)
     AND p1.change_time >= unix_timestamp('2017-08-01')
GROUP BY p1.depot_id
        ,FROM_UNIXTIME(p1.change_time, 'yyyy-MM-dd')
),
-- SA
t2 AS
(SELECT p1.depot_id
        ,FROM_UNIXTIME(p1.change_time, 'yyyy-MM-dd') AS data_date
        ,SUM(CASE WHEN p1.change_type = 5 THEN p1.change_num ELSE 0 END) AS whout_num
        ,SUM(CASE WHEN p1.change_type = 3 THEN p1.change_num ELSE 0 END) AS return_num
FROM jolly_wms.who_wms_goods_stock_detail_log p1
WHERE p1.depot_id = 7
     AND p1.change_time >= unix_timestamp('2017-08-01')
GROUP BY p1.depot_id
        ,FROM_UNIXTIME(p1.change_time, 'yyyy-MM-dd')
),
-- UNION
t3 AS
(SELECT * FROM t1
UNION ALL 
SELECT * FROM t2
)
SELECT *
FROM t3
LIMIT 10;

-- ÿ??????
-- zydb.dw_order_sub_order_fact
SELECT p1.depod_id AS depot_id
        ,SUBSTR(CASE WHEN p1.pay_id = 41 THEN p1.pay_time ELSE p1.result_pay_time END, 1, 10) AS data_date
        ,SUM(p1.goods_number) AS sale_num
FROM zydb.dw_order_sub_order_fact p1
WHERE p1.depod_id IN (4, 5, 6, 7)
     AND p1.order_status = 1
     AND p1.pay_status IN (1, 3)
     AND (CASE WHEN p1.pay_id = 41 THEN p1.pay_time ELSE p1.result_pay_time END) >= '2017-08-01'
GROUP BY p1.depod_id 
        ,SUBSTR(CASE WHEN p1.pay_id = 41 THEN p1.pay_time ELSE p1.result_pay_time END, 1, 10)
;
-- jolly.who_order_info
WITH t1 as
(SELECT p1.*
        ,decode(prepare_pay_time = 0, pay_time, prepare_pay_time) AS real_pay_time
FROM jolly.who_order_info p1
WHERE p1.order_status = 1
    AND p1.pay_status in (1, 3)
)
SELECT FROM_UNIXTIME(real_pay_time, 'yyyy-MM-dd') AS real_pay_date
        ,SUM(goods_num) AS goods_num
FROM t1
WHERE t1.real_pay_time >= UNIX_TIMESTAMP('2017-04-01')
GROUP BY FROM_UNIXTIME(real_pay_time, 'yyyy-MM-dd') 
ORDER BY real_pay_date
;


-- ÿ?ǩ????
SELECT FROM_UNIXTIME(p1.update_time, 'yyyy-MM-dd') AS data_date
        ,SUM(p2.goods_number) AS receive_num
FROM jolly.who_order_shipping_tracking p1 
LEFT JOIN zydb.dw_order_sub_order_fact p2 
             ON p1.order_id = p2.order_id
          AND p1.update_time > UNIX_TIMESTAMP(p2.shipping_time)
WHERE p1.shipping_state=3 
     AND p1.update_time >= UNIX_TIMESTAMP('2017-08-01')
     AND p1.update_time < UNIX_TIMESTAMP(TO_DATE(NOW()))
GROUP BY FROM_UNIXTIME(p1.update_time, 'yyyy-MM-dd') 
LIMIT 10;


SELECT *
FROM jolly.who_wms_picking_exception_detail p1
WHERE p1.order_sn = 'JIDA17111007585307397440'
;

JARI17103012321379095901

SELECT *
FROM jolly.who_wms_returned_order_info p1
WHERE p1.returned_order_sn = 'JIDA17111007585307397440'
;


SELECT *
FROM jolly.who_pur_set_value
WHERE value_id = 24
;



-- δ????ǰ????ʱ????
WITH t1 AS
(SELECT p1.returned_order_id
        ,p1.returned_order_sn
        ,FROM_UNIXTIME(p1.returned_time) AS returned_time
        ,p2.pay_time
        ,p2.outing_stock_time
        ,p2.shipping_time
        ,CEILING((p1.returned_time - UNIX_TIMESTAMP(p2.pay_time))/3600) AS pay_to_return
        ,CEILING((p1.returned_time - UNIX_TIMESTAMP(p2.outing_stock_time))/3600) AS pay_to_outing_stock
        ,p2.is_shiped
        ,(CASE WHEN is_shiped IN (0, 4, 5) THEN 'pay_not_prepare' ELSE 'prepare_not_ship' END) AS status
FROM jolly.who_wms_returned_order_info p1
LEFT JOIN zydb.dw_order_node_time p2
             ON p1.returned_order_id = p2.order_id
WHERE p1.returned_time >= UNIX_TIMESTAMP('2017-11-16')
     AND p1.returned_time < UNIX_TIMESTAMP('2017-11-23')
     AND p2.is_shiped NOT IN (1, 2)
)
-- is_shiped IN (3, 6, 7, 8)???????????δ????
-- is_shiped IN (0, 4, 5)???δ???

SELECT *
FROM t1 
WHERE is_shiped = 7
LIMIT 10;

-- 分组汇总
SELECT pay_to_return
        --,status
        ,COUNT(returned_order_sn) AS order_num
FROM t1
GROUP BY pay_to_return
        --,status
;






