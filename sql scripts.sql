-- 查询 WHO_ORDER_INFO 表
-- 特定订单
SELECT *
FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO
WHERE ORDER_ID = 8045413;

-- 配送方式:SHIPPING_ID, SHIPPING_NAME
-- 不同的SHIPPING_ID对应的含义是？
-- 同一个SHIPPING_ID为什么对应不同的SHIPPING_NAME?
SELECT DISTINCT SHIPPING_ID ,
                SHIPPING_NAME
FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO
ORDER BY SHIPPING_ID;

SELECT SHIPPING_ID,
       COUNT(ORDER_ID) AS ORDER_NUM
FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO
GROUP BY SHIPPING_ID
ORDER BY ORDER_NUM DESC;

-- 支付ID，支付NAME
-- PAY_ID对应支付方式，具体的含义是？
-- 相同的PAY_ID为什么对应不同的PAY_NAME?
SELECT DISTINCT PAY_ID,
                PAY_NAME
FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO
ORDER BY PAY_ID;

SELECT PAY_ID, PAY_NAME,
       COUNT(ORDER_ID) AS ORDER_NUM
FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO
WHERE PAY_STATUS = 1
GROUP BY PAY_ID, PAY_NAME
ORDER BY ORDER_NUM DESC;

SELECT COUNT(1) FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO;


-- 已支付的订单，平均物流费用为5.48元；
SELECT SUM(SHIPPING_MONEY) AS SHIPPING_MONEY,
COUNT(ORDER_ID) AS ORDER_NUM
FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO
WHERE PAY_STATUS = 1;

SELECT ORDER_ID, SHIPPING_MONEY, SHIPPING_FEE_DISCOUNT
FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO
WHERE ROWNUM < 10;

-- 使用红包或礼品券的订单数量
SELECT COUNT(1) FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO
WHERE BONUS_MONEY > 0;
--
SELECT COUNT(1) AS ORDER_NUM,
SUM(CASE WHEN ORDER_AMOUNT >0 THEN 1 ELSE 0 END) AS ORDER_NUM2,
SUM(ORDER_AMOUNT) AS ORDER_AMOUNT
FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO;

-- 最早的订单时间
SELECT UNIX_TO_ORACLE(MIN(ADD_TIME)) FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO;

-- 付税的订单数（0）
SELECT COUNT(1) FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO
WHERE TAX > 0;

-- 客件数
SELECT AVG(GOODS_NUM) FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO
WHERE PAY_STATUS = 1;

-- 把时间戳转换成日期
SELECT UNIX_TO_ORACLE(1112070645) AS CDATE
FROM DUAL;

-- 下单时选择的货币，怎么有为空的记录？
SELECT CURRENCY, COUNT(ORDER_ID) AS ORDER_NUM
FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO
WHERE PAY_STATUS = 1
GROUP BY CURRENCY
ORDER BY ORDER_NUM DESC;

-- 汇率，是跟人民币还是跟美元的汇率？
SELECT DISTINCT RATE FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO;

-- 各种金额
-- 一个订单中包括哪些金额？它们之间的关系是？
SELECT ORDER_ID, GOODS_AMOUNT, ORDER_AMOUNT, ORDER_TOTAL_AMOUNT, PAY_MONEY, TOTAL_GOODS_AMOUNT
FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO WHERE ROWNUM < 200;

-- 订单来源ORDER_SOURCE
SELECT ORDER_SOURCE, COUNT(ORDER_ID) AS ORDER_NUM
FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO
GROUP BY ORDER_SOURCE
ORDER BY ORDER_NUM DESC;

-- 前台选择物流方式和实际物流方式
-- 绝大多数都不相等，为什么？
SELECT
(CASE WHEN SHIPPING_ID = REAL_SHIPPING_ID THEN 1 ELSE 0 END) AS SHIPPING,
COUNT(ORDER_ID) AS ORDER_NUM
FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO
GROUP BY (CASE WHEN SHIPPING_ID = REAL_SHIPPING_ID THEN 1 ELSE 0 END);

-- 订单是否违禁品
-- 违禁品从哪里来？怎么判断的？
SELECT ORDER_PRO_STATUS, COUNT(ORDER_ID) AS ORDER_NUM
FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO
GROUP BY ORDER_PRO_STATUS;


-- 查询发货订单笔数和发货时长，从而计算平均发货时长
SELECT TRUNC(SHIPPING_TIME) AS SHIPPING_DATE,
              COUNT(ORDER_ID) AS SHIP_PAID_ORDER_NUM,
              ROUND(SUM(CASE WHEN PAY_ID = 41 AND SHIPPING_TIME < PAY_TIME THEN 0
                                               WHEN PAY_ID = 41 AND SHIPPING_TIME > PAY_TIME THEN (SHIPPING_TIME - PAY_TIME)
                                               WHEN PAY_ID <> 41 AND SHIPPING_TIME < RESULT_PAY_TIME THEN 0
                                               WHEN PAY_ID <> 41 AND SHIPPING_TIME > RESULT_PAY_TIME THEN (SHIPPING_TIME - RESULT_PAY_TIME)
                                               ELSE 0
                                    END)
              , 2) AS SHIPPING_TOTAL_TIME
FROM ZYBI.DW_ORDER_SUB_ORDER_FACT OSO
WHERE TRUNC(SHIPPING_TIME) >= TRUNC(SYSDATE - 5)
AND IS_SHIPED = 1
AND SITE_ID IN (400, 600, 700, 800, 900)
GROUP BY TRUNC(SHIPPING_TIME)
ORDER BY TRUNC(SHIPPING_TIME);

-- 查询特定日期的订单的发货时长TOP100
SELECT * FROM
(SELECT ORDER_ID,
                SHIPPING_TIME,
                (CASE WHEN PAY_ID = 41 THEN PAY_TIME ELSE RESULT_PAY_TIME END) AS PAY_TIME,
                ROUND((CASE WHEN PAY_ID = 41 AND SHIPPING_TIME < PAY_TIME THEN 0
                                         WHEN PAY_ID = 41 AND SHIPPING_TIME > PAY_TIME THEN (SHIPPING_TIME - PAY_TIME)
                                         WHEN PAY_ID <> 41 AND SHIPPING_TIME < RESULT_PAY_TIME THEN 0
                                         WHEN PAY_ID <> 41 AND SHIPPING_TIME > RESULT_PAY_TIME THEN (SHIPPING_TIME - RESULT_PAY_TIME)
                                         ELSE 0
                              END)
                 ,2) AS SHIPPING_TOTAL_TIME
FROM ZYBI.DW_ORDER_SUB_ORDER_FACT OSO
WHERE TRUNC(SHIPPING_TIME) = TRUNC(SYSDATE - 5)
  AND IS_SHIPED = 1
  AND SITE_ID IN (400, 600, 700, 800, 900)
ORDER BY SHIPPING_TOTAL_TIME DESC)
WHERE ROWNUM < 100;

-- 近15天发货的订单，发货时长订单数分布
SELECT SHIPPING_DATE, SHIPPING_TOTAL_TIME, COUNT(ORDER_ID) AS ORDER_NUM
FROM
(SELECT ORDER_ID,
                TRUNC(SHIPPING_TIME) AS SHIPPING_DATE,
                FLOOR((CASE WHEN PAY_ID = 41 AND SHIPPING_TIME < PAY_TIME THEN 0
                                         WHEN PAY_ID = 41 AND SHIPPING_TIME > PAY_TIME THEN (SHIPPING_TIME - PAY_TIME)
                                         WHEN PAY_ID <> 41 AND SHIPPING_TIME < RESULT_PAY_TIME THEN 0
                                         WHEN PAY_ID <> 41 AND SHIPPING_TIME > RESULT_PAY_TIME THEN (SHIPPING_TIME - RESULT_PAY_TIME)
                                         ELSE 0
                              END)
                            ) AS SHIPPING_TOTAL_TIME
FROM ZYBI.DW_ORDER_SUB_ORDER_FACT OSO
WHERE TRUNC(SHIPPING_TIME) >= TRUNC(SYSDATE - 15)
  AND IS_SHIPED = 1
  AND SITE_ID IN (400, 600, 700, 800, 900))
GROUP BY SHIPPING_DATE, SHIPPING_TOTAL_TIME
ORDER BY SHIPPING_DATE, SHIPPING_TOTAL_TIME;

-- 各仓库特定日期范围内发货时长订单数分布
WITH T AS
(
SELECT ORDER_ID,
        DEPOD_ID,
        TRUNC(SHIPPING_TIME) AS SHIPPING_DATE,
        CEIL((CASE WHEN PAY_ID = 41 AND SHIPPING_TIME < PAY_TIME THEN 0
                                 WHEN PAY_ID = 41 AND SHIPPING_TIME > PAY_TIME THEN (SHIPPING_TIME - PAY_TIME)
                                 WHEN PAY_ID <> 41 AND SHIPPING_TIME < RESULT_PAY_TIME THEN 0
                                 WHEN PAY_ID <> 41 AND SHIPPING_TIME > RESULT_PAY_TIME THEN (SHIPPING_TIME - RESULT_PAY_TIME)
                                 ELSE 0
                      END)
                    ) AS SHIPPING_IN_HOUR,
        (CASE WHEN PAY_ID = 41 AND SHIPPING_TIME < PAY_TIME THEN 0
                      WHEN PAY_ID = 41 AND SHIPPING_TIME > PAY_TIME THEN (SHIPPING_TIME - PAY_TIME)
                      WHEN PAY_ID <> 41 AND SHIPPING_TIME < RESULT_PAY_TIME THEN 0
                      WHEN PAY_ID <> 41 AND SHIPPING_TIME > RESULT_PAY_TIME THEN (SHIPPING_TIME - RESULT_PAY_TIME)
                      ELSE 0
          END) AS SHIPPING_TOTAL_TIME
FROM ZYBI.DW_ORDER_SUB_ORDER_FACT OSO
WHERE TRUNC(SHIPPING_TIME) >= TRUNC(SYSDATE - 10)
  AND IS_SHIPED = 1
  AND SITE_ID IN (400, 600, 700, 800, 900)
)

-- 各仓库特定日期范围内发货时长订单数分布
SELECT DEPOD_ID
        , SHIPPING_DATE
        , SHIPPING_IN_HOUR
        , SUM(SHIPPING_TOTAL_TIME) AS SHIPPING_TOTAL_TIME
        , COUNT(ORDER_ID) AS ORDER_NUM
FROM T
GROUP BY DEPOD_ID, SHIPPING_DATE, SHIPPING_IN_HOUR
ORDER BY SHIPPING_DATE, DEPOD_ID, SHIPPING_IN_HOUR;

-- 发货时长较长的订单明细



-- IS_SHIPED = 1时 DEPOD_ID IS NULL 的订单数(2015/16两个年度有这样的数据)
SELECT TO_CHAR(SHIPPING_TIME, 'YYYY') AS SHIPPING_YEAR,
              COUNT(ORDER_ID) AS ORDER_NUM
FROM ZYBI.DW_ORDER_SUB_ORDER_FACT
WHERE IS_SHIPED = 1
  AND DEPOD_ID IS NULL
GROUP BY TO_CHAR(SHIPPING_TIME, 'YYYY');


SELECT * FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO WHERE ROWNUM < 10;


SELECT *
FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO
WHERE IS_SEPARATE = 1
AND ROWNUM < 10;

SELECT * FROM ZYBI.DW_ORDER_SUB_ORDER_FACT WHERE ROWNUM < 10;


SELECT DISTINCT IS_SEPARATE FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO;

SELECT IS_SEPARATE, COUNT(ORDER_ID) FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO GROUP BY IS_SEPARATE;

SELECT ORDER_ID, ORDER_SN, IS_SEPARATE, PARENT_ID
FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO
WHERE PARENT_ID IS NOT NULL
AND ROWNUM < 10;



-- 查询 DW_ORDER_FACT 表

SELECT * FROM ZYBI.DW_ORDER_FACT WHERE ROWNUM < 10;

SELECT SITE_ID, COUNT(ORDER_ID) AS ORDER_NUM FROM ZYBI.DW_ORDER_FACT
GROUP BY SITE_ID
ORDER BY ORDER_NUM DESC;

-- 每年已支付订单的数量（1970年144张，如果再加上未支付的订单，1970年达到390多万张）
SELECT TO_CHAR(PAY_TIME, 'YYYY') AS ORDER_YEAR,
              COUNT(ORDER_ID) AS ORDER_NUM
FROM  ZYBI.DW_ORDER_SUB_ORDER_FACT
WHERE PAY_STATUS = 1
GROUP BY TO_CHAR(PAY_TIME, 'YYYY')
ORDER BY ORDER_YEAR;

-- 已发货订单不同支付状态的订单数
SELECT IS_SHIPED, PAY_STATUS, COUNT(ORDER_ID)
FROM ZYBI.DW_ORDER_SUB_ORDER_FACT
WHERE IS_SHIPED = 1
GROUP BY IS_SHIPED, PAY_STATUS;

-- 非COD订单RESULT_PAY_TIME和PAY_TIME差异
SELECT (CASE WHEN TIME_GE >= 30 THEN '>=30分钟' ELSE '<30分钟' END) AS TIME_GE2,
              ORDER_YEAR,
              COUNT(ORDER_ID) AS ORDER_NUM
FROM
(SELECT ORDER_ID,
               ROUND(NVL(ABS(PAY_TIME - RESULT_PAY_TIME), 0) * 24 * 60, 0) AS TIME_GE,
               TO_CHAR(SHIPPING_TIME, 'YYYY') AS ORDER_YEAR
FROM ZYBI.DW_ORDER_SUB_ORDER_FACT OSO
WHERE PAY_ID <> 41)
GROUP BY (CASE WHEN TIME_GE >= 30 THEN '>=30分钟' ELSE '<30分钟' END), ORDER_YEAR
ORDER BY ORDER_YEAR;

-- 统计SHIPPING_TIME比PAY_TIME或RESULT_PAY_TIME更早的订单数
SELECT TO_CHAR(SHIPPING_TIME, 'YYYY-MM') AS SHIPPING_YEAR,
              COUNT(ORDER_ID) AS ORDER_NUM
FROM ZYBI.RPT_SCM_ORDER_TMP P1
WHERE IS_SHIPED = 1
AND (SHIPPING_TIME < PAY_TIME OR SHIPPING_TIME < RESULT_PAY_TIME)
GROUP BY TO_CHAR(SHIPPING_TIME, 'YYYY-MM')
ORDER BY SHIPPING_YEAR;

-- 统计IS_SHIPED = 1的订单中SHIPPING_TIME IS NULL(1970)或PAY_TIME IS NULL(1970) 或 RESULT_PAY_TIME IS NULL(1970) 的订单数
SELECT TO_CHAR(SHIPPING_TIME, 'YYYY-MM') AS SHIPPING_YEAR,
              SUM(CASE WHEN TRUNC(SHIPPING_TIME)  = DATE'1970-01-01' THEN 1 ELSE 0 END) AS SHIPPING_NULL_NUM,
              SUM(CASE WHEN PAY_ID = 41 AND TRUNC(PAY_TIME)  = DATE'1970-01-01' THEN 1 ELSE 0 END) AS PAY41_NULL_NUM,
              SUM(CASE WHEN PAY_ID <> 41 AND TRUNC(RESULT_PAY_TIME)  = DATE'1970-01-01' THEN 1 ELSE 0 END) AS PAY_NULL_NUM
FROM ZYBI.RPT_SCM_ORDER_TMP P1
WHERE IS_SHIPED = 1
GROUP BY TO_CHAR(SHIPPING_TIME, 'YYYY-MM')
ORDER BY SHIPPING_YEAR;



SELECT DISTINCT POP_ID FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO;
SELECT DISTINCT POP_ID FROM ZYBI.DW_ORDER_SUB_ORDER_FACT;


SELECT COUNT(DISTINCT ID) AS ID_NUM,
COUNT(DISTINCT ORDER_ID) AS ORDER_NUM,
COUNT(DISTINCT INVOICE_NO) AS INVOICE_NUM
FROM JOLLY_BRANDS_ZY702.WHO_ORDER_SHIPPING_TRACKING;

SELECT COUNT(ORDER_ID)
FROM ZYBI.DW_ORDER_SUB_ORDER_FACT
WHERE PAY_STATUS = 1
AND IS_SHIPED = 1;

SELECT COUNT(1)
FROM JOLLY_BRANDS_ZY702.WHO_ORDER_SHIPPING_TRACKING
WHERE INVOICE_NO IS NULL;

SELECT * FROM ZYBI.DW_ORDER_SUB_ORDER_FACT
WHERE ADD_TIME >= TRUNC(TO_DATE('2017-01-01', 'YYYY-MM-DD'))
AND ROWNUM < 10;

SELECT ZYBI.ORACLE_TO_UNIX(TO_DATE('2017-01-01', 'YYYY-MM-DD')) FROM DUAL;


SELECT DISTINCT PAY_ID,
                PAY_NAME
FROM JOLLY_BRANDS_ZY702.WHO_ORDER_INFO
ORDER BY PAY_ID;

-- TRUNC(TO_DATE('2017-06-06', 'YYYY-MM-DD'))

-- 2017年POP_ID订单数
SELECT POP_ID, COUNT(ORDER_ID)
FROM ZYBI.DW_ORDER_SUB_ORDER_FACT
WHERE ADD_TIME >= TRUNC(TO_DATE('2017-01-01', 'YYYY-MM-DD'))
AND ADD_TIME < TRUNC(TO_DATE('2017-06-07', 'YYYY-MM-DD'))
GROUP BY POP_ID;





