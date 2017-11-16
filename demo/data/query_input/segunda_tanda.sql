SELECT 
    alias_t1.a,
    COUNT(DISTINCT alias_t1.a) OVER (PARTITION BY alias_t1.c),
    alias_t1.b AS total,
    alias_t1.a
FROM t1 alias_t1
WHERE total > 1000

UNION ALL

SELECT 
    a,
    b
FROM t1

UNION ALL

SELECT a,b,c FROM t2
;


-- esta query es de prueba 
INSERT OVERWRITE TABLE t1 PARTITION(a = '12/02/2019', b)
SELECT
    alias_t1.a,
    t2.a t2_a,
    IF (ISNOTNULL(t2.suma), SUM(t2.suma), 0) AS total,
    CASE
        WHEN SUM(REGEXP_REPLACE(t6.a, 'hola', 'adios')) > COUNT(${hivevar:variable}) THEN t2.a
        WHEN t6.b = 6 THEN ${hivevar:variable}
        ELSE 0
        END AS case_when
FROM 
    t1 alias_t1,
    (
        SELECT 
            tp_sub.a, 
            SUM(tp_sub.b) AS suma 
        FROM (SELECT t2.a, t1.b FROM t2 LEFT JOIN t1 ON t1.a = 'hola', t1) AS tp_sub
    ) AS t2,
    (SELECT b, a a_s FROM t4) AS sub_2
WHERE (alias_t1.a = alias_t1.b AND alias_t1.where_column > alias_t1.c) OR alias_t1.c < alias_t1.a
CLUSTER BY total INTO 7 BUCKETS
DISTRIBUTE BY alias_t1.a SORT BY t2.a ASC, total DESC
WINDOW a AS (PARTITION BY t1.b, t2.a ORDER BY c DESC, d ASC)
GROUP BY t2.a, total
HAVING (t2.a <= 150 AND total > 0)
ORDER BY alias_t1.a, t2_a DESC
UNION SELECT a FROM t5
;