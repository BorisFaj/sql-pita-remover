-- Primera query
INSERT OVERWRITE TABLE ${hivevar:db}.t1 PARTITION(PARTITION_DT = '12/02/2019', b)
SELECT 
    alias_t1.a,  -- aqui se tiene que renombrar la columna a la que corresponda de t1
    t2.a t2_a,  -- aqui se tiene que renombrar la columna, que viene de t3, la tabla es un alias
    IF (ISNOTNULL(t2.suma), SUM(t2.suma), 0) AS total,  -- aqui no se tiene que renombrar nada porque son todos alias
    CASE
        WHEN SUM(REGEXP_REPLACE(t6.a, 'hola', 'adios')) > COUNT(${hivevar:variable}) THEN t2.a
        WHEN t6.b = 6 THEN ${hivevar:variable}
        ELSE 0
        END AS case_when
    FROM t1 alias_t1,
        (SELECT tp_sub.a, SUM(tp_sub.b) AS suma FROM  -- se tiene que renombrar a que viene de t3 y b que viene de t1
         (SELECT t3.a, t1.b FROM t3 LEFT JOIN t1 ON t1.a = 'hola', t1) AS tp_sub  -- se tiene que renombrar todo menos el alias
        ) AS t2,
        (SELECT NVL(b, 0) as b, 'columna' a_s FROM t4) AS sub_2  -- se tiene que renombrar todo menos el alias
    WHERE (alias_t1.a = 'hola' AND alias_t1.where_column > alias_t1.c)  -- se tienen que renombrar las columnas conforme a t1
    OR alias_t1.c < 100
;

-- Segunda query
SELECT 
    a,
    b,
    c,
    d AS where_column,
    e AS p
FROM (
        SELECT
            ROW_NUMBER() OVER a AS t2_a,
            t1.*  -- todo t1
        FROM t1
        DISTRIBUTE BY b
        WINDOW a AS (
            PARTITION BY d, b, a
            ORDER BY p DESC, p DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
     ) AS sub_2
WHERE sub_2.t2_a = 8         
;