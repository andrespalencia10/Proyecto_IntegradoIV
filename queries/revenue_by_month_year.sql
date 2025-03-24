-- TODO: Esta consulta devolverá una tabla con los ingresos por mes y año.
-- Tendrá varias columnas: month_no, con los números de mes del 01 al 12;
-- month, con las primeras 3 letras de cada mes (ej. Ene, Feb);
-- Year2016, con los ingresos por mes de 2016 (0.00 si no existe);
-- Year2017, con los ingresos por mes de 2017 (0.00 si no existe); y
-- Year2018, con los ingresos por mes de 2018 (0.00 si no existe).
WITH monthly_revenue AS (
    SELECT 
        STRFTIME('%m', o.order_purchase_timestamp) AS month_no,
        STRFTIME('%Y', o.order_purchase_timestamp) AS year,
        SUM(oi.price + oi.freight_value) AS revenue
    FROM olist_orders o
    JOIN olist_order_items oi ON o.order_id = oi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY month_no, year
)
SELECT 
    mr.month_no,
    CASE mr.month_no
        WHEN '01' THEN 'Ene'
        WHEN '02' THEN 'Feb'
        WHEN '03' THEN 'Mar'
        WHEN '04' THEN 'Abr'
        WHEN '05' THEN 'May'
        WHEN '06' THEN 'Jun'
        WHEN '07' THEN 'Jul'
        WHEN '08' THEN 'Ago'
        WHEN '09' THEN 'Sep'
        WHEN '10' THEN 'Oct'
        WHEN '11' THEN 'Nov'
        WHEN '12' THEN 'Dic'
    END AS month,
    COALESCE(SUM(CASE WHEN mr.year = '2016' THEN mr.revenue END), 0.00) AS Year2016,
    COALESCE(SUM(CASE WHEN mr.year = '2017' THEN mr.revenue END), 0.00) AS Year2017,
    COALESCE(SUM(CASE WHEN mr.year = '2018' THEN mr.revenue END), 0.00) AS Year2018
FROM monthly_revenue mr
GROUP BY mr.month_no
ORDER BY mr.month_no;
