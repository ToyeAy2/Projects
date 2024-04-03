SELECT ss_sold_date_sk, SUM(ss_net_paid_inc_tax) AS total_sales 
FROM store_sales_40G 
WHERE ss_sold_date_sk >= 2450816 AND ss_sold_date_sk <= 2452642 
GROUP BY ss_sold_date_sk 
ORDER BY total_sales DESC 
LIMIT 5;