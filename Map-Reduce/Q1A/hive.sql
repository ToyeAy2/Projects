SELECT ss_store_sk, SUM(ss_net_paid) AS Revenue 
FROM store_sales_40G 
WHERE ss_store_sk IS NOT NULL AND ss_net_paid IS NOT NULL AND ss_sold_date_sk BETWEEN 2450816 AND 2452642 
GROUP BY ss_store_sk 
ORDER BY Revenue ASC 
LIMIT 5;