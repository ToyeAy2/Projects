SELECT s.s_store_sk, s.s_floor_space, SUM(ss.ss_net_paid) AS total_net_paid 
FROM store_sales_40G ss 
JOIN store_40G s ON ss.ss_store_sk = s.s_store_sk 
WHERE ss.ss_sold_date_sk >= 2450816 AND ss.ss_sold_date_sk <= 2452642 
GROUP BY s.s_store_sk, s.s_floor_space 
ORDER BY total_net_paid DESC, s.s_floor_space DESC 
LIMIT 5;