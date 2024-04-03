WITH sales_filtered AS (
  SELECT ss_store_sk, ss_item_sk, ss_quantity, ss_sold_date_sk 
  FROM store_sales_40G 
  WHERE ss_sold_date_sk >= 2450816 AND ss_sold_date_sk <= 2452642
),
top_stores AS (
  SELECT ss_store_sk, SUM(ss_quantity) AS total_sold 
  FROM sales_filtered
  GROUP BY ss_store_sk
  ORDER BY total_sold DESC
  LIMIT 3
),
top_items AS (
  SELECT ss_store_sk, ss_item_sk, SUM(ss_quantity) AS total_quantity 
  FROM sales_filtered 
  WHERE ss_store_sk IN (SELECT ss_store_sk FROM top_stores)
  GROUP BY ss_store_sk, ss_item_sk
),
store_items AS (
  SELECT ti.ss_store_sk, ti.ss_item_sk, ti.total_quantity, ts.total_sold 
  FROM top_items ti
  JOIN top_stores ts ON ti.ss_store_sk = ts.ss_store_sk
)
SELECT si.ss_store_sk, si.ss_item_sk, si.total_quantity
FROM (
  SELECT ss_store_sk, ss_item_sk, total_quantity, 
  ROW_NUMBER() OVER (PARTITION BY ss_store_sk ORDER BY total_quantity ASC) AS item_rank, 
  RANK() OVER (ORDER BY total_sold DESC) AS store_rank
  FROM store_items
) si
WHERE si.item_rank <= 5
ORDER BY si.store_rank ASC, si.total_quantity DESC;