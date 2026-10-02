SELECT order_id, amount, category
FROM catalog_A.database_B.schema_C.orders_l1
WHERE amount IS NOT NULL
ORDER BY order_id
