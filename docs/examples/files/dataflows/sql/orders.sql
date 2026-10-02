SELECT order_id, amount, category
FROM orders
WHERE amount > 0
ORDER BY order_id
