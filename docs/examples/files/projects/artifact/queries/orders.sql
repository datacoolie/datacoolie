SELECT o.order_id, o.amount, c.category_group
FROM orders AS o
JOIN order_categories AS c ON o.category = c.category
WHERE o.amount IS NOT NULL
ORDER BY o.order_id
