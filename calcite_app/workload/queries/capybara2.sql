SELECT l1.l_orderkey, l1.l_quantity
FROM opt_project.lineitem l1, opt_project.lineitem l2
WHERE l1.l_orderkey = l2.l_orderkey
  AND l2.l_quantity > 30