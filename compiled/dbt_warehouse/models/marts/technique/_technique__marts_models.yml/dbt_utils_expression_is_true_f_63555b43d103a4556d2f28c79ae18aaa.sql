



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_technique__workorder_product`

where not(quantity > 0)

