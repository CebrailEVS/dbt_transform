



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_article_yuman`

where not(is_out_of_stock_technicien = (technicien_quantity = 0))

