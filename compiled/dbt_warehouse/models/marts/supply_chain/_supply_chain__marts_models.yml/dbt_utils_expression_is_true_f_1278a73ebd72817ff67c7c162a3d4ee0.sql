



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_article_yuman`

where not(is_out_of_stock = (is_out_of_stock_depot and is_out_of_stock_technicien))

