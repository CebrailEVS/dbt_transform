



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_lcdp`

where not(stock_at_date = coalesce(stock_inventaire, 0) + coalesce(plus, 0) - coalesce(moins, 0))

