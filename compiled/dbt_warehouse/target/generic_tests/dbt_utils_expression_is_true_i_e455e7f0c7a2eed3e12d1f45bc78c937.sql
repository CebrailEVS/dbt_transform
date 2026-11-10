



select
    1
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__stock_theorique`

where not(abs(stock_at_date - (stock_inventaire + plus - moins)) < 0.001)

