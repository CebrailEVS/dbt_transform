



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__rupture_depot_yuman`

where not(nb_conso_180j >= 2)

