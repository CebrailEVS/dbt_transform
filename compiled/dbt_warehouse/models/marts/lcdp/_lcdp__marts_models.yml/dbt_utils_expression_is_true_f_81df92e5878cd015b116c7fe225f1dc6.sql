



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__chargement_sortie`

where not(qty_vendable_retiree >= 0)

