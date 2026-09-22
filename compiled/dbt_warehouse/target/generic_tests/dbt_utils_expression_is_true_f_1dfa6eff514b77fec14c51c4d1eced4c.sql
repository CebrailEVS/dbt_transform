



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__mouvement_produit`

where not(qty_invendus >= 0)

