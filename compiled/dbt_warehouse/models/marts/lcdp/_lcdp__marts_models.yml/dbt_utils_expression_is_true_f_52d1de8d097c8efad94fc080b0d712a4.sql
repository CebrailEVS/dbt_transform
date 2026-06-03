



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__chargement_sortie`

where not(cout_achat_chargement_eur >= 0)

