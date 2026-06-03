



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__chargement_sortie`

where not(ca_ht_sortie_eur >= 0)

