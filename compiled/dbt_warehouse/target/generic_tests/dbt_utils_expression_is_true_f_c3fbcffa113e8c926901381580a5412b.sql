



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_finance__ecriture_non_ventilee`

where not(substr(numero_compte_general, 1, 1) in ('6', '7'))

