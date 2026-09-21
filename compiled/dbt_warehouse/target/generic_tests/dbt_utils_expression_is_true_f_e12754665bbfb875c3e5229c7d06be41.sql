



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_technique__credit_repair_warranty`

where not(montant_credit < 0)

