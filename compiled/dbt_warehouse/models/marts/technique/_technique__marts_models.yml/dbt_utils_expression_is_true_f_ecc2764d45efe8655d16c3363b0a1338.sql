



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_technique__intervention_retraitee`

where not(not (a_facturer_mee = 'OUI' and a_facturer_modif = 'NOT VALIDATED'))

