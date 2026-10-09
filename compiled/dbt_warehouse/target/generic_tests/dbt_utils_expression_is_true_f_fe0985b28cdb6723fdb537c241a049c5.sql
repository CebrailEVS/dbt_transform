



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_technique__intervention_retraitee`

where not(coalesce(convertir_code_5, '') != 'OUI' or montant_effectif is not null)

