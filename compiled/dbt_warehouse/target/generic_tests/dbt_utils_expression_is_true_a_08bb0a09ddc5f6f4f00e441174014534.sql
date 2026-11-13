



select
    1
from `evs-datastack-prod`.`prod_app_compta`.`app_compta__vehicule_imputation`

where not((code_analytique is null) = (motif_a_categoriser is not null))

