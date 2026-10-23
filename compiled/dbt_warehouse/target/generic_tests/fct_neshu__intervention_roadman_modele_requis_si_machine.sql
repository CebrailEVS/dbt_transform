



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_neshu__intervention_roadman`

where not(modele_equipement_id is not null or device_id is null)

