



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_neshu__intervention_technique`

where not(device_id is not null or objet_intervent is null)

