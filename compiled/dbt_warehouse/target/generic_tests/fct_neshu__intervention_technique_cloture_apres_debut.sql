



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_neshu__intervention_technique`

where not(date_done >= date_started)

