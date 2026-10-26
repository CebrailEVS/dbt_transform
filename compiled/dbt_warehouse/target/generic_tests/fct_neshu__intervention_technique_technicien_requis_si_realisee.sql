



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_neshu__intervention_technique`

where not(technician_id is not null or intervention_state != 'REALISEE')

