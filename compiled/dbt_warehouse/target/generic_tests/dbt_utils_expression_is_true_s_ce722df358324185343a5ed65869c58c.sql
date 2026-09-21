



select
    1
from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_rw`

where not(bad_intervention_id != new_intervention_id)

