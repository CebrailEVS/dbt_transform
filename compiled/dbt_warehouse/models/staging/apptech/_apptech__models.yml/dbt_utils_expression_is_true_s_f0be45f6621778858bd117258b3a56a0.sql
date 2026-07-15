



select
    1
from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_events`

where not(periode = format('%04d-%02d', annee, mois))

