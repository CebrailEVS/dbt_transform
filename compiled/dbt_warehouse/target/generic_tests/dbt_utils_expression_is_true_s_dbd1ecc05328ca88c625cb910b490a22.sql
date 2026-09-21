



select
    1
from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_rw`

where not(periode = format('%04d-%02d', annee, mois))

