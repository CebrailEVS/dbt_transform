



select
    1
from (select * from `evs-datastack-prod`.`prod_marts`.`fct_neshu__intervention_technique` where device_id not in (367, 368, 376)) dbt_subquery

where not(modele_equipement_id is not null or device_id is null)

