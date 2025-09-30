



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__conso_business_review`

where not((device_id IS NOT NULL) OR (data_source = 'LIVRAISON'))

