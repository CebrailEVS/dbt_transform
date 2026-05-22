



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_neshu__consommation`

where not((device_id IS NOT NULL) OR (data_source = 'LIVRAISON'))

