



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_bi__usage_rapport`

where not(is_dormant = (nb_consultations = 0))

