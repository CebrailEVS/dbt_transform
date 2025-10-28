



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__passages_appro`

where not(passage_duration_min >= 0 OR passage_duration_min IS NULL)

