



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__passages_appro`

where not(passage_start_datetime <= passage_end_datetime)

