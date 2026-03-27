



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__monitoring_passages_appro`

where not(task_start_date <= task_end_date)

