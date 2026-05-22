



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_neshu__monitoring_passage_appro`

where not(task_start_date <= task_end_date)

