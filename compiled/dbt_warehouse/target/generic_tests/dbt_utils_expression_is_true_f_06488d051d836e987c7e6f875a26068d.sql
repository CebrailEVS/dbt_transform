



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_technique__repair`

where not(is_repair = (episode_rank > 1))

