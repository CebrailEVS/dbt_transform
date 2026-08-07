



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_technique__intervention`

where not((src_inter = 'NESP' and agency is not null) or (src_inter = 'YUMAN' and agency is null))

