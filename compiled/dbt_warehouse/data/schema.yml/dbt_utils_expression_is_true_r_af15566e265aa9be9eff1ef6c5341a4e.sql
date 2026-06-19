



select
    1
from `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__product_packaging`

where not(units_per_pack  > 1)

