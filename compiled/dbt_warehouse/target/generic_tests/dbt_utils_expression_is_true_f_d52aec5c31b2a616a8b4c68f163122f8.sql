



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_finance__pnl_section_mensuel`

where not(categorie_pnl_bu is not null)

