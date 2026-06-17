



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_lcdp__ca_mensuel`

where not(ca_total_ttc_eur >= 0)

