



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__supply_flux`

where not(reception_fournisseur >= 0 and livraison_client >= 0)

