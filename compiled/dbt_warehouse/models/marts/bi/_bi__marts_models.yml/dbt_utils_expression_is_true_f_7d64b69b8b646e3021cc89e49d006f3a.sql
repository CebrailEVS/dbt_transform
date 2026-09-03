



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_bi__activite_rapport_jour`

where not(nb_consultations > 0 or nb_rafraichissements > 0)

