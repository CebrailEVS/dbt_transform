



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__point_commande_neshu`

where not(quantite_a_commander_conditionnee * coeff_conditionnement >= quantite_a_commander)

