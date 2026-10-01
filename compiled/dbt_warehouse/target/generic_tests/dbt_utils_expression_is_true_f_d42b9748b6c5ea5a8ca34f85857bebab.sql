



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__point_commande_neshu`

where not((quantite_excedentaire > 0) = (statut_reappro = 'surstock'))

