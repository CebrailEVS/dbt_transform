

-- Stock quotidien des vans des techniciens (zones « ST - NOM ») : lignes à quantité
-- positive, additionnées par jour, van et référence. L'app en tire par requête la
-- présence d'une pièce dans les vans actifs de chaque dépôt, jour par jour (taux de
-- service historisé), sans charger l'historique quotidien en mémoire.
select
    stock_date,
    stock,
    reference,
    sum(quantite) as quantite
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_yuman`
where
    starts_with(stock, 'ST - ')
    -- filtre ligne à ligne AVANT la somme, comme l'app
    and quantite > 0
group by stock_date, stock, reference