

-- Stock quotidien des vans des techniciens (zones « ST - NOM ») : lignes à quantité
-- positive, additionnées par jour, van et référence. L'app en tire par requête la
-- présence d'une pièce dans les vans actifs de chaque dépôt, jour par jour (taux de
-- service historisé), sans charger l'historique quotidien en mémoire.
with stock_yuman as (
    -- Stock théorique Yuman exporté chaque jour : une ligne par zone (dépôt ou van) et référence.
    select
        reference,
        designation,
        nom_du_stock as stock,
        case when (nom_du_stock like '%DEPOT%') then 'DEPOT' else 'TECHNICIEN' end as type_stock,
        quantite,
        date(export_date) as stock_date
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_evs_sftp__stock_theorique`
    where
        reference is not null
        and nom_du_stock is not null
)

select
    stock_date,
    stock,
    reference,
    sum(quantite) as quantite
from stock_yuman
where
    starts_with(stock, 'ST - ')
    -- filtre ligne à ligne AVANT la somme, comme l'app
    and quantite > 0
group by stock_date, stock, reference