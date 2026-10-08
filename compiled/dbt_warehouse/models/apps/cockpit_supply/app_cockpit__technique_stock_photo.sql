

-- Photos de stock TechCare dont l'app a besoin : la première photo GLOBALE de chaque
-- mois (stock d'ouverture / de clôture des flux) et la dernière photo de chaque ZONE
-- (dépôt ou van) dans chaque mois (stock courant, fin de mois des dépôts). Par zone et
-- non par (zone × article) : chez Yuman une ligne absente vaut stock 0, une référence
-- sortie d'une zone en cours de mois doit donc disparaître de sa photo de fin de mois.
-- Toutes les lignes de ces jours sont gardées, doublons compris (l'app les additionne).
with photos as (
    select
        *,
        min(stock_date) over (partition by date_trunc(stock_date, month)) as d_premiere_mois,
        max(stock_date) over (partition by stock, date_trunc(stock_date, month)) as d_derniere_zone_mois,
        max(stock_date) over () as d_courante
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_yuman`
)

select
    stock_date,
    stock,
    type_stock,
    reference,
    designation,
    quantite,
    stock_date = d_premiere_mois as is_premiere_photo_mois,
    stock_date = d_derniere_zone_mois as is_derniere_photo_zone_mois,
    stock_date = d_courante as is_photo_courante
from photos
where stock_date = d_premiere_mois or stock_date = d_derniere_zone_mois