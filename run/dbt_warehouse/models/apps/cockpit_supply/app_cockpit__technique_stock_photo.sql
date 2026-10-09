
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__technique_stock_photo`
      
    partition by date_trunc(stock_date, month)
    cluster by stock, reference

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Photos de stock TechCare (d\u00e9p\u00f4ts ateliers et vans des techniciens) dont l'application Cockpit Supply a besoin : la photo du d\u00e9but de chaque mois (stocks d'ouverture et de cl\u00f4ture des flux) et la derni\u00e8re photo de chaque zone dans chaque mois (stock courant, fin de mois des d\u00e9p\u00f4ts, CODIR).\n[COMMENT CONSTRUITE] Sous-ensemble de fct_supply_chain__stock_yuman : toutes les lignes des jours qui sont soit le premier jour de photo du mois (toutes zones confondues), soit le dernier jour de photo de leur zone (colonne stock) dans le mois. Colonnes reprises sans transformation, plus trois indicateurs de type de photo.\n[GRAIN] Celui de la source : 1 ligne par (stock_date, stock, reference), avec les quelques doublons de la source (l'app les additionne), sur environ 2 jours par zone et par mois.\n[NOTES] Bornes par ZONE et non par (zone \u00d7 article) : chez Yuman une ligne absente vaut stock 0, une r\u00e9f\u00e9rence sortie d'une zone en cours de mois doit dispara\u00eetre de sa photo de fin de mois (diff\u00e9rence volontaire avec app_cockpit__neshu_stock_photo). Les r\u00e8gles de date restent appliqu\u00e9es par l'app ; ce mod\u00e8le lui fournit seulement les jours n\u00e9cessaires, d'o\u00f9 des chiffres identiques. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

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
    );
  