
  
    

    create or replace table `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__demande_mensuelle`
      
    
    cluster by company_id, product_code

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Socle de demande NESHU : consommation mensuelle par d\u00e9p\u00f4t et par article. Mati\u00e8re premi\u00e8re commune \u00e0 la cha\u00eene de pr\u00e9vision/r\u00e9approvisionnement (classification ABC \u00d7 Syntetos-Boylan, forecast, point de commande). \"Demande\" = ce qui sort r\u00e9ellement du d\u00e9p\u00f4t.\n[COMMENT CONSTRUITE] Union de deux flux, statuts FAIT/VALIDE, agr\u00e9g\u00e9e par mois : - sorties clients : int_oracle_neshu__livraison_tasks (source = d\u00e9p\u00f4t, toujours COMPANY) ; - chargements v\u00e9hicules : int_oracle_neshu__livraison_interne_tasks (source COMPANY,\n  destination \u2260 COMPANY pour neutraliser les transferts inter-d\u00e9p\u00f4ts).\nQuantit\u00e9 = quantity (d\u00e9conditionn\u00e9e en unit\u00e9s de base). Reprend la d\u00e9finition de demande valid\u00e9e avec la logistique sur le mart de couverture. Les lignes \u00e0 quantit\u00e9 nulle (mouvements fant\u00f4mes) sont exclues ; aucune quantit\u00e9 n\u00e9gative n'existe en source, donc la demande agr\u00e9g\u00e9e est toujours strictement positive.\n[GRAIN] 1 ligne par (company_id, product_id, product_code, demande_mois). Repr\u00e9sentation *sparse* : seuls les mois avec demande existent (pas de ligne \u00e0 z\u00e9ro) ; la densification calendaire (pour l'ADI) est faite en aval, dans la classification.\n[NOTES] company_id = d\u00e9p\u00f4t source (product_source_id). Le p\u00e9rim\u00e8tre (Neshu, hors accessoires non-consommables, articles arr\u00eat\u00e9s) est appliqu\u00e9 en aval, pas ici : le socle reste la demande brute observ\u00e9e.\n"""
    )
    as (
      

-- Socle de demande NESHU : série mensuelle de consommation par dépôt et par article.
-- Reprend la définition de demande validée avec la logistique (mart couverture) :
--   demande = sorties clients (BL) + chargements des véhicules de tournée.
-- Grain : company_id (dépôt) x product x mois. Représentation sparse (pas de mois à zéro) :
-- la densification (spine calendaire pour l'ADI) est faite en aval, dans la classification.
-- Les lignes à quantité nulle (mouvements fantômes) sont exclues ; aucune quantité négative
-- n'existe en source, donc la demande agrégée est toujours strictement positive.

with sorties_clients as (

    -- Bons de livraison clients. La source est toujours un dépôt (product_source_type='COMPANY',
    -- vérifié : 100 % des lignes). Filtre explicite par robustesse.
    select
        product_source_id as company_id,
        product_id,
        product_code,
        date_trunc(date(task_start_date), month) as demande_mois,
        quantity
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__livraison_tasks`
    where
        task_status_code in ('FAIT', 'VALIDE')
        and product_source_type = 'COMPANY'
        -- exclut les lignes fantômes à quantité nulle (non-événements)
        and quantity > 0

),

sorties_vehicules as (

    -- Chargements des véhicules de tournée depuis un dépôt.
    -- On neutralise les transferts inter-dépôts (destination = COMPANY) : c'est une
    -- relocalisation de stock, pas de la demande.
    select
        product_source_id as company_id,
        product_id,
        product_code,
        date_trunc(date(task_start_date), month) as demande_mois,
        quantity
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__livraison_interne_tasks`
    where
        task_status_code in ('FAIT', 'VALIDE')
        and product_source_type = 'COMPANY'
        and product_destination_type != 'COMPANY'
        -- exclut les lignes fantômes à quantité nulle (non-événements)
        and quantity > 0

),

demande_unifiee as (

    select * from sorties_clients
    union all
    select * from sorties_vehicules

)

select
    company_id,
    product_id,
    product_code,
    demande_mois,
    sum(quantity) as quantite_demandee,
    count(*) as nb_mouvements
from demande_unifiee
group by company_id, product_id, product_code, demande_mois
    );
  