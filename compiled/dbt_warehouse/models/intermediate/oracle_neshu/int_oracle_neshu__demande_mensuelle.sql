

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

),

demande_rattachee as (

    -- Rattachement territoire réappro : Strasbourg (118) → Lyon (116), qui le réapprovisionne.
    -- La demande de Strasbourg est ainsi additionnée à celle de Lyon (macro partagée avec le
    -- stock/encours du point de commande). Les autres dépôts sont inchangés ; Bordeaux (120)
    -- reste distinct et sera exclu en aval. Cf. macro neshu_depot_reappro.
    select
        
-- Rattachement des dépôts au pilotage du réapprovisionnement NESHU (forecast / point de commande).
--
-- Strasbourg (118) est réapprovisionné DEPUIS Lyon (116) : il ne passe quasiment aucune commande
-- fournisseur en direct (vérifié en données : 4 lignes sur tout l'historique). Sa demande et son
-- stock doivent donc remonter sur Lyon, qui commande au fournisseur pour le territoire Lyon +
-- Strasbourg puis réexpédie en interne. On rattache donc 118 → 116.
--
-- Bordeaux (120) commande de façon indépendante (Distrilog) et n'est PAS remappé : il est simplement
-- exclu du pilotage en aval (filtre sur le périmètre 114/116/364 = Rungis / Lyon(+Strasbourg) /
-- Marseille), puisqu'après remap il reste le seul id hors de ce périmètre.
--
-- Macro partagée par le socle de demande (int_oracle_neshu__demande_mensuelle) et par les CTE stock
-- et encours du point de commande, pour que le rattachement reste cohérent sur les trois flux.
case when company_id = 118 then 116 else company_id end
 as company_id,
        product_id,
        product_code,
        demande_mois,
        quantity
    from demande_unifiee

)

select
    company_id,
    product_id,
    product_code,
    demande_mois,
    sum(quantity) as quantite_demandee,
    count(*) as nb_mouvements
from demande_rattachee
group by company_id, product_id, product_code, demande_mois