

-- Un emplacement de stock Yuman (nom_du_stock) est soit un DÉPÔT (libellé contenant 'DEPOT',
-- 4 sites), soit un TECHNICIEN (van, actif 'ST - …' ou inactif 'Stock technicien - …').
-- La rupture (quantite = 0) arrive en source sans emplacement (nom_du_stock NULL).

with reference_designation as (
    -- Désignation unique par référence (attribut article, invariant dans le temps).
    -- Une même référence porte parfois plusieurs libellés : on écarte les "marqueurs"
    -- (NE PAS UTILISER, OLD, Remplacé par...) et on retient le plus petit des libellés
    -- valides ; fallback sur min(tout) si la référence n'a QUE des marqueurs (jamais NULL).
    select
        reference,
        coalesce(
            min(case when not regexp_contains(upper(designation), r'NE PAS UTILISER|^OLD |REMPLAC.{0,4}PAR') then designation end),
            min(designation)
        ) as designation
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_gcs__stock_theorique`
    where reference is not null
    group by reference
),

article_stock as (
    select
        -- Grain
        date(export_date) as stock_date,
        reference,

        -- Disponibilite : global (tous emplacements), depot (reappro), technicien (terrain).
        -- Invariant : is_out_of_stock = is_out_of_stock_depot AND is_out_of_stock_technicien.
        sum(quantite) = 0 as is_out_of_stock,
        sum(case when (nom_du_stock like '%DEPOT%') then quantite else 0 end) = 0 as is_out_of_stock_depot,
        sum(case when not (nom_du_stock like '%DEPOT%') then quantite else 0 end) = 0
            as is_out_of_stock_technicien,

        -- Mesures : quantites agregees
        sum(quantite) as total_quantity,
        sum(case when (nom_du_stock like '%DEPOT%') then quantite else 0 end) as depot_quantity,
        sum(case when not (nom_du_stock like '%DEPOT%') then quantite else 0 end) as technicien_quantity,

        -- Mesures : nb d'emplacements approvisionnes (quantite > 0)
        count(distinct case
            when (nom_du_stock like '%DEPOT%') and quantite > 0 then nom_du_stock
        end) as nb_depots_en_stock,
        count(distinct case
            when not (nom_du_stock like '%DEPOT%') and quantite > 0 then nom_du_stock
        end) as nb_techniciens_en_stock,

        -- Metadonnees dbt
        current_timestamp() as dbt_updated_at,
        '686fc251-6ad8-4f1c-a9fa-2e8ba0fa76de' as dbt_invocation_id

    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_gcs__stock_theorique`
    where reference is not null
    group by stock_date, reference
)

select
    a.stock_date,
    a.reference,
    rd.designation,
    a.is_out_of_stock,
    a.is_out_of_stock_depot,
    a.is_out_of_stock_technicien,
    a.total_quantity,
    a.depot_quantity,
    a.technicien_quantity,
    a.nb_depots_en_stock,
    a.nb_techniciens_en_stock,
    a.dbt_updated_at,
    a.dbt_invocation_id
from article_stock as a
left join reference_designation as rd
    on a.reference = rd.reference