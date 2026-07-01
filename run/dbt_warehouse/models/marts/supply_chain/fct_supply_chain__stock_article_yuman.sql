
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_article_yuman`
      
    partition by stock_date
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Stock journalier des articles Yuman au niveau **article** (tous magasins agr\u00e9g\u00e9s), avec le drapeau de rupture. R\u00e9pond \u00e0 \u00ab quels articles sont en rupture totale \u00bb et sert de base \u00e0 un futur taux de disponibilit\u00e9 article.\n[COMMENT CONSTRUITE] Agr\u00e9gation de stg_yuman_gcs__stock_theorique par (reference, date). Chaque emplacement (nom_du_stock) est class\u00e9 D\u00c9P\u00d4T (libell\u00e9 contenant 'DEPOT', 4 sites) ou TECHNICIEN (van, actif 'ST - \u2026' ou inactif). total_quantity = somme tous emplacements ; depot_quantity / technicien_quantity = sommes ventil\u00e9es ; is_out_of_stock = total nul (absent partout) ; is_out_of_stock_depot = total d\u00e9p\u00f4t nul (indisponible au r\u00e9appro). Contrairement au mart par magasin, AUCUN filtre sur nom_du_stock : les lignes de rupture (quantite = 0, sans emplacement en source) sont conserv\u00e9es.\n[GRAIN] 1 ligne par (reference, stock_date).\n[NOTES] \u26a0\ufe0f Sp\u00e9cificit\u00e9 Yuman : la rupture n'existe qu'au niveau **article**, pas par emplacement (l'export ne localise pas une rupture \u2014 contrairement \u00e0 Neshu o\u00f9 stock_neshu la porte par entit\u00e9). Les emplacements m\u00ealent **d\u00e9p\u00f4ts ET techniciens** : un article pr\u00e9sent uniquement chez un technicien est en stock globalement (is_out_of_stock = false) mais en rupture d\u00e9p\u00f4t (is_out_of_stock_depot = true) \u2192 pour le r\u00e9appro, se fier \u00e0 is_out_of_stock_depot ; is_out_of_stock_technicien donne la vue terrain (vans). Invariant : is_out_of_stock = is_out_of_stock_depot AND is_out_of_stock_technicien. La distinction d\u00e9p\u00f4t/technicien n'est possible que sur les lignes en stock (quantite > 0, seules \u00e0 porter un emplacement) ; une rupture totale (quantite = 0) arrive sans emplacement et compte comme vide des deux c\u00f4t\u00e9s. Classification d\u00e9p\u00f4t/technicien = heuristique sur le libell\u00e9 ('DEPOT'). Le d\u00e9tail par emplacement reste dans fct_supply_chain__stock_yuman. La d\u00e9signation est r\u00e9solue au grain r\u00e9f\u00e9rence en \u00e9cartant les libell\u00e9s-marqueur (voir macro yuman_is_marqueur_designation).\n"""
    )
    as (
      

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
        '64ef5529-e432-4bf0-9445-78a66cb482b0' as dbt_invocation_id

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
    );
  