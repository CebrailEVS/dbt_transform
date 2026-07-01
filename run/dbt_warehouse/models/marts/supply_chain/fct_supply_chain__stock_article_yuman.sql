
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_article_yuman`
      
    partition by stock_date
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Stock journalier des articles Yuman au niveau **article** (tous magasins agr\u00e9g\u00e9s), avec le drapeau de rupture. R\u00e9pond \u00e0 \u00ab quels articles sont en rupture totale \u00bb et sert de base \u00e0 un futur taux de disponibilit\u00e9 article.\n[COMMENT CONSTRUITE] Agr\u00e9gation de stg_yuman_gcs__stock_theorique par (reference, date). Chaque emplacement (nom_du_stock) est class\u00e9 D\u00c9P\u00d4T (libell\u00e9 contenant 'DEPOT', 4 sites) ou TECHNICIEN (van, actif 'ST - \u2026' ou inactif). total_quantity = somme tous emplacements ; depot_quantity / technicien_quantity = sommes ventil\u00e9es ; is_out_of_stock = total nul (absent partout) ; is_out_of_stock_depot = total d\u00e9p\u00f4t nul (indisponible au r\u00e9appro). Contrairement au mart par magasin, AUCUN filtre sur nom_du_stock : les lignes de rupture (quantite = 0, sans emplacement en source) sont conserv\u00e9es.\n[GRAIN] 1 ligne par (reference, stock_date).\n[NOTES] \u26a0\ufe0f Sp\u00e9cificit\u00e9 Yuman : la rupture n'existe qu'au niveau **article**, pas par emplacement (l'export ne localise pas une rupture \u2014 contrairement \u00e0 Neshu o\u00f9 stock_neshu la porte par entit\u00e9). Les emplacements m\u00ealent **d\u00e9p\u00f4ts ET techniciens** : un article pr\u00e9sent uniquement chez un technicien est en stock globalement (is_out_of_stock = false) mais en rupture d\u00e9p\u00f4t (is_out_of_stock_depot = true) \u2192 pour le r\u00e9appro, se fier \u00e0 is_out_of_stock_depot ; is_out_of_stock_technicien donne la vue terrain (vans). Invariant : is_out_of_stock = is_out_of_stock_depot AND is_out_of_stock_technicien. La distinction d\u00e9p\u00f4t/technicien n'est possible que sur les lignes en stock (quantite > 0, seules \u00e0 porter un emplacement) ; une rupture totale (quantite = 0) arrive sans emplacement et compte comme vide des deux c\u00f4t\u00e9s. Classification d\u00e9p\u00f4t/technicien = heuristique sur le libell\u00e9 ('DEPOT'). Le d\u00e9tail par emplacement reste dans fct_supply_chain__stock_yuman. ~181 couples reference/date ont 2 d\u00e9signations en source \u2192 min() retenu.\n"""
    )
    as (
      

-- Un emplacement de stock Yuman (nom_du_stock) est soit un DÉPÔT (libellé contenant 'DEPOT',
-- 4 sites), soit un TECHNICIEN (van, actif 'ST - …' ou inactif 'Stock technicien - …').
-- La rupture (quantite = 0) arrive en source sans emplacement (nom_du_stock NULL).

with article_stock as (
    select
        -- Grain
        date(export_date) as stock_date,
        reference,

        -- Attribut metier (min() = choix deterministe : ~181 couples reference/date
        -- portent 2 designations distinctes en source)
        min(designation) as designation,

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
        '797d9f7e-cb98-48ef-a510-093856213dd5' as dbt_invocation_id

    from `evs-datastack-prod`.`prod_staging`.`stg_yuman_gcs__stock_theorique`
    where reference is not null
    group by stock_date, reference
)

select *
from article_stock
    );
  