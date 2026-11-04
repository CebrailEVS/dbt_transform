
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_disponibilite_depot_mensuel`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Disponibilit\u00e9 mensuelle des articles dans les d\u00e9p\u00f4ts Neshu, tel que lu par l'application Cockpit Supply.\n[COMMENT CONSTRUITE] Agr\u00e9gat mensuel de int_oracle_neshu__stock_theorique sur les d\u00e9p\u00f4ts : jours observ\u00e9s, jours sans rupture, taux en % ; code d\u00e9p\u00f4t du dernier jour du mois. M\u00eame calcul que fct_supply_chain__disponibilite_article_neshu_depot_mensuel.\n[GRAIN] 1 ligne par (mois, d\u00e9p\u00f4t, article).\n[NOTES] lue via get_disponibilite ; \u00e9crans : _conformite, accueil, groupe, neshu_pilotage, neshu_prevision, neshu_stocks, neshu_terrain, supply, v2_nav. Les r\u00e8gles de l'app (statuts, p\u00e9riodes, exclusions) restent appliqu\u00e9es par l'app ; les filtres repris ici sont identiques et sans perte. R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Disponibilité mensuelle des articles dans les dépôts Neshu, pour Cockpit Supply, construite depuis
-- l'intermediate : part des jours observés où l'article n'est pas en rupture. Libellés du dernier jour du mois.
with mensuel as (
    select
        date_trunc(date(date_system), month) as mois,
        id_entity as company_id,
        product_code,
        any_value(entity_code having max date_system) as entity_code,
        count(distinct date(date_system)) as jours_observes,
        count(distinct case when is_out_of_stock = false then date(date_system) end) as jours_disponibles
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__stock_theorique`
    where entity_type = 'company'
    group by 1, 2, 3
)

select
    mois,
    company_id,
    entity_code,
    product_code,
    round(safe_divide(jours_disponibles, jours_observes) * 100, 1) as taux_disponibilite_pct
from mensuel
    );
  