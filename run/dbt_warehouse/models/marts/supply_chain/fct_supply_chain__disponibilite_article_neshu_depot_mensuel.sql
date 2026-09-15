
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__disponibilite_article_neshu_depot_mensuel`
      
    
    cluster by company_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Taux de disponibilit\u00e9 mensuel des articles dans les d\u00e9p\u00f4ts Neshu : part des jours du mois o\u00f9 l'article \u00e9tait en stock (> 0).\n[COMMENT CONSTRUITE] Agr\u00e9gation mensuelle de fct_supply_chain__stock_neshu filtr\u00e9 sur les d\u00e9p\u00f4ts (entity_type='company'). jours_observes = nb de jours o\u00f9 le couple d\u00e9p\u00f4t/article est r\u00e9f\u00e9renc\u00e9 dans les donn\u00e9es ; jours_disponibles = nb de jours r\u00e9f\u00e9renc\u00e9s o\u00f9 is_out_of_stock=false. taux = jours_disponibles / jours_observes. mois = date_trunc \u00e0 month.\n[GRAIN] 1 ligne par (mois, d\u00e9p\u00f4t, article).\n[NOTES] Le taux est calcul\u00e9 depuis le 1er r\u00e9f\u00e9rencement de l'article dans le d\u00e9p\u00f4t (d\u00e9nominateur = jours r\u00e9f\u00e9renc\u00e9s), pas sur le mois calendaire : un article arriv\u00e9 en cours de mois et jamais en rupture affiche 100 %, sans p\u00e9nalit\u00e9 pour les jours ant\u00e9rieurs \u00e0 son arriv\u00e9e. Le mois en cours est partiel (taux \u00e0 interpr\u00e9ter avec jours_observes). Cl\u00e9 m\u00e9tier = product_code ; entity_name/entity_code/product_name refl\u00e8tent le libell\u00e9 du snapshot le plus r\u00e9cent du mois (un article peut \u00eatre renomm\u00e9 en cours de mois sans changer de code).\n"""
    )
    as (
      

with monthly as (
    select
        date_trunc(date(date_system), month) as mois,
        id_entity as company_id,
        product_code,
        any_value(entity_code having max date_system) as entity_code,
        any_value(entity_name having max date_system) as entity_name,
        any_value(product_name having max date_system) as product_name,
        count(distinct date(date_system)) as jours_observes,
        count(distinct case
            when is_out_of_stock = false then date(date_system)
        end) as jours_disponibles
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_neshu`
    where entity_type = 'company'
    group by 1, 2, 3
)

select
    mois,
    company_id,
    entity_code,
    entity_name,
    product_code,
    product_name,
    jours_observes,
    jours_disponibles,
    round(safe_divide(jours_disponibles, jours_observes) * 100, 1) as taux_disponibilite_pct
from monthly
    );
  