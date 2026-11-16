
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__disponibilite_article_neshu_depot_mensuel`
      
    
    cluster by company_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Taux de disponibilit\u00e9 mensuel des articles dans les d\u00e9p\u00f4ts Neshu : part des jours du mois o\u00f9 l'article \u00e9tait en stock (> 0).\n[COMMENT CONSTRUITE] Macro disponibilite_depot_mensuelle sur fct_supply_chain__stock_neshu filtr\u00e9 sur les d\u00e9p\u00f4ts (entity_type='company'), m\u00eame calcul que app_cockpit__neshu_disponibilite_depot_mensuel. Grille = jours d'extraction du d\u00e9p\u00f4t \u00d7 articles d\u00e9j\u00e0 vus dans ce d\u00e9p\u00f4t, depuis leur premi\u00e8re apparition ; disponible = stock_at_date > 0 ; taux = jours_disponibles / jours_observes. mois = date_trunc \u00e0 month.\n[GRAIN] 1 ligne par (mois, d\u00e9p\u00f4t, article).\n[NOTES] L'extraction ERP n'envoie pas les stocks nuls : un jour sans ligne est une rupture, et un article \u00e0 0 tout le mois reste dans le calcul (0 %). Les jours ant\u00e9rieurs \u00e0 la premi\u00e8re apparition de l'article dans le d\u00e9p\u00f4t ne comptent pas ; un jour o\u00f9 tout le d\u00e9p\u00f4t manque est une panne d'extraction, exclue. Le mois en cours est partiel (taux \u00e0 interpr\u00e9ter avec jours_observes). Cl\u00e9 m\u00e9tier = product_code ; entity_name/entity_code/product_name = libell\u00e9s de la derni\u00e8re ligne connue \u00e0 la fin du mois.\n"""
    )
    as (
      

-- Règle de la macro disponibilite_depot_mensuelle, la même que pour l'application Cockpit Supply.
with disponibilite as (
    

with photo as (
    select
        snapshot_date,
        id_entity,
        product_code,
        entity_code,
        entity_name,
        product_name,
        stock_at_date
    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__stock_neshu`
    where entity_type = 'company'
),

jour_depot as (
    select distinct
        id_entity,
        snapshot_date
    from photo
),

premiere_apparition as (
    select
        id_entity,
        product_code,
        min(snapshot_date) as premier_jour
    from photo
    group by id_entity, product_code
),

grille as (
    select
        j.snapshot_date,
        a.id_entity,
        a.product_code,
        -- Libellés de la dernière ligne connue, reportés sur les jours sans ligne.
        last_value(p.entity_code ignore nulls) over serie as entity_code,
        last_value(p.entity_name ignore nulls) over serie as entity_name,
        last_value(p.product_name ignore nulls) over serie as product_name,
        coalesce(p.stock_at_date > 0, false) as is_disponible
    from premiere_apparition as a
    inner join jour_depot as j
        on
            a.id_entity = j.id_entity
            and a.premier_jour <= j.snapshot_date
    left join photo as p
        on
            a.id_entity = p.id_entity
            and a.product_code = p.product_code
            and j.snapshot_date = p.snapshot_date
    window serie as (
        partition by a.id_entity, a.product_code
        order by j.snapshot_date
        rows between unbounded preceding and current row
    )
),

mensuel as (
    select
        date_trunc(snapshot_date, month) as mois,
        id_entity as company_id,
        product_code,
        any_value(entity_code having max snapshot_date) as entity_code,
        any_value(entity_name having max snapshot_date) as entity_name,
        any_value(product_name having max snapshot_date) as product_name,
        count(*) as jours_observes,
        countif(is_disponible) as jours_disponibles
    from grille
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
from mensuel

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
    taux_disponibilite_pct
from disponibilite
    );
  