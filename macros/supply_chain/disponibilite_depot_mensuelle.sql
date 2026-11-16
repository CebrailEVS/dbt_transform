{% macro disponibilite_depot_mensuelle(stock_relation) %}
{#
    Disponibilité mensuelle des articles dans les dépôts, depuis une photo de stock quotidienne
    (une ligne par jour, dépôt et article). L'extraction ERP n'envoie pas les articles à stock nul :
    la grille croise donc les jours d'extraction de chaque dépôt avec les articles déjà vus dans ce
    dépôt, à partir de leur première apparition. Un jour sans ligne est une rupture ; disponible =
    stock strictement positif, un stock négatif est donc une rupture. Un jour où tout le dépôt manque
    est une panne d'extraction : il n'entre pas dans la grille.
    Partagée par les modèles de l'app et les marts, pour qu'ils gardent exactement le même calcul.
#}
with photo as (
    select
        snapshot_date,
        id_entity,
        product_code,
        entity_code,
        entity_name,
        product_name,
        stock_at_date
    from {{ stock_relation }}
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
{% endmacro %}
