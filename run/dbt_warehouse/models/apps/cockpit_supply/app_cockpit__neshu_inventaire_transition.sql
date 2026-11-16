
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__neshu_inventaire_transition`
      
    
    cluster by entity_code, product_code

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Changements de date d'inventaire par entit\u00e9 Neshu et par article : chaque nouvel inventaire physique, avec le stock th\u00e9orique de la veille. Alimente les indicateurs ISO \u00ab fr\u00e9quence des inventaires \u00bb et \u00ab fiabilit\u00e9 des stocks \u00bb de Cockpit Supply.\n[COMMENT CONSTRUITE] Historique journalier de int_oracle_neshu__stock_theorique (lignes \u00e0 date_inventaire et date_system renseign\u00e9es, v\u00e9hicules marqu\u00e9s inactifs exclus comme dans l'app), ordonn\u00e9 par date_system au sein de (entity_code, product_code) ; on garde la premi\u00e8re ligne de chaque s\u00e9rie et chaque ligne o\u00f9 date_inventaire change, avec la date d'inventaire et le stock th\u00e9orique de la ligne pr\u00e9c\u00e9dente (LAG). stock_veille lit la photo du jour pr\u00e9c\u00e9dent sur toutes les lignes de la s\u00e9rie : un article absent de cette photo y \u00e9tait \u00e0 0, Distrilog n'exportant pas les stocks nuls.\n[GRAIN] 1 ligne par (entity_code, product_code, date_system), restreinte aux premi\u00e8res lignes et aux changements de date d'inventaire.\n[NOTES] Reproduit exactement le calcul pandas de l'app (api/processus.py) qui exigeait tout l'historique journalier en m\u00e9moire : fr\u00e9quence = nombre de dates d'inventaire distinctes par entit\u00e9 et par mois ; fiabilit\u00e9 = \u00e9cart entre stock_at_date_precedent et stock_inventaire sur les lignes o\u00f9 is_premiere_ligne est faux. stock_at_date_precedent reprend la derni\u00e8re ligne connue de la s\u00e9rie, qui peut dater de plusieurs jours quand l'article \u00e9tait \u00e0 0 entre-temps ; stock_veille donne le stock syst\u00e8me r\u00e9el la veille du comptage. Un article remis \u00e0 0 par un comptage n'a pas de ligne ici : voir app_cockpit__neshu_inventaire_remise_zero. Liste des v\u00e9hicules hors motif ^V\\d+ (ARKEMA, STRASBOURG, RATP) reprise de la config de l'app (DESTINATIONS_VEHICULES_EXTRA). R\u00e9serv\u00e9 \u00e0 l'application.\n"""
    )
    as (
      

-- Jour de photo précédent : Distrilog n'exporte pas les stocks nuls, un article absent de la photo de la
-- veille y était donc à 0.
with photos as (
    select
        snapshot_date,
        lag(snapshot_date) over (order by snapshot_date) as snapshot_date_veille
    from (
        select distinct snapshot_date
        from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__stock_theorique`
    ) as jours
),

-- Stock de la veille lu sur toutes les lignes de la série, date d'inventaire renseignée ou non ;
-- NULL le premier jour de l'historique (veille inconnue).
serie as (
    select
        st.entity_code,
        st.product_code,
        st.date_system,
        st.date_inventaire,
        st.stock_at_date,
        st.stock_inventaire,
        st.is_vehicle_active,
        case
            when ph.snapshot_date_veille is null then null
            when lag(st.snapshot_date) over par_serie = ph.snapshot_date_veille
                then lag(st.stock_at_date) over par_serie
            else 0
        end as stock_veille
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__stock_theorique` as st
    inner join photos as ph on st.snapshot_date = ph.snapshot_date
    window par_serie as (partition by st.entity_code, st.product_code order by st.snapshot_date)
),

lignes as (
    select
        entity_code,
        product_code,
        date_system,
        date_inventaire,
        stock_at_date,
        stock_inventaire,
        stock_veille
    from serie
    where
        date_inventaire is not null
        and date_system is not null
        -- Même périmètre que l'app (_filter_vehicule_actif) : on écarte les véhicules
        -- marqués inactifs ; un état inconnu (NULL) est conservé.
        and not (
            (
                regexp_contains(entity_code, r'^V\d+')
                or entity_code in ('ARKEMA', 'STRASBOURG', 'RATP')
            )
            and coalesce(is_vehicle_active = false, false)
        )
),

avec_precedent as (
    select
        entity_code,
        product_code,
        date_system,
        date_inventaire,
        stock_at_date,
        stock_inventaire,
        stock_veille,
        lag(date_inventaire) over par_article as date_inventaire_precedente,
        lag(stock_at_date) over par_article as stock_at_date_precedent
    from lignes
    window par_article as (partition by entity_code, product_code order by date_system)
)

select
    date_system,
    entity_code,
    product_code,
    date_inventaire,
    date_inventaire_precedente,
    date_inventaire_precedente is null as is_premiere_ligne,
    stock_at_date_precedent,
    stock_veille,
    stock_inventaire,
    stock_at_date
from avec_precedent
where
    date_inventaire_precedente is null
    or date_inventaire != date_inventaire_precedente
    );
  