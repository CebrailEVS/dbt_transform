
  
    

    create or replace table `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__general_calendrier_ouvre`
      
    
    

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Calendrier des jours ouvr\u00e9s de Cockpit Supply : lundi \u00e0 vendredi hors jours f\u00e9ri\u00e9s, pour la France m\u00e9tropolitaine et pour l'Alsace-Moselle (Vendredi saint et 26 d\u00e9cembre en plus). Sert aux d\u00e9lais en jours ouvr\u00e9s et aux dur\u00e9es hors week-ends et jours f\u00e9ri\u00e9s.\n[COMMENT CONSTRUITE] Toutes les dates des ann\u00e9es couvertes par ref_general__feries_metropole, crois\u00e9es avec les deux calendriers ; jours f\u00e9ri\u00e9s de ref_general__feries_metropole, plus ref_general__feries_alsace_moselle pour l'Alsace-Moselle.\n[GRAIN] 1 ligne par (calendrier, jour).\n[NOTES] Lu par les d\u00e9lais de traitement des commandes (ISO). Au-del\u00e0 de la derni\u00e8re ann\u00e9e du seed m\u00e9tropole, aucune ligne : prolonger le seed avant. R\u00e9serv\u00e9 \u00e0 l'application : pas de rapport Power BI dessus.\n"""
    )
    as (
      

-- Jours ouvrés de Cockpit Supply, par calendrier : lundi à vendredi hors jours fériés. Alsace-Moselle =
-- fériés de métropole + ceux du seed Alsace-Moselle. Bornes = années couvertes par le seed métropole :
-- au-delà, un jour férié serait compté ouvré.
with bornes as (
    select
        date(min(annee), 1, 1) as premier_jour,
        date(max(annee), 12, 31) as dernier_jour
    from `evs-datastack-prod`.`prod_reference`.`ref_general__feries_metropole`
),

jours as (
    select jour
    from bornes
    cross join unnest(generate_date_array(bornes.premier_jour, bornes.dernier_jour)) as jour
),

feries_sources as (
    select
        'METROPOLE' as calendrier,
        date_ferie,
        nom_jour_ferie
    from `evs-datastack-prod`.`prod_reference`.`ref_general__feries_metropole`

    union all

    select
        'ALSACE_MOSELLE' as calendrier,
        date_ferie,
        nom_jour_ferie
    from `evs-datastack-prod`.`prod_reference`.`ref_general__feries_metropole`

    union all

    select
        'ALSACE_MOSELLE' as calendrier,
        date_ferie,
        nom_jour_ferie
    from `evs-datastack-prod`.`prod_reference`.`ref_general__feries_alsace_moselle`
),

-- Deux fériés peuvent tomber le même jour (Ascension et 1er mai en 2008) : un jour, une ligne
feries as (
    select
        calendrier,
        date_ferie,
        string_agg(nom_jour_ferie, ', ' order by nom_jour_ferie) as nom_jour_ferie
    from feries_sources
    group by calendrier, date_ferie
),

calendriers as (
    select calendrier
    from unnest(['METROPOLE', 'ALSACE_MOSELLE']) as calendrier
)

select
    c.calendrier,
    j.jour,
    f.nom_jour_ferie,
    extract(dayofweek from j.jour) not in (1, 7) and f.date_ferie is null as is_jour_ouvre
from jours as j
cross join calendriers as c
left join feries as f
    on
        c.calendrier = f.calendrier
        and j.jour = f.date_ferie
    );
  