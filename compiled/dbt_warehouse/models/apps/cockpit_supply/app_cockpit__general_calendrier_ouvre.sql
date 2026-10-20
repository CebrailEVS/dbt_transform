

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