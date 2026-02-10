{{ config(
    materialized='table',
    description='Modèle intermediate construisant la clé de facturation des interventions techniques selon typologie, machine, mini-prev et zone montagne'
) }}

with inter as (
    select
        *,
        case
            when pickup_date < creation_date then creation_date
            else pickup_date
        end as date_creation_delai
    from {{ ref('stg_nesp_tech__interventions') }}
    where
        etat_intervention in ('terminée signée', 'signature différée')
        and agency in ('evs idf', 'evs', 'evs paris', 'evs paris 2')
),

feries as (
    select cast(date_ferie as date) as date_ferie
    from {{ ref('ref_general__feries_metropole') }}
),

exploded as (
    select
        i.n_planning,
        i.type,
        i.code_machine,
        i.date_creation_delai,
        i.date_heure_debut,
        i.date_heure_fin,
        d as cal_date,
        f.date_ferie
    from inter as i
    cross join
        unnest(
            generate_date_array(
                least(date(i.date_creation_delai), date(i.date_heure_debut)),
                date(i.date_heure_fin)
            )
        ) as d
    left join feries as f
        on d = f.date_ferie
),

delais as (
    select
        n_planning,
        type,
        code_machine,

        -- Jours ouvres avant debut
        countif(
            extract(dayofweek from cal_date) not in (1, 7)
            and date_ferie is null
            and cal_date <= date(date_heure_debut)
        ) as delai_jours_debut,

        -- Jours ouvres avant fin
        countif(
            extract(dayofweek from cal_date) not in (1, 7)
            and date_ferie is null
            and cal_date <= date(date_heure_fin)
        ) as delai_jours_fin,

        -- Heures ouvrees jusqu'au debut
        sum(case
            when
                extract(dayofweek from cal_date) in (1, 7)
                or date_ferie is not null
                or cal_date > date(date_heure_debut)
                then 0
            else
                timestamp_diff(
                    least(
                        date_heure_debut,
                        timestamp_add(timestamp(cal_date), interval 1 day)
                    ),
                    greatest(date_creation_delai, timestamp(cal_date)),
                    second
                ) / 3600
        end) as delai_heures_debut,

        -- Heures ouvrees jusqu'a la fin
        sum(case
            when
                extract(dayofweek from cal_date) in (1, 7)
                or date_ferie is not null
                or cal_date > date(date_heure_fin)
                then 0
            else
                timestamp_diff(
                    least(
                        date_heure_fin,
                        timestamp_add(timestamp(cal_date), interval 1 day)
                    ),
                    greatest(date_creation_delai, timestamp(cal_date)),
                    second
                ) / 3600
        end) as delai_heures_fin

    from exploded
    group by n_planning, type, code_machine
),

final as (
    select
        *,

        delai_jours_fin - delai_jours_debut as delai_traitement_jours,

        -- Categorisation SLA fin
        case
            when delai_jours_fin <= 1 then 'J+0'
            when delai_jours_fin = 2 then 'J+1'
            when delai_jours_fin = 3 then 'J+2'
            when delai_jours_fin = 4 then 'J+3'
            else 'J++'
        end as type_delai_fin,

        -- Categorisation SLA debut
        case
            when delai_jours_debut <= 1 then 'J+0'
            when delai_jours_debut = 2 then 'J+1'
            when delai_jours_debut = 3 then 'J+2'
            when delai_jours_debut = 4 then 'J+3'
            else 'J++'
        end as type_delai_debut,

        -- Flag bonus
        (
            delai_jours_fin <= 2
            and type = '5'
            and code_machine not like 'ag%'
        ) as delai_bonus_bool,

        -- Montant bonus
        case
            when
                delai_jours_fin <= 2
                and type = '5'
                and code_machine not like 'ag%'
                then 15
            else 0
        end as delai_bonus_valeur

    from delais
)

select
    -- Info Intervention
    n_planning,
    type,
    code_machine,

    -- Info delais debut
    delai_jours_debut,
    delai_heures_debut,
    type_delai_debut,

    -- Info delais fin
    delai_jours_fin,
    delai_heures_fin,
    type_delai_fin,
    delai_traitement_jours,

    -- Info Bonus Curative
    delai_bonus_bool,
    delai_bonus_valeur

from final
