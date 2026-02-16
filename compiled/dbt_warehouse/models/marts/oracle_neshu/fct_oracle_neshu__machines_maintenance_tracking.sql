-- fct_oracle_neshu__machines_maintenance_tracking.sql


with machines_base as (
    select * from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__machines_yuman_maintenance_base`
),

-- INTERVENTIONS YUMAN (toutes, pas de filtre ici)
all_workorders as (
    select
        wd.demand_id,
        wd.workorder_id,
        wd.material_id,
        wd.demand_status,
        wdc.demand_category_name,
        wo.date_planned,
        wo.date_done,
        wo.workorder_type,
        wo.workorder_status
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorder_demands` as wd
    left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorder_demands_categories` as wdc
        on wd.demand_category_id = wdc.demand_category_id
    left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorders` as wo
        on wd.workorder_id = wo.workorder_id
),

-- ENRICHISSEMENT : Ajouter les interventions AUX machines (LEFT JOIN)
workorder_enrichi as (
    select
        mb.*,
        wo.demand_id,
        wo.workorder_id,
        wo.demand_status,
        wo.demand_category_name,
        wo.date_planned,
        wo.date_done,
        wo.workorder_type,
        wo.workorder_status
    from machines_base as mb
    left join all_workorders as wo
        on mb.material_id = wo.material_id
),

-- LISTE INTERVENTION PREV DLOG
intervention_prev_dlog as (
    select
        concat('NESH_', device_code) as device_code,
        max(task_end_date) as last_preventive_date
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__inter_techinique_tasks`
    where
        dc04 = 'DC0402'
        and task_status_code = 'FAIT'
    group by device_code
),

-- AJOUT PREV DLOG DANS LA LISTE DES INTERVENTION YUMAN
material_enrichi as (
    select
        we.*,
        ipd.last_preventive_date
    from workorder_enrichi as we
    left join intervention_prev_dlog as ipd
        on we.material_serial_number = ipd.device_code
),

-- CALCUL DU RETARD PAR MACHINE
calcul_retard as (
    select
        device_id,
        material_id,
        device_code,
        material_serial_number,
        last_installation_date,
        device_name,
        company_code,
        company_name,
        client_code,
        client_name,
        client_category,
        site_postal_code,
        demand_status,
        date_planned,
        material_created_at,

        -- Déterminer la dernière préventive de la source 1 (Yuman)
        max(
            case
                when (
                    workorder_type = 'Preventive'
                    or demand_category_name = 'PREVENTIVE PROG - NESHU'
                )
                and workorder_status = 'Closed'
                    then date_done
            end
        ) over (partition by material_id)
            as derniere_preventive_source1,

        -- Récupérer la préventive externe (source 2 - DLOG)
        max(last_preventive_date) over (
            partition by material_id
        ) as derniere_preventive_source2,

        -- Date du jour
        current_timestamp() as today

    from material_enrichi
),

-- LOGIQUE PRINCIPALE DE CALCUL
retard_final as (
    select distinct
        device_id,
        material_id,
        device_code,
        material_serial_number,
        last_installation_date,
        device_name,
        company_code,
        company_name,
        client_code,
        client_name,
        client_category,
        site_postal_code,
        demand_status,
        date_planned,
        material_created_at,

        -- CAS 1: Machine installée depuis moins de 13 mois
        case
            when
                last_installation_date > timestamp_sub(today, interval 395 day)
                then false

            -- CAS 2: Machine ancienne sans aucune préventive
            when
                derniere_preventive_source1 is null
                and derniere_preventive_source2 is null
                then true

            -- CAS 3: Machine avec préventives - vérifier si retard > 365 jours
            when
                greatest(
                    coalesce(
                        derniere_preventive_source1,
                        timestamp('1900-01-01')
                    ),
                    coalesce(
                        derniere_preventive_source2,
                        timestamp('1900-01-01')
                    )
                ) < timestamp_sub(today, interval 365 day)
                then true
            else false
        end as retard_bol,

        -- CALCUL DU DÉLAI
        case
            -- CAS 1: Machine récente (< 13 mois)
            when
                last_installation_date > timestamp_sub(today, interval 395 day)
                then
                    timestamp_diff(
                        timestamp_add(
                            last_installation_date, interval 395 day
                        ),
                        today,
                        day
                    )

            -- CAS 2: Aucune préventive
            when
                derniere_preventive_source1 is null
                and derniere_preventive_source2 is null
                then
                    -timestamp_diff(
                        today,
                        timestamp_add(
                            last_installation_date, interval 365 day
                        ),
                        day
                    )

            -- CAS 3: Avec préventives
            when
                timestamp_diff(
                    today,
                    greatest(
                        coalesce(
                            derniere_preventive_source1,
                            timestamp('1900-01-01')
                        ),
                        coalesce(
                            derniere_preventive_source2,
                            timestamp('1900-01-01')
                        )
                    ),
                    day
                ) > 365
                then
                    -(
                        timestamp_diff(
                            today,
                            greatest(
                                coalesce(
                                    derniere_preventive_source1,
                                    timestamp('1900-01-01')
                                ),
                                coalesce(
                                    derniere_preventive_source2,
                                    timestamp('1900-01-01')
                                )
                            ),
                            day
                        ) - 365
                    )
            else
                365 - timestamp_diff(
                    today,
                    greatest(
                        coalesce(
                            derniere_preventive_source1,
                            timestamp('1900-01-01')
                        ),
                        coalesce(
                            derniere_preventive_source2,
                            timestamp('1900-01-01')
                        )
                    ),
                    day
                )
        end as retard_delai,

        -- SOURCE DE LA DERNIÈRE PRÉVENTIVE
        case
            when
                last_installation_date > timestamp_sub(today, interval 395 day)
                then 'aucune'
            when
                derniere_preventive_source1 is null
                and derniere_preventive_source2 is null
                then 'aucune'
            when
                derniere_preventive_source2 is null
                or (
                    derniere_preventive_source1 is not null
                    and derniere_preventive_source1 > derniere_preventive_source2
                )
                then 'yuman'
            else
                'dlog'
        end as source_last_preventive,

        today

    from calcul_retard
),

-- DÉDUPLICATION: Garder une seule ligne par machine
deduplique as (
    select
        * except (today),
        row_number() over (
            partition by material_serial_number
            order by
                retard_bol asc,                -- Les machines non en retard d'abord (FALSE avant TRUE)
                material_created_at desc,      -- Si les 2 non en retard : la plus récente
                retard_delai asc               -- Si les 2 en retard : le plus grand délai (négatif donc ASC)
        ) as rn
    from retard_final
),

-- RÉSULTAT RETARD DEDUPLIQUÉ
resultat_retard as (
    select
        device_id,
        material_id,
        device_code,
        material_serial_number,
        last_installation_date,
        device_name,
        company_code,
        company_name,
        client_code,
        client_name,
        client_category,
        site_postal_code,
        retard_bol,
        retard_delai,
        source_last_preventive,
        material_created_at
    from deduplique
    where rn = 1
),

-- DEMANDES D'INTERVENTION YUMAN OUVERTES & PLANIFIÉES
di_data as (
    select
        wd.material_id,
        wo.date_planned,
        wd.created_at as demand_created_at,  -- Récupérer la date de création
        case
            when wd.demand_status = 'Open' then 'Ouvert'
            when wo.workorder_status = 'Scheduled' then 'Planifie'
            else 'Aucune'
        end as status_inter
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorder_demands` as wd
    left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorder_demands_categories` as wdc
        on wd.demand_category_id = wdc.demand_category_id
    left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorders` as wo
        on wd.workorder_id = wo.workorder_id
    where
        wdc.demand_category_name = 'PREVENTIVE PROG - NESHU'
        and (
            wd.demand_status = 'Open'
            or wo.workorder_status = 'Scheduled'
        )
),

-- JOIN INTERMÉDIAIRE
joined_data as (
    select
        rr.device_id,
        rr.device_code,
        rr.device_name,
        rr.company_code,
        rr.company_name,
        rr.last_installation_date,
        rr.material_id,
        rr.material_serial_number,
        rr.client_code,
        rr.client_name,
        rr.client_category,
        rr.site_postal_code,
        rr.retard_bol,
        rr.retard_delai,
        rr.source_last_preventive,
        rr.material_created_at,
        di.status_inter,
        di.date_planned,
        di.demand_created_at
    from resultat_retard as rr
    left join di_data as di
        on rr.material_id = di.material_id
),

-- DÉDUPLICATION PAR DEVICE_ID (garder la DI la plus récente)
deduplicated as (
    select
        *,
        row_number() over (
            partition by device_id
            order by demand_created_at desc nulls last  -- Les plus récentes d'abord, NULL à la fin
        ) as rn
    from joined_data
),

-- ENRICHISSEMENT FINAL
final as (
    select
        device_id,
        device_code,
        device_name,
        company_code,
        company_name,
        last_installation_date as device_last_installation_date,
        material_id,
        material_serial_number,
        client_code,
        client_name,
        client_category,
        site_postal_code,
        retard_bol,
        retard_delai,
        source_last_preventive,
        coalesce(status_inter, 'Aucune') as status_inter,
        date_planned,
        material_created_at,

        -- Métadonnées dbt
        current_timestamp() as dbt_updated_at,
        '64a4dc76-772d-4d6a-b4b0-06492673b8bc' as dbt_invocation_id  -- noqa: TMP

    from deduplicated
    where rn = 1  -- Ne garder qu'une ligne par device_id
)

select * from final