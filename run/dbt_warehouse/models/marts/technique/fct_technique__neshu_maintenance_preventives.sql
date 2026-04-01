
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_technique__neshu_maintenance_preventives`
      
    
    cluster by company_code, device_code

    
    OPTIONS(
      description="""Table de faits pour le suivi des maintenances pr\u00e9ventives des machines NESHU.\n\nCette table croise les donn\u00e9es Oracle (DLOG) et Yuman pour :\n- Identifier les machines en retard de maintenance pr\u00e9ventive\n- Calculer le d\u00e9lai de retard ou d'avance par rapport \u00e0 l'\u00e9ch\u00e9ance annuelle\n- Suivre le statut des demandes d'intervention ouvertes ou planifi\u00e9es\n\n**R\u00e8gles m\u00e9tier de calcul du retard :**\n- Machine < 13 mois (395 jours) : pas de retard (p\u00e9riode de gr\u00e2ce)\n- Machine sans pr\u00e9ventive : en retard si > 365 jours apr\u00e8s installation\n- Machine avec pr\u00e9ventives : en retard si derni\u00e8re pr\u00e9ventive > 365 jours\n\n**Sources de donn\u00e9es :**\n- Oracle NESHU (DLOG) : machines et interventions techniques (preventive dlog)\n- Yuman : mat\u00e9riels, clients, sites et workorders\n"""
    )
    as (
      -- fct_technique__machines_maintenance_tracking.sql


-- LISTE MACHINE DLOG filtré & clean
with liste_machine_oracle as (
    select
        d.device_id,
        CONCAT('NESH_', d.device_code) as device_code,
        d.device_name,
        CONCAT('NESH_', company_code) as company_code,
        d.company_name,
        d.last_installation_date,
        d.created_at as device_created_at,
        d.updated_at as device_updated_at
    from `evs-datastack-prod`.`prod_marts`.`dim_oracle_neshu__device` as d
    where
        is_active
        and device_type_id in (1, 2)
        and REGEXP_CONTAINS(company_code, r'^CN[0-9]{4}$')
        and device_name in (
            'MOMENTO 100', 'GEMINI 200', 'MOMENTO 200', 'MINITOWER GEMINI', 'MINITOWER MOMENTO',
            'SBS MOMENTO 100', 'TOWER GEMINI', 'TOWER MOMENTO', 'MILANO LYO FTS120',
            'MILANO GRAIN FTS60E', 'MILANO GRAIN FTS60E + MODULO', 'BLUSODA', 'BLUSODA GAZ',
            'TOWER BLUSODA', 'TOWER BLUSODA GAZ', 'MILANO LYO FTS120 + MODULO',
            'BLUSODA',
            'TOWER ONE GAZ',
            'OPTIBEANX12 + MODULO',
            'OPTIBEAN X 12',
            'OPTIBEAN X 12 TS',
            'OPTIBEANX12TS + MODULO',
            'OPTIBEANX12TS + MODULO',
            'OPTIBEAN 12'
        )
),

-- LISTE MACHINE YUMAN ENRICHI CLIENT / SITE filtré & clean
yuman_materials_clean as (
    select
        ym.material_id,
        ym.material_description,
        ym.material_name,
        ym.material_brand,
        ym.material_serial_number,
        ycat.category_name,
        ym.material_in_service_date,
        ym.created_at,
        ym.updated_at,
        yc.client_id,
        yc.client_code,
        yc.client_name,
        yc.client_category,
        yc.partner_name,
        ys.site_id,
        ys.site_code,
        ys.site_postal_code
    from `evs-datastack-prod`.`prod_staging`.`stg_yuman__materials` as ym
    left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__sites` as ys
        on ym.site_id = ys.site_id
    left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__clients` as yc
        on ys.client_id = yc.client_id
    left join `evs-datastack-prod`.`prod_staging`.`stg_yuman__materials_categories` as ycat
        on ym.category_id = ycat.category_id
    where
        yc.partner_name = 'NESHU'
        and ys.site_code not in (
            'NESH_DEPOTATELIERBORDEAUX', 'NESH_DEPOTATELIERLYON', 'NESH_DEPOTATELIERMARSEILLE',
            'NESH_DEPOTATELIERRUNGIS', 'NESH_DEPOTATELIERSTRASBOURG', 'NESH_DEPOTBORDEAUX',
            'NESH_DEPOTLYON', 'NESH_DEPOTMARSEILLE', 'NESH_DEPOTPERIMES', 'NESH_DEPOTREBUS',
            'NESH_DEPOTRUNGIS', 'NESH_DEPOTSTRASBOURG', 'NESH_RELYON', 'NESH_RERUNGIS', 'NESH_STOCKNUNSHEN'
        )
        and ym.material_name not like '%GENERIQUE NESHU%'
        and ym.material_serial_number not in
        (
            'NESH_MA00226', 'NESH_MA00227', 'NESH_MA00247', 'NESH_MA00248', 'NESH_MA00249',
            'NESH_MA00250', 'NESH_MA00251', 'NESH_MA00252', 'NESH_MA00253', 'NESH_MA00254',
            'NESH_MA00193', 'NESH_MA00194', 'NESH_MA00195', 'NESH_MA00196', 'NESH_MA00197',
            'NESH_MA00198', 'NESH_MA00199', 'NESH_MA00200', 'NESH_MA00201', 'NESH_MA00202',
            'NESH_MA00203', 'NESH_MA00204', 'NESH_MA00205', 'NESH_MA00206', 'NESH_MA00207',
            'NESH_MA00208', 'NESH_MA00209', 'NESH_MA00210', 'NESH_MA00211', 'NESH_MA00212',
            'NESH_MA00213', 'NESH_MA00214', 'NESH_MA00215', 'NESH_MA00228', 'NESH_MA00229',
            'NESH_MA00230', 'NESH_MA00231', 'NESH_MA00232', 'NESH_MA00233', 'NESH_MA00234',
            'NESH_MA00235', 'NESH_MA00236', 'NESH_MA00237', 'NESH_MA00238', 'NESH_MA00239',
            'NESH_MA00240', 'NESH_MA00241', 'NESH_MA00242', 'NESH_MA00244', 'NESH_MA00245',
            'NESH_MA00246', 'NESH_MA00256', 'NESH_MA00257', 'NESH_MA00258', 'NESH_MA00259',
            'NESH_MA00260', 'NESH_MA00261', 'NESH_MA00262', 'NESH_MA00263', 'NESH_MA00264',
            'NESH_MA00265', 'NESH_MA00266', 'NESH_MA00270', 'NESH_MA00216', 'NESH_MA00217',
            'NESH_MA00220', 'NESH_MA00221', 'NESH_MA00222', 'NESH_MA00223', 'NESH_MA00181',
            'NESH_MA00182', 'NESH_MA00184', 'NESH_MA00185', 'NESH_MA00183', 'NESH_MA00186',
            'NESH_MA00187', 'NESH_AS00401', 'NESH_AS00403', 'NESH_AS00393', 'NESH_AS00557',
            'NESH_AS00558', 'NESH_AS00242', 'NESH_AS00241', 'NESH_AS00562', 'NESH_AS00559',
            'NESH_AS00070', 'NESH_AS00568', 'NESH_AS00563', 'NESH_AS00561', 'NESH_AS00560',
            'NESH_AS00317', 'NESH_AS00314', 'NESH_MA00136', 'NESH_AS00011', 'NESH_AS00012',
            'NESH_MA00170', 'NESH_AS00004'
        )
),

-- JOINTURE ENTRE LA LISTE MACHINE DLOG avec les données YUMAN
machines_base as (
    select
        lo.device_id,
        lo.device_code,
        lo.device_name,
        lo.company_code,
        lo.company_name,
        lo.last_installation_date,
        lo.device_created_at,
        lo.device_updated_at,
        ym.material_id,
        ym.material_serial_number,
        ym.material_name,
        ym.client_id,
        ym.client_code,
        ym.client_name,
        ym.client_category,
        ym.site_id,
        ym.site_code,
        ym.site_postal_code,
        ym.created_at as material_created_at,
        ym.updated_at as material_updated_at
    from liste_machine_oracle as lo
    left join yuman_materials_clean as ym
        on lo.device_code = ym.material_serial_number and lo.company_code = ym.client_code
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
        CONCAT('NESH_', device_code) as device_code,
        MAX(task_end_date) as last_preventive_date
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
        MAX(
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
        MAX(last_preventive_date) over (
            partition by material_id
        ) as derniere_preventive_source2,

        -- Date du jour
        CURRENT_TIMESTAMP() as today

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
                last_installation_date > TIMESTAMP_SUB(today, interval 395 day)
                then false

            -- CAS 2: Machine ancienne sans aucune préventive
            when
                derniere_preventive_source1 is null
                and derniere_preventive_source2 is null
                then true

            -- CAS 3: Machine avec préventives - vérifier si retard > 365 jours
            when
                GREATEST(
                    COALESCE(
                        derniere_preventive_source1,
                        TIMESTAMP('1900-01-01')
                    ),
                    COALESCE(
                        derniere_preventive_source2,
                        TIMESTAMP('1900-01-01')
                    )
                ) < TIMESTAMP_SUB(today, interval 365 day)
                then true
            else false
        end as retard_bol,

        -- CALCUL DU DÉLAI
        case
            -- CAS 1: Machine récente (< 13 mois)
            when
                last_installation_date > TIMESTAMP_SUB(today, interval 395 day)
                then
                    TIMESTAMP_DIFF(
                        TIMESTAMP_ADD(
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
                    -TIMESTAMP_DIFF(
                        today,
                        TIMESTAMP_ADD(
                            last_installation_date, interval 365 day
                        ),
                        day
                    )

            -- CAS 3: Avec préventives
            when
                TIMESTAMP_DIFF(
                    today,
                    GREATEST(
                        COALESCE(
                            derniere_preventive_source1,
                            TIMESTAMP('1900-01-01')
                        ),
                        COALESCE(
                            derniere_preventive_source2,
                            TIMESTAMP('1900-01-01')
                        )
                    ),
                    day
                ) > 365
                then
                    -(
                        TIMESTAMP_DIFF(
                            today,
                            GREATEST(
                                COALESCE(
                                    derniere_preventive_source1,
                                    TIMESTAMP('1900-01-01')
                                ),
                                COALESCE(
                                    derniere_preventive_source2,
                                    TIMESTAMP('1900-01-01')
                                )
                            ),
                            day
                        ) - 365
                    )
            else
                365 - TIMESTAMP_DIFF(
                    today,
                    GREATEST(
                        COALESCE(
                            derniere_preventive_source1,
                            TIMESTAMP('1900-01-01')
                        ),
                        COALESCE(
                            derniere_preventive_source2,
                            TIMESTAMP('1900-01-01')
                        )
                    ),
                    day
                )
        end as retard_delai,

        -- SOURCE DE LA DERNIÈRE PRÉVENTIVE
        case
            when
                last_installation_date > TIMESTAMP_SUB(today, interval 395 day)
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
        ROW_NUMBER() over (
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
        ROW_NUMBER() over (
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
        COALESCE(status_inter, 'Aucune') as status_inter,
        date_planned,
        material_created_at,

        -- Métadonnées dbt
        CURRENT_TIMESTAMP() as dbt_updated_at,
        'df6730f5-7a11-4b44-91d1-f95d18797a0d' as dbt_invocation_id  -- noqa: TMP

    from deduplicated
    where rn = 1  -- Ne garder qu'une ligne par device_id
)

select * from final
    );
  