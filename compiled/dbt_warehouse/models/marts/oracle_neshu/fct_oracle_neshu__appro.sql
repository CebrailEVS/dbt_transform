

-- Un seul pointage par jour et par roadman
with pointage_unique_par_jour as (
    select
        task_id,
        company_id,
        resources_roadman_id,
        roadman_code,
        status_code,
        label_code,
        date(date_pointage) as date_jour,
        created_at,
        updated_at,
        extracted_at,
        min(case
            when format_timestamp('%H:%M:%S', date_pointage) >= '03:00:00' then date_pointage
        end) as premier_pointage
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__pointage`
    where label_code = 'START_DAY'
    group by
        task_id, company_id, resources_roadman_id, roadman_code,
        status_code, label_code, date(date_pointage),
        created_at, updated_at, extracted_at
),

pointage_min_par_jour as (
    select
        p.*,
        row_number() over (
            partition by
                p.resources_roadman_id,
                p.date_jour
            order by
                p.premier_pointage
        ) as rn
    from pointage_unique_par_jour as p
),

pointage_final_table as (
    select
        task_id,
        company_id,
        resources_roadman_id,
        roadman_code,
        status_code,
        label_code,
        date_jour as date_pointage_jour,
        premier_pointage as date_pointage,
        created_at,
        updated_at,
        extracted_at
    from pointage_min_par_jour
    where rn = 1 and resources_roadman_id is not null
),

resources_roadman as (
    select
        thr.idtask,
        min(r.idresources) as resources_roadman_id,
        min(r.code) as roadman_code
    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_resources` as thr
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__resources` as r
        on
            thr.idresources = r.idresources
            and r.idresources_type = 2
    group by thr.idtask
),

passage_appro as (
    select
        pa.task_id,
        pa.device_id,
        pa.company_id,
        pa.product_source_id,
        r.resources_roadman_id,
        pa.product_destination_id,
        pa.product_source_type,
        pa.product_destination_type,
        pa.company_code,
        c.name as company_name,
        c.name || ' - ' || pa.company_code as company_info,
        d.code as device_code,
        d.name as device_name,
        d.name || ' - ' || d.code as device_info,
        g.gea_code,
        r.roadman_code,
        pa.task_location_info,
        case
            when pa.task_status_code in ('FAIT', 'ENCOURS') then 'FAIT'
            else pa.task_status_code
        end as task_status_code,
        p.date_pointage,
        p.date_pointage_jour,
        date(pa.task_start_date) as start_date_day,
        pa.task_start_date,
        pa.task_end_date,
        -- Durée brute (audit)
        pa.task_end_date - pa.task_start_date as passage_duration_interval,
        -- Métrique BI
        case
            when
                pa.task_start_date is not null
                and pa.task_end_date is not null
                and pa.task_status_code = 'FAIT'
                then
                    timestamp_diff(
                        pa.task_end_date,
                        pa.task_start_date,
                        second
                    ) / 60.0
        end as passage_duration_min,
        case
            when
                pa.task_start_date is not null
                and pa.task_end_date is not null
                and pa.task_status_code = 'FAIT'
                then
                    timestamp_diff(
                        pa.task_end_date,
                        pa.task_start_date,
                        second
                    ) / 3600.0
        end as passage_duration_hours,
        case when pa.task_status_code in ('FAIT', 'ENCOURS') then 1 else 0 end as is_done,
        case when pa.task_status_code in ('PREVU', 'FAIT', 'ENCOURS') then 1 else 0 end as is_planned,
        case
            when p.date_pointage is null then 1
            else 0
        end as pointage_missing_flag,
        pa.created_at,
        pa.updated_at
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__appro_tasks` as pa

    left join resources_roadman as r
        on pa.task_id = r.idtask

    left join `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__roadman_gea` as g
        on r.roadman_code = g.roadman_code

    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__company` as c
        on pa.company_id = c.idcompany

    left join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__device` as d
        on pa.device_id = d.iddevice

    left join pointage_final_table as p
        on
            date(pa.task_start_date) = p.date_pointage_jour
            and r.resources_roadman_id = p.resources_roadman_id

    where r.resources_roadman_id is not null
),

passage_with_metrics as (
    select
        pa.*,
        -- Dernière task_end_date du roadman pour ce jour (statut FAIT uniquement)
        max(case when pa.task_status_code = 'FAIT' then pa.task_end_date end)
            over (
                partition by pa.resources_roadman_id, date(pa.task_start_date)
                order by pa.task_end_date
                rows between unbounded preceding and unbounded following
            )
            as last_task_end_of_day,

        -- Utilise date_pointage si disponible, sinon première task_start_date du jour
        coalesce(
            pa.date_pointage,
            first_value(pa.task_start_date) over (
                partition by pa.resources_roadman_id, date(pa.task_start_date)
                order by pa.task_start_date
                rows between unbounded preceding and unbounded following
            )
        ) as effective_work_start,

        -- Nombre de passages du roadman ce jour
        count(*) over (
            partition by pa.resources_roadman_id, date(pa.task_start_date)
        ) as daily_task_count,

        -- Temps moyen par passage du roadman ce jour
        avg(timestamp_diff(pa.task_end_date, pa.task_start_date, minute)) over (
            partition by pa.resources_roadman_id, date(pa.task_start_date)
        ) as avg_passage_duration_day,

        -- Numéro de passage dans la journée
        row_number() over (
            partition by pa.resources_roadman_id, date(pa.task_start_date)
            order by pa.task_start_date
        ) as passage_rank_of_day,

        -- 📊 NOUVEAUX CALCULS DE TAUX --

        -- Compteurs pour le roadman ce jour
        sum(pa.is_done) over (
            partition by pa.resources_roadman_id, date(pa.task_start_date)
        ) as done_count_roadman_day,

        sum(pa.is_planned) over (
            partition by pa.resources_roadman_id, date(pa.task_start_date)
        ) as planned_count_roadman_day,

        -- Taux de réalisation du roadman ce jour
        safe_divide(
            sum(pa.is_done) over (
                partition by pa.resources_roadman_id, date(pa.task_start_date)
            ),
            sum(pa.is_planned) over (
                partition by pa.resources_roadman_id, date(pa.task_start_date)
            )
        ) as taux_realisation_roadman_day,

        -- Taux de réalisation global du jour (tous roadmen)
        safe_divide(
            sum(pa.is_done) over (partition by date(pa.task_start_date)),
            sum(pa.is_planned) over (partition by date(pa.task_start_date))
        ) as taux_realisation_global_day

    from passage_appro as pa
),

-- Calcul de work_duration_min
passage_work_duration as (
    select
        *,
        -- Calcul du work_duration en minutes (brut)
        timestamp_diff(last_task_end_of_day, effective_work_start, minute) as work_duration_min_raw,

        -- Calcul du work_duration en minutes (nettoyé)
        case
            when timestamp_diff(
                last_task_end_of_day, effective_work_start, minute
            ) > 720 then null  -- > 12h = suspect
            when timestamp_diff(
                last_task_end_of_day, effective_work_start, minute
            ) < 0 then null    -- négatif = erreur
            when
                last_task_end_of_day is null
                or effective_work_start is null
                then null
            else timestamp_diff(
                last_task_end_of_day, effective_work_start, minute
            )
        end as work_duration_min
    from passage_with_metrics
)

-- Final table
select
    -- 1️⃣ IDs
    task_id,
    device_id,
    company_id,
    resources_roadman_id,

    -- 2️⃣ Attributs
    company_info,
    device_info,
    roadman_code,
    gea_code,
    task_status_code,

    -- 3️⃣ Dates / Temps
    date_pointage,
    date_pointage_jour,
    start_date_day,
    task_start_date,
    task_end_date,
    last_task_end_of_day,
    effective_work_start,

    -- 4️⃣ Mesures / KPI
    is_planned,
    is_done,
    passage_rank_of_day,
    daily_task_count,
    avg_passage_duration_day,
    passage_duration_interval,
    passage_duration_min,
    passage_duration_hours,
    work_duration_min_raw,
    work_duration_min,
    pointage_missing_flag,
    to_hex(md5(cast(coalesce(cast(company_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(start_date_day as string), '_dbt_utils_surrogate_key_null_') as string))) as client_jour_key,  -- noqa: TMP

    case
        when is_done = 1 then 1
        else 0
    end as passage_client_done,

    -- 📊 Nouveaux KPI taux de réalisation
    done_count_roadman_day,
    planned_count_roadman_day,
    taux_realisation_roadman_day,
    taux_realisation_global_day,

    -- 5️⃣ Métadonnées
    created_at,
    updated_at,

    -- Métadonnées dbt
    current_timestamp() as dbt_updated_at,
    '03229da1-54a3-41e2-af89-b274a343e19d' as dbt_invocation_id  -- noqa: TMP

from passage_work_duration