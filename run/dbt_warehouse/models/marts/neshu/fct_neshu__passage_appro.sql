
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_neshu__passage_appro`
      
    partition by timestamp_trunc(task_start_date, day)
    cluster by company_id, device_id, roadman_code

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Passages APPRO des roadmen enrichis des m\u00e9triques journali\u00e8res de temps de travail.\n[COMMENT CONSTRUITE] Issu de int_oracle_neshu__appro_tasks joint \u00e0 int_oracle_neshu__pointage_tasks (START_DAY) pour le pointage journalier du roadman. Calculs window function par roadman et jour : rang du passage, nombre quotidien de passages, dur\u00e9e moyenne, dur\u00e9e nette du temps de travail (derni\u00e8re fin - d\u00e9but effectif, nettoy\u00e9e des valeurs aberrantes <0 ou >12h). Statut ENCOURS reclass\u00e9 en FAIT apr\u00e8s validation m\u00e9tier.\n[GRAIN] 1 ligne par task_id (1 t\u00e2che APPRO).\n[NOTES] effective_work_start = pointage si disponible, sinon premi\u00e8re t\u00e2che du jour. is_planned (PREVU/FAIT/ENCOURS/ANOMALIE) / is_done (FAIT/ENCOURS) / is_anomaly (ANOMALIE) : indicateurs binaires. Source de v\u00e9rit\u00e9 unique des passages appro (KPIs journaliers + attributs d\u00e9taill\u00e9s), consomm\u00e9e par le monitoring et le reporting appro.\n"""
    )
    as (
      

-- ============================================================
-- CTE 1 : Pointage unique par roadman / jour
-- 1 ligne par (roadman, jour) avec le premier pointage START_DAY
-- ============================================================
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
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__pointage_tasks`
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

-- ============================================================
-- CTE 2 : Tâches appro enrichies + pointage joint
-- ============================================================
passage_appro as (
    select
        e.*,
        p.date_pointage,
        p.date_pointage_jour,
        case when p.date_pointage is null then 1 else 0 end as pointage_missing_flag
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__appro_tasks_enriched` as e
    left join pointage_final_table as p
        on
            e.start_date_day = p.date_pointage_jour
            and e.resources_roadman_id = p.resources_roadman_id
),

-- ============================================================
-- CTE 3 : Métriques journalières par roadman (window functions)
-- ============================================================
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

-- ============================================================
-- CTE 4 : Calcul de work_duration_min (nettoyé des aberrants)
-- ============================================================
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

-- ============================================================
-- Sélection finale
-- ============================================================
select
    -- 1️⃣ IDs
    task_id,
    device_id,
    company_id,
    resources_roadman_id,
    product_source_id,
    product_destination_id,

    -- 2️⃣ Attributs
    company_info,
    company_code,
    company_name,
    device_info,
    device_code,
    device_name,
    roadman_code,
    gea_code,
    product_source_type,
    product_destination_type,
    task_location_info,
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
    is_anomaly,
    passage_rank_of_day,
    daily_task_count,
    avg_passage_duration_day,
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

    -- 📊 KPI taux de réalisation
    done_count_roadman_day,
    planned_count_roadman_day,
    taux_realisation_roadman_day,
    taux_realisation_global_day,

    -- 5️⃣ Métadonnées
    created_at,
    updated_at,

    -- Métadonnées dbt
    current_timestamp() as dbt_updated_at,
    '509d7683-bae3-4925-8142-10e08d8d4755' as dbt_invocation_id  -- noqa: TMP

from passage_work_duration
    );
  