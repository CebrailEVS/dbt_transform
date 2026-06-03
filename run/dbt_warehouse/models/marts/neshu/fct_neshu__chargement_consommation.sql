
  
    

    create or replace table `evs-datastack-prod`.`prod_marts`.`fct_neshu__chargement_consommation`
      
    partition by date_debut_passage_appro
    cluster by device_id

    
    OPTIONS(
      description="""[QUOI M\u00c9TIER] Quantit\u00e9s charg\u00e9es vs consomm\u00e9es par machine et par passage APPRO (taux d'\u00e9coulement).\n[COMMENT CONSTRUITE] Pour chaque passage APPRO d'une machine, calcul via LAG du passage pr\u00e9c\u00e9dent pour borner la p\u00e9riode d'\u00e9coulement ; q_consommee = somme des t\u00e9l\u00e9m\u00e9tries entre les deux passages ; q_chargee = somme des chargements lors du passage actuel. Sources : int_oracle_neshu__chargement_tasks, int_oracle_neshu__telemetry_tasks, jointes par device_id et period bounds.\n[GRAIN] 1 ligne par (device_id, date_debut_passage_appro, product_id).\n[NOTES] roadman_code = nom du roadman associ\u00e9 au v\u00e9hicule source (attribut d'affichage). product_source_id / product_source_type = identifiant et type Oracle de la source du chargement (expos\u00e9s pour diagnostic, pas de test FK car mart combinant plusieurs intermediates). product_type aplati pour filtre BI. product_code conserv\u00e9 en attribut d'affichage.\n"""
    )
    as (
      

-- -----------------------------------------------------------------------------------
-- CTE 1 : Récupération des passages APPRO avec leur passage précédent
-- Utilisation de LAG pour identifier la période sur laquelle agréger les télémetries.
-- -----------------------------------------------------------------------------------
with passage_avec_suivant as (
    select
        ta.device_id,
        ta.task_start_date,
        rm.resources_name as roadman_code,
        ta.product_source_id,
        ta.product_source_type,
        lag(ta.task_start_date) over (
            partition by ta.device_id
            order by ta.task_start_date
        ) as date_passage_precedent
    from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__appro_tasks` as ta
    left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__resource` as rm
        on
            ta.product_source_id = rm.resources_id
            and rm.resources_type = 'VEHICLE'
    where
        ta.task_start_date >= '2025-01-01'
        and ta.task_status_code = 'FAIT'
),

-- -----------------------------------------------------------------------------------
-- CTE 2 : Agrégations des quantités consommées (télémetries)
-- Les télémetries sont prises entre le passage précédent et le passage actuel.
-- Filtre HAVING pour supprimer les lignes vides (product_type NULL & somme = 0).
-- -----------------------------------------------------------------------------------
telemetry_agg as (
    select
        pa.device_id,
        pa.task_start_date,
        p.product_type as product_type_2,
        (case
            when p.product_type in ('BOISSONS FRAICHES', 'SNACKING') then 'SODA + SNACKS'
            else p.product_type
        end) as product_type,
        p.product_id,
        p.product_code,
        coalesce(sum(t.telemetry_quantity), 0) as q_consommee
    from passage_avec_suivant as pa
    left join `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__telemetry_tasks` as t
        on
            pa.device_id = t.device_id
            and t.task_start_date
            between coalesce(pa.date_passage_precedent, timestamp('2024-12-30 00:00:00')) and pa.task_start_date
    left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__product` as p
        on t.product_id = p.product_id
    group by 1, 2, 3, 4, 5, 6
    having p.product_type is not null or sum(t.telemetry_quantity) > 0
),

-- -----------------------------------------------------------------------------------
-- CTE 3 : Agrégations des quantités chargées
-- Les chargements sont associés au passage APPRO via une jointure sur la DATE.
-- -----------------------------------------------------------------------------------
chargement_agg as (
    select
        pa.device_id,
        pa.task_start_date,
        p.product_type as product_type_2,
        (case
            when p.product_type in ('BOISSONS FRAICHES', 'SNACKING') then 'SODA + SNACKS'
            else p.product_type
        end) as product_type,
        p.product_id,
        p.product_code,
        coalesce(sum(cm.load_quantity), 0) as q_chargee
    from passage_avec_suivant as pa
    left join `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__chargement_tasks` as cm
        on
            pa.device_id = cm.device_id
            and date(pa.task_start_date) = date(cm.task_start_date)
    left join `evs-datastack-prod`.`prod_marts`.`dim_neshu__product` as p
        on cm.product_id = p.product_id
    group by 1, 2, 3, 4, 5, 6
    having product_type is not null or sum(cm.load_quantity) > 0
),

-- -----------------------------------------------------------------------------------
-- CTE 4 : Fusion telemetry + chargement via FULL JOIN
-- On récupère aussi roadman & date passage précédent depuis la CTE initiale.
-- -----------------------------------------------------------------------------------
fusion_telemetry_chargement as (
    select
        coalesce(t.device_id, c.device_id) as device_id,
        date(coalesce(t.task_start_date, c.task_start_date)) as date_debut_passage_appro,
        min(coalesce(t.task_start_date, c.task_start_date)) as task_start_date_min,
        min(pa.date_passage_precedent) as date_passage_precedent,
        max(pa.roadman_code) as roadman_code,
        max(pa.product_source_id) as product_source_id,
        max(pa.product_source_type) as product_source_type,
        coalesce(t.product_type, c.product_type) as product_type,
        coalesce(t.product_id, c.product_id) as product_id,
        coalesce(t.product_code, c.product_code) as product_code,
        sum(coalesce(t.q_consommee, 0)) as q_consommee,
        max(coalesce(c.q_chargee, 0)) as q_chargee
    from telemetry_agg as t
    full join chargement_agg as c
        on
            t.device_id = c.device_id
            and t.task_start_date = c.task_start_date
            and t.product_type = c.product_type
            and t.product_id = c.product_id
    left join passage_avec_suivant as pa
        on
            pa.device_id = coalesce(t.device_id, c.device_id)
            and pa.task_start_date = coalesce(t.task_start_date, c.task_start_date)
    group by
        1, 2, 8, 9, 10
)

-- -----------------------------------------------------------------------------------
-- Final : Sélection finale avec métadonnées dbt
-- -----------------------------------------------------------------------------------
select
    device_id,
    date_debut_passage_appro,
    task_start_date_min,
    date_passage_precedent,
    roadman_code,
    product_source_id,
    product_source_type,
    product_type,
    product_id,
    product_code,
    q_consommee,
    q_chargee,

    -- Métadonnées dbt
    current_timestamp() as dbt_updated_at,
    '29f4868e-2220-4383-939b-4680069779a3' as dbt_invocation_id  -- noqa: TMP

from fusion_telemetry_chargement
    );
  