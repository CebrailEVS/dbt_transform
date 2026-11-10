{{ config(materialized='table', cluster_by=['snapshot_date', 'device_id']) }}

-- État des machines Cafés du Phare à chaque fin de mois, pour la clôture de Cockpit Supply. L'emplacement d'une
-- machine est la destination de son dernier mouvement validé à la fin du jour (heure de Paris). Une machine déjà
-- créée mais sans mouvement validé à cette date (réception annulée, pratique courante) est placée à la source de
-- son premier mouvement validé, ou à son emplacement actuel si elle n'en a aucun : is_emplacement_deduit le signale.

with fins_de_mois as (
    select last_day(mois) as snapshot_date
    from unnest(generate_date_array(date '2026-01-01', current_date('Europe/Paris'), interval 1 month)) as mois
    where last_day(mois) < current_date('Europe/Paris')
),

machines as (
    select
        m.device_id,
        m.device_code,
        m.device_serial_number,
        m.device_name,
        m.product_code,
        m.product_name,
        m.company_code,
        m.company_name,
        m.device_material_status,
        m.purchase_date,
        m.purchase_cost,
        date(d.created_at, 'Europe/Paris') as created_date
    from {{ ref('app_cockpit__lcdp_machine') }} as m
    inner join {{ ref('stg_oracle_lcdp__device') }} as d
        on m.device_id = d.iddevice
    where m.device_type = 'MACHINE'
),

mouvements_valides as (
    select
        task_id,
        movement_at,
        created_at,
        movement_type,
        device_id,
        source_kind,
        source_code,
        source_name,
        destination_kind,
        destination_code,
        destination_name,
        material_status_code,
        material_status
    from {{ ref('app_cockpit__lcdp_mouvement_machine') }}
    where
        device_type = 'MACHINE'
        and task_status_code in ('FAIT', 'VALIDE')
),

-- Dernier mouvement validé daté au plus tard à la fin du mois.
dernier_mouvement as (
    select
        f.snapshot_date,
        mv.*
    from fins_de_mois as f
    inner join mouvements_valides as mv
        on date(mv.movement_at) <= f.snapshot_date
    qualify row_number() over (
        partition by f.snapshot_date, mv.device_id order by mv.movement_at desc, mv.task_id desc
    ) = 1
),

-- Premier mouvement validé après la fin du mois : sa source (dépôt, client, ou fournisseur pour une machine
-- commandée) situe une machine qui n'avait pas encore de mouvement validé.
mouvement_suivant as (
    select
        f.snapshot_date,
        mv.*
    from fins_de_mois as f
    inner join mouvements_valides as mv
        on date(mv.movement_at) > f.snapshot_date
    qualify row_number() over (
        partition by f.snapshot_date, mv.device_id order by mv.movement_at asc, mv.task_id asc
    ) = 1
),

machines_avec_mouvement as (
    select distinct device_id
    from mouvements_valides
),

etats as (
    select
        f.snapshot_date,
        m.*,
        dm.device_id is not null as has_mouvement_avant,
        dm.destination_kind as dm_kind,
        dm.destination_code as dm_code,
        dm.destination_name as dm_name,
        dm.material_status_code as dm_status_code,
        dm.material_status as dm_status,
        dm.task_id as dm_task_id,
        dm.movement_type as dm_movement_type,
        dm.movement_at as dm_movement_at,
        dm.created_at as dm_created_at,
        coalesce(ms.source_kind not in ('AUCUNE', 'EQUIPEMENT'), false) as has_source_suivante,
        ms.source_kind as ms_kind,
        ms.source_code as ms_code,
        ms.source_name as ms_name,
        mam.device_id is not null as has_mouvement_valide
    from fins_de_mois as f
    cross join machines as m
    left join dernier_mouvement as dm
        on f.snapshot_date = dm.snapshot_date and m.device_id = dm.device_id
    left join mouvement_suivant as ms
        on f.snapshot_date = ms.snapshot_date and m.device_id = ms.device_id
    left join machines_avec_mouvement as mam
        on m.device_id = mam.device_id
)

select
    snapshot_date,
    device_id,
    device_code,
    device_serial_number,
    device_name,
    product_code,
    product_name,
    case when has_mouvement_avant then dm_kind when has_source_suivante then ms_kind end as location_kind,
    case
        when has_mouvement_avant then dm_code
        when has_source_suivante then ms_code
        else company_code
    end as company_code,
    case
        when has_mouvement_avant then dm_name
        when has_source_suivante then ms_name
        else company_name
    end as company_name,
    case when has_mouvement_avant then dm_status_code end as material_status_code,
    case
        when has_mouvement_avant then dm_status
        when not has_mouvement_valide then device_material_status
    end as material_status,
    purchase_date,
    purchase_cost,
    dm_task_id as last_movement_task_id,
    dm_movement_type as last_movement_type,
    dm_movement_at as last_movement_at,
    dm_created_at as last_movement_created_at,
    coalesce(date(dm_created_at) > snapshot_date, false) as is_saisie_apres_fin_mois,
    not has_mouvement_avant as is_emplacement_deduit
from etats
where
    has_mouvement_avant
    or (created_date <= snapshot_date and (has_source_suivante or not has_mouvement_valide))
