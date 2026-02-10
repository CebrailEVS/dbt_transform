{{ config(
    materialized='table',
    schema='marts',
    alias='fct_yuman__workorder_pricing',
    partition_by={"field": "date_done", "data_type": "timestamp"}
) }}

with base_workorders as (
    select
        demand_id,
        workorder_id,
        material_id,
        site_id,
        client_id,
        technician_id,
        manager_id,
        demand_description,
        demand_status,
        demand_created_at,
        demand_updated_at,
        demand_category_name,
        workorder_number,
        workorder_category,
        workorder_status,
        workorder_technician_name,
        workorder_date_creation,
        workorder_motif_non_intervention,
        workorder_detail_non_intervention,
        workorder_raison_mise_en_pause,
        workorder_explication_mise_en_pause,
        date_planned,
        date_started,
        date_done,
        partner_name,
        client_code,
        client_name,
        client_category,
        client_is_active,
        site_code,
        site_name,
        site_address,
        site_postal_code,
        material_serial_number,
        technician_equipe,

        lower(coalesce(
            workorder_category,
            case
                when workorder_type = 'Reactive' then 'curatif'
                when workorder_type = 'Preventive' then 'préventif'
                when workorder_type = 'Installation' then 'installation'
                else workorder_type
            end
        )) as workorder_type_raw,

        lower(trim(case
            when material_category is not null then material_category
            when lower(client_name) like '%generique%' then concat(partner_name, '_GENERIQUE')
            when partner_name = 'AUUM' then 'MGZ'
            when partner_name = 'TWYD' then 'FONTAINE TWYD'
            when partner_name = 'EXPRESSO' then 'MILANO'
            when partner_name = 'NESHU' then 'MILANO'
            when partner_name = 'BRITA' then 'viv t 85 c2-tg-i-cu ce'
            when partner_name = 'DAAN' then 'lave-vaisselle'
            when partner_name = 'NU' then 'frigo nu'
        end)) as machine_raw,

        case
            when site_postal_code is null or site_postal_code = '00000'
                then (
                    select code_postal
                    from unnest(regexp_extract_all(demand_description, r'\b\d{5}\b')) as code_postal
                    where
                        not regexp_contains(
                            left(demand_description, strpos(demand_description, code_postal) - 1),
                            r'(?i)N°\s*$|interventions?\s*$'
                        )
                    limit 1  -- noqa: AM09
                )
            else regexp_extract(site_address, r'\b(\d{5})\b')
        end as postal_code_site,

        (
            workorder_status = 'Closed'
            and workorder_motif_non_intervention is null
            and workorder_detail_non_intervention is null
        ) as a_facturer

    from {{ ref('int_yuman__demands_workorders_enriched') }}
),

ref_type_inter as (
    select
        lower(type_intervention_brut) as workorder_type_raw,
        lower(type_inter) as workorder_type_clean
    from {{ ref('ref_yuman__type_inter_clean') }}
),

ref_machine as (
    select
        machine_raw,
        machine_clean
    from (
        select
            lower(trim(machine_brut)) as machine_raw,
            lower(trim(machine)) as machine_clean,
            row_number() over (partition by lower(trim(machine_brut)) order by machine) as rn
        from {{ ref('ref_yuman__machine_clean') }}
    )
    where rn = 1
),

ref_cp_metropole as (
    select
        code_postal,
        metropole
    from {{ ref('ref_yuman__cp_metropole') }}
),

ref_dpt_metropole as (
    select
        departement,
        metropole
    from {{ ref('ref_yuman__dpt_metropole') }}
),

ref_tarification as (
    select
        lower(concat(
            type_inter, '_',
            machine, '_',
            marque, '_',
            type_tarif, '_',
            cast(metropole as string)
        )) as key_tarif,
        montant,
        prod
    from {{ ref('ref_yuman__tarification_clean') }}
),

workorders_enriched as (
    select
        w.*,
        coalesce(ti.workorder_type_clean, w.workorder_type_raw) as workorder_type_clean,
        coalesce(m.machine_clean, w.machine_raw) as machine_clean,
        coalesce(cp.metropole, dp.metropole) as metropole_city,
        case
            when w.postal_code_site is null then 1
            when cp.code_postal is not null then 1
            when dp.departement is not null then 1
            else 0
        end as metropole,
        row_number() over (partition by w.workorder_id order by w.date_done) as rn
    from base_workorders as w
    left join ref_type_inter as ti
        on w.workorder_type_raw = ti.workorder_type_raw
    left join ref_machine as m
        on w.machine_raw = m.machine_raw
    left join ref_cp_metropole as cp
        on w.postal_code_site = cp.code_postal
    left join ref_dpt_metropole as dp
        on left(w.postal_code_site, 2) = dp.departement
),

workorders_dedup as (
    select
        *,
        case
            when postal_code_site is null then 1
            else count(
                case when a_facturer then site_id end
            ) over (partition by site_id, date(date_done))
        end as reccurence
    from workorders_enriched
),

workorders_with_tarif as (
    select
        *,
        case
            when postal_code_site is null then 'Tarif normal'
            when partner_name in ('AUUM', 'FONTAINCO', 'TWYD', 'NESHU', 'NU', 'DAANTECH', 'EXPRESSO', 'DAAN')
                then
                    case
                        when reccurence < 5 then 'Tarif normal'
                        when reccurence between 5 and 20 then 'Remise niv1'
                        else 'Remise niv2'
                    end
            when partner_name in ('BRITA', 'FONTAINCO')
                then
                    case
                        when reccurence < 2 then 'Tarif normal'
                        when reccurence between 2 and 5 then 'Remise niv1'
                        else 'Remise niv2'
                    end
            else 'Tarif normal'
        end as type_tarif
    from workorders_dedup
),

final_result as (
    select
        w.*,
        t.montant,
        t.prod,
        lower(concat(
            w.workorder_type_clean, '_',
            w.machine_clean, '_',
            w.partner_name, '_',
            w.type_tarif, '_',
            cast(w.metropole as string)
        )) as key_tarif_used
    from workorders_with_tarif as w
    left join ref_tarification as t
        on
            lower(concat(
                w.workorder_type_clean, '_',
                w.machine_clean, '_',
                w.partner_name, '_',
                w.type_tarif, '_',
                cast(w.metropole as string)
            )) = t.key_tarif
)

select
    demand_id,
    workorder_id,
    material_id,
    site_id,
    client_id,
    technician_id,
    manager_id,
    demand_description,
    demand_status,
    demand_created_at,
    demand_updated_at,
    demand_category_name,
    workorder_number,
    workorder_category,
    workorder_status,
    workorder_technician_name,
    workorder_date_creation,
    workorder_motif_non_intervention,
    workorder_detail_non_intervention,
    workorder_raison_mise_en_pause,
    workorder_explication_mise_en_pause,
    date_planned,
    date_started,
    date_done,
    partner_name,
    client_code,
    client_name,
    client_category,
    client_is_active,
    site_code,
    site_name,
    site_address,
    postal_code_site as site_postal_code,
    material_serial_number,
    workorder_type_raw,
    machine_raw,

    -- Renamed metrics and keys
    workorder_type_clean,
    machine_clean,
    metropole as metropolitan,
    metropole_city,
    technician_equipe,
    reccurence as recurrence_count,
    type_tarif as pricing_type,
    key_tarif_used as pricing_key_used,
    a_facturer as to_invoice,
    montant as amount,
    prod as prod_number,
    case
        when montant is not null and a_facturer = true then 'VALIDATED'
        when montant is null and a_facturer = true then 'MISSING_TARIF'
        else 'NOT_BILLABLE'
    end as billing_validation_status

from final_result
