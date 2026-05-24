
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_yuman__workorders`
      
    
    

    
    OPTIONS(
      description="""Interventions transform\u00e9es et nettoy\u00e9es depuis l'API Yuman"""
    )
    as (
      

with source_data as (

    select *
    from `evs-datastack-prod`.`prod_raw`.`yuman_workorders`

),

base_workorders as (

    select
        -- IDs and foreign keys
        id as workorder_id,
        site_id,
        client_id,
        technician_id,
        manager_id,
        category_id,
        contact_id,
        -- Workorder details
        number as workorder_number,
        workorder_type,
        _embed_category_name as workorder_category,
        status as workorder_status,
        title as workorder_title,
        description as workorder_description,
        report as workorder_report,
        _embed_technician_name as workorder_technician_name,
        time_taken as workorder_time_taken,
        -- JSON
        safe.parse_json(_embed_fields) as embed_fields,
        -- Dates
        timestamp(date_planned) as date_planned,
        timestamp(date_started) as date_started,
        timestamp(date_done) as date_done,
        timestamp(created_at) as created_at,
        timestamp(updated_at) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at
    from source_data

),

json_parsed as (

    select
        *,
        -- Extraction des champs spécifiques depuis le JSON array
        (
            select field
            from unnest(json_extract_array(embed_fields)) as field
            where json_extract_scalar(field, '$.name') = 'DATE DE CREATION'
        ) as date_creation_field,
        (
            select field
            from unnest(json_extract_array(embed_fields)) as field
            where json_extract_scalar(field, '$.name') = 'MOTIF DE NON INTERVENTION'
        ) as motif_non_intervention_field,
        (
            select field
            from unnest(json_extract_array(embed_fields)) as field
            where json_extract_scalar(field, '$.name') = 'NON INTERVENTION : DETAIL/AUTRE MOTIF'
        ) as detail_non_intervention_field,
        (
            select field
            from unnest(json_extract_array(embed_fields)) as field
            where json_extract_scalar(field, '$.name') = 'RAISON MISE EN PAUSE'
        ) as raison_pause_field,
        (
            select field
            from unnest(json_extract_array(embed_fields)) as field
            where json_extract_scalar(field, '$.name') = 'EXPLICATION MISE EN PAUSE'
        ) as explication_pause_field,
        (
            select field
            from unnest(json_extract_array(embed_fields)) as field
            where json_extract_scalar(field, '$.name') = "NECESSITE D'INTERVENIR"
        ) as necessite_intervenir_field,
        (
            select field
            from unnest(json_extract_array(embed_fields)) as field
            where json_extract_scalar(field, '$.name') = 'SI NON POURQUOI ?'
        ) as si_non_pourquoi_field
    from base_workorders

),

final as (

    select
        -- Colonnes de base
        workorder_id,
        site_id,
        client_id,
        technician_id,
        manager_id,
        category_id,
        contact_id,
        workorder_number,
        workorder_type,
        workorder_category,
        workorder_status,
        workorder_title,
        workorder_description,
        workorder_report,
        workorder_technician_name,
        workorder_time_taken,
        -- Champs extraits du JSON avec nettoyage
        case
            when json_extract_scalar(date_creation_field, '$.value') is not null
                then safe.parse_date('%d/%m/%Y', json_extract_scalar(date_creation_field, '$.value'))
        end as workorder_date_creation,
        json_extract_scalar(motif_non_intervention_field, '$.value') as workorder_motif_non_intervention,
        json_extract_scalar(detail_non_intervention_field, '$.value') as workorder_detail_non_intervention,
        json_extract_scalar(raison_pause_field, '$.value') as workorder_raison_mise_en_pause,
        json_extract_scalar(explication_pause_field, '$.value') as workorder_explication_mise_en_pause,
        json_extract_scalar(necessite_intervenir_field, '$.value') as workorder_necessite_intervenir,
        json_extract_scalar(si_non_pourquoi_field, '$.value') as workorder_si_non_pourquoi,
        -- Dates
        date_planned,
        date_started,
        date_done,
        created_at,
        updated_at,
        extracted_at,
        deleted_at
    from json_parsed

)

select *
from final
    );
  