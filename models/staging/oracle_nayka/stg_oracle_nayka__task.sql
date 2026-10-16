{{
    config(
        materialized='table',
        description="Tâches Nayka (Oracle task), table centrale de l'ERP : documents commerciaux (commandes, BL, factures), mouvements de stock, interventions, planifications. Le type fonctionnel est porté par task_type.code_type."
    )
}}

with source_data as (
    select *
    from {{ source('oracle_nayka', 'nayka_task') }}
),

cleaned_data as (
    -- Non reprises : colonnes jamais renseignées, paramétrage de l'ERP (idinstance, spantime constant,
    -- flag au sens non documenté).
    select
        cast(idtask as int64) as idtask,
        cast(task_idtask as int64) as task_idtask,
        cast(idtask_type as int64) as idtask_type,
        cast(idtask_status as int64) as idtask_status,
        cast(idcompany_self as int64) as idcompany_self,
        cast(idcompany_peer as int64) as idcompany_peer,
        cast(iddevice as int64) as iddevice,
        cast(idcontact as int64) as idcontact,
        cast(idlocation as int64) as idlocation,
        cast(idcurrency as int64) as idcurrency,
        cast(idcontact_creation as int64) as idcontact_creation,
        cast(idcontact_modification as int64) as idcontact_modification,

        -- Source et destination d'un mouvement : une société (dépôt, client) ou un équipement, selon
        -- type_product_*. Le préfixe idproduct_ de la source est trompeur.
        cast(idproduct_source as int64) as idproduct_source,
        type_product_source,
        cast(idproduct_destination as int64) as idproduct_destination,
        type_product_destination,

        code,
        prefix_document_number,
        document_number,
        -- La source écrit la chaîne 'null' faute de valeur.
        nullif(external_reference, 'null') as external_reference,
        comments_self,
        comments_peer,

        cast(code_status_record as int64) as code_status_record,
        cast(locked as int64) as locked,
        cast(isobligatory as int64) as isobligatory,
        cast(use_mois_comptable as int64) as use_mois_comptable,
        cast(cast(is_vat_paid as int64) as bool) as is_vat_paid,
        cast(sequence as int64) as sequence,  -- noqa: RF04

        cast(realized_percentage as numeric) as realized_percentage,
        cast(planed_ref_value as numeric) as planed_ref_value,
        cast(real_ref_value as numeric) as real_ref_value,

        -- Champs personnalisés de l'instance, stockés en XML.
        safe_cast(regexp_extract(xml, r'<HTPRICE>([^<]*)</HTPRICE>') as numeric) as ht_price,
        safe_cast(regexp_extract(xml, r'<TTCPRICE>([^<]*)</TTCPRICE>') as numeric) as ttc_price,
        regexp_extract(xml, r'<RECEIVER>([^<]*)</RECEIVER>') as receiver,

        -- Instants, malgré le suffixe _date de la source. real_end_date en 2099 = planification sans fin.
        {{ paris_vers_utc('planed_start_date') }} as planed_start_date,
        {{ paris_vers_utc('planed_end_date') }} as planed_end_date,
        {{ paris_vers_utc('real_start_date') }} as real_start_date,
        {{ paris_vers_utc('real_end_date') }} as real_end_date,
        {{ paris_vers_utc('validation_date') }} as validation_date,
        {{ paris_vers_utc('conversion_date') }} as conversion_date,

        -- Période d'application en jours calendaires : début à minuit, fin à 23:59:59.
        date(application_start_date) as application_start_date,
        date(application_end_date) as application_end_date,

        {{ paris_vers_utc('creation_date') }} as created_at,
        {{ paris_vers_utc('coalesce(modification_date, creation_date)') }} as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
