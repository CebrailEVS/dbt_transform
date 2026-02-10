

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_ecriturec`
),

cleaned_data as (
    select
        -- Identifiants
        cast(ec_no as int64) as ec_no,
        cast(ec_no_link as int64) as ec_no_link,
        cast(cb_marq as int64) as cb_marq,

        -- Journal et comptes
        jo_num,
        cg_num,
        ct_num,
        ec_intitule,

        -- Montants et sens
        coalesce(ec_sens, 0) as ec_sens,
        coalesce(ec_montant, 0.0) as ec_montant,
        coalesce(ec_montant_regle, 0.0) as ec_montant_regle,
        ec_devise,
        n_devise,

        -- Dates
        timestamp(ec_date) as ec_date,
        timestamp(jm_date) as jm_date,
        ec_jour,
        case when ec_echeance = '1753-01-01' then null else timestamp(ec_echeance) end as ec_echeance,
        case when ec_date_rappro = '1753-01-01' then null else timestamp(ec_date_rappro) end as ec_date_rappro,
        case when ec_date_regle = '1753-01-01' then null else timestamp(ec_date_regle) end as ec_date_regle,

        -- Métadonnées
        cb_createur,
        cb_creation_user,
        timestamp(cb_creation) as created_at,
        timestamp(coalesce(cb_modification, cb_creation)) as updated_at,
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at

    from source_data
)

select *
from cleaned_data