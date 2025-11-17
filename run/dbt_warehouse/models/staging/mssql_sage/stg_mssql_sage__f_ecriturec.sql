-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_ecriturec` as DBT_INTERNAL_DEST
        using (

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
        case when ec_echeance = '1753-01-01' then NULL else timestamp(ec_echeance) end as ec_echeance,
        case when ec_date_rappro = '1753-01-01' then NULL else timestamp(ec_date_rappro) end as ec_date_rappro,
        case when ec_date_regle = '1753-01-01' then NULL else timestamp(ec_date_regle) end as ec_date_regle,

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


WHERE
    (
        updated_at > (
            SELECT MAX(updated_at)
            FROM `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_ecriturec`
        )
        OR updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    )

        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.cb_marq = DBT_INTERNAL_DEST.cb_marq))

    
    when matched then update set
        `ec_no` = DBT_INTERNAL_SOURCE.`ec_no`,`ec_no_link` = DBT_INTERNAL_SOURCE.`ec_no_link`,`cb_marq` = DBT_INTERNAL_SOURCE.`cb_marq`,`jo_num` = DBT_INTERNAL_SOURCE.`jo_num`,`cg_num` = DBT_INTERNAL_SOURCE.`cg_num`,`ct_num` = DBT_INTERNAL_SOURCE.`ct_num`,`ec_intitule` = DBT_INTERNAL_SOURCE.`ec_intitule`,`ec_sens` = DBT_INTERNAL_SOURCE.`ec_sens`,`ec_montant` = DBT_INTERNAL_SOURCE.`ec_montant`,`ec_montant_regle` = DBT_INTERNAL_SOURCE.`ec_montant_regle`,`ec_devise` = DBT_INTERNAL_SOURCE.`ec_devise`,`n_devise` = DBT_INTERNAL_SOURCE.`n_devise`,`ec_date` = DBT_INTERNAL_SOURCE.`ec_date`,`jm_date` = DBT_INTERNAL_SOURCE.`jm_date`,`ec_jour` = DBT_INTERNAL_SOURCE.`ec_jour`,`ec_echeance` = DBT_INTERNAL_SOURCE.`ec_echeance`,`ec_date_rappro` = DBT_INTERNAL_SOURCE.`ec_date_rappro`,`ec_date_regle` = DBT_INTERNAL_SOURCE.`ec_date_regle`,`cb_createur` = DBT_INTERNAL_SOURCE.`cb_createur`,`cb_creation_user` = DBT_INTERNAL_SOURCE.`cb_creation_user`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`,`updated_at` = DBT_INTERNAL_SOURCE.`updated_at`,`extracted_at` = DBT_INTERNAL_SOURCE.`extracted_at`,`deleted_at` = DBT_INTERNAL_SOURCE.`deleted_at`
    

    when not matched then insert
        (`ec_no`, `ec_no_link`, `cb_marq`, `jo_num`, `cg_num`, `ct_num`, `ec_intitule`, `ec_sens`, `ec_montant`, `ec_montant_regle`, `ec_devise`, `n_devise`, `ec_date`, `jm_date`, `ec_jour`, `ec_echeance`, `ec_date_rappro`, `ec_date_regle`, `cb_createur`, `cb_creation_user`, `created_at`, `updated_at`, `extracted_at`, `deleted_at`)
    values
        (`ec_no`, `ec_no_link`, `cb_marq`, `jo_num`, `cg_num`, `ct_num`, `ec_intitule`, `ec_sens`, `ec_montant`, `ec_montant_regle`, `ec_devise`, `n_devise`, `ec_date`, `jm_date`, `ec_jour`, `ec_echeance`, `ec_date_rappro`, `ec_date_regle`, `cb_createur`, `cb_creation_user`, `created_at`, `updated_at`, `extracted_at`, `deleted_at`)


    