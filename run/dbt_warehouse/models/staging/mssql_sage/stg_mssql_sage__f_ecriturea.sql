-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_ecriturea` as DBT_INTERNAL_DEST
        using (

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`dbo_f_ecriturea`
),

cleaned_data as (
    select
        -- IDs et numéro convertis en BIGINT
        cast(cb_marq as int64) as cb_marq,
        cast(ec_no as int64) as ec_no,
        cast(n_analytique as int64) as n_analytique,
        cast(ea_ligne as int64) as ea_ligne,

        -- Colonnes texte
        ca_num,
        cb_createur,
        cb_creation_user,

        -- Colonnes numériques avec gestion des valeurs nulles
        ea_montant,
        ea_quantite,

        -- Timestamps harmonisés
        timestamp(cb_creation) as created_at,
        timestamp(coalesce(cb_modification, cb_creation)) as updated_at, -- Use COALESCE to ensure updated_at is never null, falling back to creation_date
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
            FROM `evs-datastack-prod`.`prod_staging`.`stg_mssql_sage__f_ecriturea`
        )
        OR updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    )

        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.cb_marq = DBT_INTERNAL_DEST.cb_marq))

    
    when matched then update set
        `cb_marq` = DBT_INTERNAL_SOURCE.`cb_marq`,`ec_no` = DBT_INTERNAL_SOURCE.`ec_no`,`n_analytique` = DBT_INTERNAL_SOURCE.`n_analytique`,`ea_ligne` = DBT_INTERNAL_SOURCE.`ea_ligne`,`ca_num` = DBT_INTERNAL_SOURCE.`ca_num`,`cb_createur` = DBT_INTERNAL_SOURCE.`cb_createur`,`cb_creation_user` = DBT_INTERNAL_SOURCE.`cb_creation_user`,`ea_montant` = DBT_INTERNAL_SOURCE.`ea_montant`,`ea_quantite` = DBT_INTERNAL_SOURCE.`ea_quantite`,`created_at` = DBT_INTERNAL_SOURCE.`created_at`,`updated_at` = DBT_INTERNAL_SOURCE.`updated_at`,`extracted_at` = DBT_INTERNAL_SOURCE.`extracted_at`,`deleted_at` = DBT_INTERNAL_SOURCE.`deleted_at`
    

    when not matched then insert
        (`cb_marq`, `ec_no`, `n_analytique`, `ea_ligne`, `ca_num`, `cb_createur`, `cb_creation_user`, `ea_montant`, `ea_quantite`, `created_at`, `updated_at`, `extracted_at`, `deleted_at`)
    values
        (`cb_marq`, `ec_no`, `n_analytique`, `ea_ligne`, `ca_num`, `cb_createur`, `cb_creation_user`, `ea_montant`, `ea_quantite`, `created_at`, `updated_at`, `extracted_at`, `deleted_at`)


    