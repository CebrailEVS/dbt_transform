-- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
                
                
            
                
                
            
        
    

    

    merge into `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_amount` as DBT_INTERNAL_DEST
        using (

-- ⚠️ Deux particularités par rapport aux autres staging oracle_neshu.
--
-- 1. La table n'a NI creation_date NI modification_date. Le pipeline dlt la
--    réplique via un curseur emprunté à son parent `task`, qui atterrit dans
--    `_parent_modification_date`. C'est donc lui qui alimente `updated_at`, et
--    `created_at` n'existe pas ici.
--
-- 2. La clé fait QUATRE colonnes : une même tâche porte une ligne par taux et
--    par région de taxe. `tax_rate` en fait partie, elle reste donc en NUMERIC
--    exact — la caster en float64 comme les autres montants ferait s'écraser
--    des lignes distinctes au merge, sans erreur.

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`evs_task_has_amount`
),

cleaned_data as (
    select
        -- IDs (clé de merge composite)
        cast(idtask as int64) as idtask,
        cast(idtax as int64) as idtax,
        cast(idtax_region as int64) as idtax_region,

        -- Composante de la clé : précision exacte obligatoire (cf. en-tête)
        cast(tax_rate as numeric) as tax_rate,

        -- Colonnes texte
        tax_code,
        tax_name,

        -- Colonnes numériques
        cast(tax_amount as float64) as tax_amount,
        cast(amount_without_tax as float64) as amount_without_tax,
        cast(percentage as float64) as percentage,

        -- Timestamps harmonisés (standard DBT)
        timestamp(_parent_modification_date) as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
),

-- ⚠️ Sécurité de jointure pour éviter les orphelins
filtered_data as (
    select c.*
    from cleaned_data as c
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task` as t
        on c.idtask = t.idtask
)

select * from filtered_data

    where
        (
            updated_at > (
                select max(t.updated_at)
                from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_amount` as t
            )
            or updated_at >= timestamp_sub(current_timestamp(), interval 7 day)
        )

        ) as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.idtask = DBT_INTERNAL_DEST.idtask
                ) and (
                    DBT_INTERNAL_SOURCE.tax_rate = DBT_INTERNAL_DEST.tax_rate
                ) and (
                    DBT_INTERNAL_SOURCE.idtax = DBT_INTERNAL_DEST.idtax
                ) and (
                    DBT_INTERNAL_SOURCE.idtax_region = DBT_INTERNAL_DEST.idtax_region
                )

    
    when matched then update set
        `idtask` = DBT_INTERNAL_SOURCE.`idtask`,`idtax` = DBT_INTERNAL_SOURCE.`idtax`,`idtax_region` = DBT_INTERNAL_SOURCE.`idtax_region`,`tax_rate` = DBT_INTERNAL_SOURCE.`tax_rate`,`tax_code` = DBT_INTERNAL_SOURCE.`tax_code`,`tax_name` = DBT_INTERNAL_SOURCE.`tax_name`,`tax_amount` = DBT_INTERNAL_SOURCE.`tax_amount`,`amount_without_tax` = DBT_INTERNAL_SOURCE.`amount_without_tax`,`percentage` = DBT_INTERNAL_SOURCE.`percentage`,`updated_at` = DBT_INTERNAL_SOURCE.`updated_at`,`extracted_at` = DBT_INTERNAL_SOURCE.`extracted_at`
    

    when not matched then insert
        (`idtask`, `idtax`, `idtax_region`, `tax_rate`, `tax_code`, `tax_name`, `tax_amount`, `amount_without_tax`, `percentage`, `updated_at`, `extracted_at`)
    values
        (`idtask`, `idtax`, `idtax_region`, `tax_rate`, `tax_code`, `tax_name`, `tax_amount`, `amount_without_tax`, `percentage`, `updated_at`, `extracted_at`)


    