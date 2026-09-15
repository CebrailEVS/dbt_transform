-- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
        
    

    

    merge into `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_thp` as DBT_INTERNAL_DEST
        using (

-- ⚠️ Comme task_has_amount, la table n'a aucune colonne de modification : dlt la
-- réplique via un curseur emprunté, mais à `task_has_product` cette fois et non
-- à `task`. C'est la seule jonction du périmètre dans ce cas. Le curseur atterrit
-- dans `_parent_modification_date` et alimente `updated_at` ; `created_at`
-- n'existe pas.
--
-- Ne pas confondre avec `label_has_product`, qui étiquette un PRODUIT du
-- catalogue. Ici c'est la ligne de produit d'une tâche.

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`evs_label_has_thp`
),

cleaned_data as (
    select
        -- IDs (clé de merge composite)
        cast(idlabel as int64) as idlabel,
        cast(idtask_has_product as int64) as idtask_has_product,

        -- Timestamps harmonisés (standard DBT)
        timestamp(_parent_modification_date) as updated_at,
        timestamp(_extracted_at) as extracted_at

    from source_data
),

-- ⚠️ Sécurité de jointure pour éviter les orphelins
filtered_data as (
    select c.*
    from cleaned_data as c
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_product` as thp
        on c.idtask_has_product = thp.idtask_has_product
)

select * from filtered_data

    where
        (
            updated_at > (
                select max(t.updated_at)
                from `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__label_has_thp` as t
            )
            or updated_at >= timestamp_sub(current_timestamp(), interval 7 day)
        )

        ) as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.idlabel = DBT_INTERNAL_DEST.idlabel
                ) and (
                    DBT_INTERNAL_SOURCE.idtask_has_product = DBT_INTERNAL_DEST.idtask_has_product
                )

    
    when matched then update set
        `idlabel` = DBT_INTERNAL_SOURCE.`idlabel`,`idtask_has_product` = DBT_INTERNAL_SOURCE.`idtask_has_product`,`updated_at` = DBT_INTERNAL_SOURCE.`updated_at`,`extracted_at` = DBT_INTERNAL_SOURCE.`extracted_at`
    

    when not matched then insert
        (`idlabel`, `idtask_has_product`, `updated_at`, `extracted_at`)
    values
        (`idlabel`, `idtask_has_product`, `updated_at`, `extracted_at`)


    