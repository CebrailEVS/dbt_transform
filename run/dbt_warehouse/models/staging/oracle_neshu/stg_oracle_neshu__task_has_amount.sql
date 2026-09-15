
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_neshu__task_has_amount`
      
    partition by timestamp_trunc(updated_at, day)
    cluster by idtask, idtax, idtax_region

    
    OPTIONS(
      description="""Montants et taxes par t\u00e2che, nettoy\u00e9s depuis la base Oracle. Porte le co\u00fbt des produits charg\u00e9s du P&L client."""
    )
    as (
      

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

    );
  