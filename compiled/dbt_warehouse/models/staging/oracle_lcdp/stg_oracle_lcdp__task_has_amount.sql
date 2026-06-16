

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`lcdp_task_has_amount`
),

cleaned_data as (
    select
        -- IDs convertis en BIGINT
        cast(idtask as int64) as idtask,
        cast(idtax as int64) as idtax,
        cast(idtax_region as int64) as idtax_region,

        -- Montants / taux
        cast(tax_rate as float64) as tax_rate,
        cast(tax_amount as float64) as tax_amount,
        cast(amount_without_tax as float64) as amount_without_tax,
        cast(percentage as float64) as percentage,

        -- Timestamps harmonisés
        timestamp(_sdc_extracted_at) as extracted_at,
        timestamp(_sdc_deleted_at) as deleted_at

    from source_data
),

-- Synchro avec la table des tâches pour éviter les orphelins
filtered_data as (
    select cr.*
    from cleaned_data as cr
    inner join `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task` as t
        on cr.idtask = t.idtask
)

select * from filtered_data