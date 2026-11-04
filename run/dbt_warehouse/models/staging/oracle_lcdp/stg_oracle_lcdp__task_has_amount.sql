
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_lcdp__task_has_amount`
      
    
    

    
    OPTIONS(
      description="""task_has_amount nettoy\u00e9s depuis lcdp_task_has_amount - ventilation HT/TVA par taux et par t\u00e2che (montants encaiss\u00e9s). Source du HT/TVA des comptages cash. Inner join sur stg_oracle_lcdp__task pour \u00e9carter les orphelins."""
    )
    as (
      

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
        timestamp(_extracted_at) as extracted_at

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
    );
  