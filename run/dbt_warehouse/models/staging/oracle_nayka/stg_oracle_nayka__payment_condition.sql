
  
    

    create or replace table `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__payment_condition`
      
    
    

    
    OPTIONS(
      description=""""""
    )
    as (
      

with source_data as (
    select *
    from `evs-datastack-prod`.`prod_raw`.`nayka_payment_condition`
),

cleaned_data as (
    -- Retirées : idstring (table string non extraite), idinstance (constante), tmp_update et
    -- comments (vides), early_calculation, unit_due et accruing (paramètres internes du calcul d'échéance).
    select
        cast(idpayment_condition as int64) as idpayment_condition,
        code,
        cast(installment_days_number as int64) as installment_days_number,
        cast(installment_month_day_number as int64) as installment_month_day_number,
        cast(multiple_installment as int64) as multiple_installment,
        cast(is_active as bool) as is_active,
        timestamp(datetime(modification_date), 'Europe/Paris') as updated_at,
        timestamp(_extracted_at) as extracted_at
    from source_data
)

select *
from cleaned_data
    );
  