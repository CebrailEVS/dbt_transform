
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        code_type as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_oracle_nayka__product_type`
    group by code_type

)

select *
from all_values
where value_field not in (
    'PRODUCT','PART','CALCULATED_PRODUCT','MODEL','PRICE_LINE','PAYMENT_MODE','GENERIC','COMMENT','HUMAN_RESOURCES','CONTAINER'
)



  
  
      
    ) dbt_internal_test