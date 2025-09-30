
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        data_source as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__conso_business_review`
    group by data_source

)

select *
from all_values
where value_field not in (
    'TELEMETRIE','CHARGEMENT','LIVRAISON'
)



  
  
      
    ) dbt_internal_test