
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        convertir_code_5 as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_aguila`
    group by convertir_code_5

)

select *
from all_values
where value_field not in (
    'OUI','NON'
)



  
  
      
    ) dbt_internal_test