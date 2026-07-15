
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        a_facturer as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_staging`.`stg_apptech__suivi_tech_modif_intervention`
    group by a_facturer

)

select *
from all_values
where value_field not in (
    'OUI','NON'
)



  
  
      
    ) dbt_internal_test