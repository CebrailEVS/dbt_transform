
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        type_document as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__nunshen_mouvement_stock`
    group by type_document

)

select *
from all_values
where value_field not in (
    3,4,6,7,13,20,21,23,26
)



  
  
      
    ) dbt_internal_test