
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        type_retraitement as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_intermediate`.`int_apptech__retraitements`
    group by type_retraitement

)

select *
from all_values
where value_field not in (
    'astreinte','mee','curative','aguila','pause','modif_intervention'
)



  
  
      
    ) dbt_internal_test