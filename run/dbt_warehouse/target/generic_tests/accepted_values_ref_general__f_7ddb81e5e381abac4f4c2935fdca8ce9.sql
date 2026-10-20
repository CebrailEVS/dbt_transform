
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        nom_jour_ferie as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_reference`.`ref_general__feries_alsace_moselle`
    group by nom_jour_ferie

)

select *
from all_values
where value_field not in (
    'vendredi_saint','saint_etienne'
)



  
  
      
    ) dbt_internal_test