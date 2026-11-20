
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select alerte_compile
from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__alerte_conso_aguila`
where alerte_compile is null



  
  
      
    ) dbt_internal_test