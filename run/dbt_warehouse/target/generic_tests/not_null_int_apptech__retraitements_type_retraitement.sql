
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select type_retraitement
from `evs-datastack-prod`.`prod_intermediate`.`int_apptech__retraitements`
where type_retraitement is null



  
  
      
    ) dbt_internal_test