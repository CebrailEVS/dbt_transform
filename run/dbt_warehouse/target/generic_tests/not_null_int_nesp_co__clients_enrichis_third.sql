
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select third
from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_co__clients_enrichis`
where third is null



  
  
      
    ) dbt_internal_test