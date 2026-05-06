
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select co_secteur
from `evs-datastack-prod`.`prod_reference`.`ref_nesp_co__commerciaux`
where co_secteur is null



  
  
      
    ) dbt_internal_test