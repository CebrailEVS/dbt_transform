
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select famille_neshu
from `evs-datastack-prod`.`prod_reference`.`ref_yuman__machine_clean`
where famille_neshu is null



  
  
      
    ) dbt_internal_test