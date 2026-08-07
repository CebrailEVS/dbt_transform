
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select valid_from
from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__key_facturation`
where valid_from is null



  
  
      
    ) dbt_internal_test