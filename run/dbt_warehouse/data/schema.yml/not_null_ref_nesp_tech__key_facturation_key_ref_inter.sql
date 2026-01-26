
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select key_ref_inter
from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__key_facturation`
where key_ref_inter is null



  
  
      
    ) dbt_internal_test