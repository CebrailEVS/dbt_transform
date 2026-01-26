
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select tarif_factu
from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__key_facturation`
where tarif_factu is null



  
  
      
    ) dbt_internal_test