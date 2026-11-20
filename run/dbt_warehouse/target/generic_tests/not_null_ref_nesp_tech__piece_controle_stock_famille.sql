
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select famille
from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__piece_controle_stock`
where famille is null



  
  
      
    ) dbt_internal_test