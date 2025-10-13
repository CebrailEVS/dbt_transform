
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select agence
from `evs-datastack-prod`.`prod_reference`.`tech_piece_agence_mapping`
where agence is null



  
  
      
    ) dbt_internal_test