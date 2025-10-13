
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select nom
from `evs-datastack-prod`.`prod_reference`.`tech_piece_agence_mapping`
where nom is null



  
  
      
    ) dbt_internal_test