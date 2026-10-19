
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select classe_machine
from `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__classe_modele_machine`
where classe_machine is null



  
  
      
    ) dbt_internal_test