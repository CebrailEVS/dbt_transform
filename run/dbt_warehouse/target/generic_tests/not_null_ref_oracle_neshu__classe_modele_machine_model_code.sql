
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select model_code
from `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__classe_modele_machine`
where model_code is null



  
  
      
    ) dbt_internal_test