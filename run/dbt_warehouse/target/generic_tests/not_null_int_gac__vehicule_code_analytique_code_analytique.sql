
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select code_analytique
from `evs-datastack-prod`.`prod_intermediate`.`int_gac__vehicule_code_analytique`
where code_analytique is null



  
  
      
    ) dbt_internal_test