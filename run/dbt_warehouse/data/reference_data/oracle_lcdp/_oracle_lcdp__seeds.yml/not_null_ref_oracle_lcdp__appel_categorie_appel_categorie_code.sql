
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select appel_categorie_code
from `evs-datastack-prod`.`prod_reference`.`ref_oracle_lcdp__appel_categorie`
where appel_categorie_code is null



  
  
      
    ) dbt_internal_test