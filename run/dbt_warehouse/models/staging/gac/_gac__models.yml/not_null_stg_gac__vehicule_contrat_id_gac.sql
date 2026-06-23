
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select contrat_id_gac
from `evs-datastack-prod`.`prod_staging`.`stg_gac__vehicule`
where contrat_id_gac is null



  
  
      
    ) dbt_internal_test