
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select modele_equipement_id
from `evs-datastack-prod`.`prod_marts`.`dim_neshu__modele_equipement`
where modele_equipement_id is null



  
  
      
    ) dbt_internal_test