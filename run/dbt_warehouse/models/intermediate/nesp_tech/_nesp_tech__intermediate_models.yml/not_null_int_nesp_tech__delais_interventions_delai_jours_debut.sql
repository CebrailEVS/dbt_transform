
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select delai_jours_debut
from `evs-datastack-prod`.`prod_intermediate`.`int_nesp_tech__delais_interventions`
where delai_jours_debut is null



  
  
      
    ) dbt_internal_test