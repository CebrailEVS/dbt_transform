
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_heure_debut
from `evs-datastack-prod`.`prod_marts`.`fct_technique__consommation_article_nespresso`
where date_heure_debut is null



  
  
      
    ) dbt_internal_test