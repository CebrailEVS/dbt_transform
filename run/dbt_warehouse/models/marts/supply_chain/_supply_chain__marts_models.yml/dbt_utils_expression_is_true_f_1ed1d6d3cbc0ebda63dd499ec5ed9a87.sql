
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__classification_article_neshu`

where not((methode_prevision = 'exclu') = (statut_vie in ('arrete', 'inactif')))


  
  
      
    ) dbt_internal_test