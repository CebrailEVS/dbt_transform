
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__disponibilite_article_neshu_depot_mensuel`

where not(taux_disponibilite_pct between 0 and 100)


  
  
      
    ) dbt_internal_test