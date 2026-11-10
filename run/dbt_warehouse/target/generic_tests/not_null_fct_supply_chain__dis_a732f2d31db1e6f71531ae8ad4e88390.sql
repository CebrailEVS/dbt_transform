
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select taux_disponibilite_pct
from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__disponibilite_article_neshu_depot_mensuel`
where taux_disponibilite_pct is null



  
  
      
    ) dbt_internal_test