
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_debut_passage_appro
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__chargement_vs_conso`
where date_debut_passage_appro is null



  
  
      
    ) dbt_internal_test