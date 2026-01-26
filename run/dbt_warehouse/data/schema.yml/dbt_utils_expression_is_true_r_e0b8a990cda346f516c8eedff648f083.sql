
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__articles_prix`

where not(article_prix_unitaire  >= 0)


  
  
      
    ) dbt_internal_test