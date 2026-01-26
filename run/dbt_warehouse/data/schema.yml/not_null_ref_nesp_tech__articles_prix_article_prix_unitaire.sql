
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select article_prix_unitaire
from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__articles_prix`
where article_prix_unitaire is null



  
  
      
    ) dbt_internal_test