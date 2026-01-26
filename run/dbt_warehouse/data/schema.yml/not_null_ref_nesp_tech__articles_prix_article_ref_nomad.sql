
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select article_ref_nomad
from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__articles_prix`
where article_ref_nomad is null



  
  
      
    ) dbt_internal_test