
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select key_inter
from `evs-datastack-prod`.`prod_intermediate`.`int_tech_evs__interventions_enriched`
where key_inter is null



  
  
      
    ) dbt_internal_test