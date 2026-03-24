
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select intervention_id
from `evs-datastack-prod`.`prod_intermediate`.`int_tech_evs__interventions_enriched`
where intervention_id is null



  
  
      
    ) dbt_internal_test