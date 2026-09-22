
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select forced_source
from `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__consommation_source_override`
where forced_source is null



  
  
      
    ) dbt_internal_test