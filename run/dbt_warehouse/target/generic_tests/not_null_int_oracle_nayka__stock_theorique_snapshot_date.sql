
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select snapshot_date
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_nayka__stock_theorique`
where snapshot_date is null



  
  
      
    ) dbt_internal_test