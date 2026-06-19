
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__product_packaging`

where not(units_per_pack  > 1)


  
  
      
    ) dbt_internal_test