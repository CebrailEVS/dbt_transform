
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select units_per_pack
from `evs-datastack-prod`.`prod_reference`.`ref_oracle_neshu__product_packaging`
where units_per_pack is null



  
  
      
    ) dbt_internal_test