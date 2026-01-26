
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select montagne
from `evs-datastack-prod`.`prod_reference`.`ref_nesp_tech__cps_montagne_primes`
where montagne is null



  
  
      
    ) dbt_internal_test