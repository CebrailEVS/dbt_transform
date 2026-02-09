
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select date_pointage_jour
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_lcdp__pointage`
where date_pointage_jour is null



  
  
      
    ) dbt_internal_test