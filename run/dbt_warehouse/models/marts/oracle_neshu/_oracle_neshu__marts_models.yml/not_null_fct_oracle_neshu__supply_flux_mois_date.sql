
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select mois_date
from `evs-datastack-prod`.`prod_marts`.`fct_oracle_neshu__supply_flux`
where mois_date is null



  
  
      
    ) dbt_internal_test