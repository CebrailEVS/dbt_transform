
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select mois
from `evs-datastack-prod`.`prod_intermediate`.`int_oracle_neshu__ca_client_mensuel`
where mois is null



  
  
      
    ) dbt_internal_test