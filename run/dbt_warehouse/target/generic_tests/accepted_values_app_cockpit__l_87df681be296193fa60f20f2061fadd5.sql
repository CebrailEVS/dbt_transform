
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        destination_code as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_app_cockpit_supply`.`app_cockpit__lcdp_invendu`
    group by destination_code

)

select *
from all_values
where value_field not in (
    'PERIME','CASSE'
)



  
  
      
    ) dbt_internal_test