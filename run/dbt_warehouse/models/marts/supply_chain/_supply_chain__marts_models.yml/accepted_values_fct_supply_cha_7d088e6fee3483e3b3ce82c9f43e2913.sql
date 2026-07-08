
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        depot as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_supply_chain__rupture_depot_yuman`
    group by depot

)

select *
from all_values
where value_field not in (
    '06 - ATELIER RUNGIS DEPOT','07 - ATELIER LYON DEPOT','08 - ATELIER BORDEAUX DEPOT','09 - ATELIER STRASBOURG DEPOT'
)



  
  
      
    ) dbt_internal_test