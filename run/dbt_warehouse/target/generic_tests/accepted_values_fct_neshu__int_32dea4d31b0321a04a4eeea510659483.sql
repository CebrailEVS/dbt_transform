
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        statut_inter as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_marts`.`fct_neshu__intervention_roadman`
    group by statut_inter

)

select *
from all_values
where value_field not in (
    'TERMINATED','TOTECH'
)



  
  
      
    ) dbt_internal_test