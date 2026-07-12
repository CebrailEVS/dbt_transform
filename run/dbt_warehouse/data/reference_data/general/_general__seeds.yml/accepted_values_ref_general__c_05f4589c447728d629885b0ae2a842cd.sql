
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        saison as value_field,
        count(*) as n_records

    from `evs-datastack-prod`.`prod_reference`.`ref_general__calendrier_saison`
    group by saison

)

select *
from all_values
where value_field not in (
    'ete','hiver'
)



  
  
      
    ) dbt_internal_test